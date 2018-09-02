import logging
from collections import defaultdict

from psycopg2.extensions import TRANSACTION_STATUS_INTRANS, TRANSACTION_STATUS_INERROR

_log = logging.getLogger(__name__)
_log.setLevel(logging.WARN)


class Transaction(object):
    __transaction_stack = defaultdict(list)  # cxn -> [active_transaction_contexts]

    def __init__(self, cxn, force_discard=False):
        self.cxn = cxn
        self._force_discard = force_discard
        self._rolled_back = False
        self._original_autocommit = None
        self._patched_originals = None
        self._containing_txn = None

    def __enter__(self):
        if len(self._transaction_stack) == 0:
            _log.info('Creating new outer transaction for {!r}'.format(self.cxn))

            self._containing_txn = (self.cxn.get_transaction_status() == TRANSACTION_STATUS_INTRANS)
            if not self._containing_txn:
                _log.info('%r: BEGIN', self.cxn)

            self._try_patch(self.cxn)

        self._original_autocommit = self.cxn.autocommit
        if self.cxn.autocommit:
            self.cxn.autocommit = False

        self._savepoint_id = 'savepoint_{}'.format(len(self._transaction_stack))
        self._transaction_stack.append(self)

        _execute_and_log(self.cxn, 'SAVEPOINT ' + self._savepoint_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        exception_raised = exc_type is not None
        try:
            if self._force_discard or exception_raised:
                if not self._rolled_back:
                    self.rollback()
            elif not self._rolled_back:
                self._commit()

            assert self._transaction_stack.pop() is self, ('Out-of-order Transaction context '
                                                           'exits. Are you calling __exit__() '
                                                           'manually and getting it wrong?')

            if len(self._transaction_stack) == 0:
                self._restore_patches(self.cxn)
                del self.__transaction_stack[self.cxn]
                if not self._containing_txn:
                    _log.info('%r: COMMIT', self.cxn)
                    self.cxn.commit()

            if self.cxn.autocommit != self._original_autocommit:
                self.cxn.autocommit = self._original_autocommit
        except:
            if exc_type:
                _log.error('Exception raised when trying to exit Transaction context. '
                           'Original exception:\n', exc_info=(exc_type, exc_val, exc_tb))
            raise

    def _commit(self):
        if self.cxn.get_transaction_status() == TRANSACTION_STATUS_INERROR:
            raise Exception('SQL error occurred within current transaction. Transaction.rollback() '
                            'must be called before exiting transaction context.')
        _execute_and_log(self.cxn, 'RELEASE SAVEPOINT ' + self._savepoint_id)

    def rollback(self):
        if self not in self._transaction_stack:
            raise Exception('Cannot rollback outside transaction context.')
        if self._transaction_stack[-1] is not self:
            raise Exception('Cannot rollback outer transaction from nested transaction context.')
        if self._rolled_back:
            raise Exception('Transaction already rolled back.')
        _execute_and_log(self.cxn, 'ROLLBACK TO SAVEPOINT ' + self._savepoint_id)
        self._rolled_back = True

    @property
    def _transaction_stack(self):
        return self.__transaction_stack[self.cxn]

    def _try_patch(self, cxn):
        """
        Try to patch `cxn` methods to assert helpfully when called in the Transaction context.

        NB: This is not possible if `cxn` is coming from an extension module (e.g. a pure
        pycopg2.extensions.connection instance), but it is possible if `cxn` is a Python subclass.
        """
        original_commit, original_rollback = cxn.commit, cxn.rollback

        def new_commit():
            raise Exception('Explicit commit() forbidden within a Transaction context. '
                            '(Transaction will be automatically committed on successful exit from '
                            'context.)')

        def new_rollback():
            raise Exception('Explicit rollback() forbidden within a Transaction context. '
                            '(Either call Transaction.rollback() or allow an exception to '
                            'propogate out of the context.)')

        try:
            cxn.commit, cxn.rollback = new_commit, new_rollback
        except AttributeError:
            pass  # Patching failed
        else:
            self._patched_originals = original_commit, original_rollback

    def _restore_patches(self, cxn):
        if self._patched_originals is not None:
            cxn.commit, cxn.rollback = self._patched_originals


def _execute_and_log(cxn, sql):
    with cxn.cursor() as cur:
        _log.info('%r: %s', cxn, sql)
        cur.execute(sql)
