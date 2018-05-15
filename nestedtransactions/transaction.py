import logging
from collections import defaultdict


_log = logging.getLogger(__name__)
_log.setLevel(logging.WARN)


class Transaction(object):
    __transaction_stack = defaultdict(list)

    def __init__(self, cxn):
        self.cxn = cxn
        self._rolled_back = False
        self._original_autocommit = None

    def __enter__(self):
        self._original_autocommit = self.cxn.autocommit
        if self.cxn.autocommit:
            self.cxn.autocommit = False

        self._savepoint_id = 'savepoint_{}'.format(len(self._transaction_stack))
        self._transaction_stack.append(self)
        self.cxn.cursor().execute('SAVEPOINT ' + self._savepoint_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                self.rollback()
            elif not self._rolled_back:
                self.cxn.cursor().execute('RELEASE SAVEPOINT ' + self._savepoint_id)

            assert self._transaction_stack.pop() == self

            if len(self._transaction_stack) == 0:
                del self.__transaction_stack[self.cxn]
                self.cxn.commit()

            if self.cxn.autocommit != self._original_autocommit:
                self.cxn.autocommit = self._original_autocommit
        except:
            if exc_type:
                _log.error('Exception raised when trying to exit Transaction context. '
                           'Original exception:\n', exc_info=(exc_type, exc_val, exc_tb))
            raise

    def rollback(self):
        if self not in self._transaction_stack:
            raise Exception('Cannot rollback outside transaction context.')
        if self._transaction_stack[-1] is not self:
            raise Exception('Cannot rollback outer transaction from nested transaction context.')
        if self._rolled_back:
            raise Exception('Transaction already rolled back.')
        self.cxn.cursor().execute('ROLLBACK TO SAVEPOINT ' + self._savepoint_id)
        self._rolled_back = True

    @property
    def _transaction_stack(self):
        return self.__transaction_stack[self.cxn]
