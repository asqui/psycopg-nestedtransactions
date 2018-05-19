import psycopg2
import pytest
import testing.postgresql
from psycopg2.extensions import STATUS_READY, STATUS_IN_TRANSACTION, TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS

from nestedtransactions.transaction import Transaction


@pytest.fixture(scope='module')
def db():
    with testing.postgresql.Postgresql() as db:
        yield db


@pytest.fixture(autouse=True)
def create_tmp_table(db):
    with _connect(db) as cxn:
        with cxn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS tmp_table')
            cur.execute('CREATE TABLE tmp_table(Id VARCHAR(80) PRIMARY KEY)')


@pytest.fixture()
def cxn(db):
    with _connect(db) as cxn:
        yield cxn


@pytest.fixture()
def other_cxn(db):
    with _connect(db) as cxn:
        yield cxn


def _connect(db):
    cxn = psycopg2.connect(**db.dsn())
    cxn.autocommit = True
    assert_not_in_transaction(cxn)
    return cxn


def test_no_open_transaction_on_successful_exit(cxn):
    assert_not_in_transaction(cxn)
    with Transaction(cxn):
        assert_in_transaction(cxn)
    assert_not_in_transaction(cxn)


def test_no_open_transaction_on_exception(cxn):
    assert_not_in_transaction(cxn)
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            assert_in_transaction(cxn)
            raise ExpectedException('This rolls back the transaction')
    assert_not_in_transaction(cxn)


def test_autocommit_on_successful_exit(cxn):
    assert cxn.autocommit is True, 'Pre-condition'
    assert_not_in_transaction(cxn)
    with Transaction(cxn):
        assert_in_transaction(cxn)
    assert_not_in_transaction(cxn)
    assert cxn.autocommit is True


def test_autocommit_on_exception(cxn):
    assert cxn.autocommit is True, 'Pre-condition'
    assert_not_in_transaction(cxn)
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            assert_in_transaction(cxn)
            raise ExpectedException('This rolls back the transaction')
    assert_not_in_transaction(cxn)
    assert cxn.autocommit is True


def test_autocommit_off_successful_exit(cxn):
    cxn.autocommit = False
    assert_not_in_transaction(cxn)
    with Transaction(cxn):
        assert_in_transaction(cxn)
    assert_not_in_transaction(cxn)
    assert cxn.autocommit is False


def test_autocommit_off_exception(cxn):
    cxn.autocommit = False
    assert_not_in_transaction(cxn)
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            assert_in_transaction(cxn)
            raise ExpectedException('This rolls back the transaction')
    assert_not_in_transaction(cxn)
    assert cxn.autocommit is False


def test_autocommit_off_transaction_in_progress_successful_exit_leaves_transaction_running(cxn,
                                                                                           other_cxn):
    cxn.autocommit = False
    insert_row(cxn, 'prior')
    assert_in_transaction(cxn)
    with Transaction(cxn):
        insert_row(cxn, 'new')
    assert_in_transaction(cxn)
    assert cxn.autocommit is False
    assert_rows(other_cxn, set())  # Nothing committed; changes not visible on another connection


def test_autocommit_off_transaction_in_progress_exception_leaves_transaction_running(cxn,
                                                                                     other_cxn):
    cxn.autocommit = False
    insert_row(cxn, 'prior')
    assert_in_transaction(cxn)
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            insert_row(cxn, 'new')
            raise ExpectedException('This rolls back just the inner transaction')
    assert_in_transaction(cxn)
    assert cxn.autocommit is False
    assert_rows(cxn, {'prior'}, still_in_transaction=True)
    assert_rows(other_cxn, set())  # Nothing committed; changes not visible on another connection


def test_changes_applied_on_successful_exit(cxn, other_cxn):
    with Transaction(cxn):
        insert_row(cxn, 'value')
    assert_rows(cxn, {'value'})
    assert_rows(other_cxn, {'value'})


def test_changes_discarded_on_exception(cxn, other_cxn):
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            insert_row(cxn, 'value')
            raise ExpectedException('This discards the insert')
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


def test_transaction_stack_dict_does_not_leak(cxn):
    assert len(Transaction._Transaction__transaction_stack) == 0, 'Pre-condition'
    with Transaction(cxn):
        assert len(Transaction._Transaction__transaction_stack) == 1
    assert len(Transaction._Transaction__transaction_stack) == 0, 'Post-condition'


@pytest.mark.skip
def test_forced_discard_changes_discarded_on_successful_exit(cxn, other_cxn):
    with Transaction(cxn, force_disard = True):
        insert_row(cxn, 'value')
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


@pytest.mark.skip
def test_forced_discard_changes_discarded_on_exception(cxn, other_cxn):
    with pytest.raises(ExpectedException):
        with Transaction(cxn, force_disard=True):
            insert_row(cxn, 'value')
            raise ExpectedException()
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


def test_inner_and_outer_changes_persisted_on_successful_exit(cxn, other_cxn):
    with Transaction(cxn):
        insert_row(cxn, 'outer-before')
        with Transaction(cxn):
            insert_row(cxn, 'inner')
        insert_row(cxn, 'outer-after')
    assert_rows(cxn, {'outer-before', 'inner', 'outer-after'})
    assert_rows(other_cxn, {'outer-before', 'inner', 'outer-after'})


def test_inner_and_outer_changes_discarded_on_outer_exception(cxn, other_cxn):
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            insert_row(cxn, 'outer')
            with Transaction(cxn):
                insert_row(cxn, 'inner')
            raise ExpectedException()
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


def test_inner_and_outer_changes_discarded_on_unhandled_inner_exception(cxn, other_cxn):
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            insert_row(cxn, 'outer')
            with Transaction(cxn):
                insert_row(cxn, 'inner')
                raise ExpectedException()
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


def test_inner_changes_discarded_on_exception_but_outer_changes_persisted_on_successful_exit(cxn,
                                                                                             other_cxn):
    with Transaction(cxn):
        insert_row(cxn, 'outer-before')
        with pytest.raises(ExpectedException):
            with Transaction(cxn):
                insert_row(cxn, 'inner')
                raise ExpectedException()
        insert_row(cxn, 'outer-after')
    assert_rows(cxn, {'outer-before', 'outer-after'})
    assert_rows(other_cxn, {'outer-before', 'outer-after'})


def test_explicit_rollback_outside_context_raises(cxn):
    with Transaction(cxn) as txn:
        insert_row(cxn, 'value')
    with pytest.raises(Exception, match='Cannot rollback outside transaction context.'):
        txn.rollback()


def test_explicit_rollback_inner_discards_only_inner_changes(cxn, other_cxn):
    with Transaction(cxn):
        insert_row(cxn, 'outer-before')
        with Transaction(cxn) as inner:
            insert_row(cxn, 'inner')
            inner.rollback()
        insert_row(cxn, 'outer-after')
    assert_rows(cxn, {'outer-before', 'outer-after'})
    assert_rows(other_cxn, {'outer-before', 'outer-after'})


def test_explicit_rollback_discards_changes(cxn, other_cxn):
    with Transaction(cxn) as txn:
        insert_row(cxn, 'value')
        txn.rollback()
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


def test_explicit_rollback_followed_by_exception_inside_context(cxn, other_cxn):
    with pytest.raises(ExpectedException):
        with Transaction(cxn) as txn:
            insert_row(cxn, 'value')
            txn.rollback()
            raise ExpectedException()
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


def test_explicit_rollback_repeated_raises(cxn):
    with Transaction(cxn) as txn:
        insert_row(cxn, 'value')
        txn.rollback()
        with pytest.raises(Exception, match='Transaction already rolled back.'):
            txn.rollback()


def test_explicit_rollback_outer_discards_inner_and_outer_changes(cxn, other_cxn):
    with Transaction(cxn) as outer:
        insert_row(cxn, 'outer')
        with Transaction(cxn):
            insert_row(cxn, 'inner')
        outer.rollback()
    assert_rows(cxn, set())
    assert_rows(other_cxn, set())


def test_rollback_outer_transaction_while_inner_transaction_is_active_not_allowed(cxn):
    with Transaction(cxn) as outer:
        with Transaction(cxn):
            with pytest.raises(Exception, match='Cannot rollback outer transaction '
                                                'from nested transaction context.'):
                outer.rollback()


def test_transactions_on_multiple_connections_are_independent(cxn, other_cxn):
    with Transaction(cxn) as outer_txn:
        insert_row(cxn, 'outer')
        with Transaction(other_cxn):
            insert_row(other_cxn, 'inner')
            outer_txn.rollback()
            assert_rows(other_cxn, {'inner'}, still_in_transaction=True)

    assert_rows(cxn, {'inner'})


def insert_row(cxn, value):
    with cxn.cursor() as cur:
        cur.execute('INSERT INTO tmp_table VALUES (%s)', (value,))


def assert_rows(cxn, expected, still_in_transaction=False):
    if still_in_transaction:
        assert_in_transaction(cxn)
    else:
        assert_not_in_transaction(cxn)

    with cxn.cursor() as cur:
        cur.execute('SELECT * FROM tmp_table')
        rows = cur.fetchall()
        assert set(v for (v,) in rows) == expected


def assert_in_transaction(cxn):
    assert cxn.status == STATUS_IN_TRANSACTION
    assert cxn.get_transaction_status() == TRANSACTION_STATUS_INTRANS


def assert_not_in_transaction(cxn):
    assert cxn.status == STATUS_READY
    assert cxn.get_transaction_status() == TRANSACTION_STATUS_IDLE


class ExpectedException(Exception):
    pass
