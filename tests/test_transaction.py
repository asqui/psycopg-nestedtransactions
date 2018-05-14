import psycopg2
import pytest
import testing.postgresql
from psycopg2.extensions import STATUS_READY, STATUS_IN_TRANSACTION, TRANSACTION_STATUS_IDLE

from nestedtransactions.transaction import Transaction


def test_no_open_transactions_on_successful_exit(cxn):
    assert_not_in_transaction(cxn)
    with Transaction(cxn):
        assert_in_transaction(cxn)
        cxn.cursor().execute('SELECT 1')
        assert_in_transaction(cxn)
    assert_not_in_transaction(cxn)


def test_no_open_transactions_on_exception(cxn):
    assert_not_in_transaction(cxn)
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            assert_in_transaction(cxn)
            cxn.cursor().execute('SELECT 1')
            assert_in_transaction(cxn)
            raise ExpectedException('This rolls back the transaction')
    assert_not_in_transaction(cxn)


def test_changes_applied_on_successful_exit(cxn):
    cur = cxn.cursor()
    with Transaction(cxn):
        cur.execute("INSERT INTO tmp_table VALUES ('outer')")
    cur.execute("SELECT * FROM tmp_table WHERE Id = 'outer'")
    rows = cur.fetchall()
    assert len(rows) == 1


def test_changes_discarded_on_exception(cxn):
    cur = cxn.cursor()
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            cur.execute("INSERT INTO tmp_table VALUES ('outer')")
            raise ExpectedException('This discards the insert')
    cur.execute("SELECT * FROM tmp_table WHERE Id = 'outer'")
    rows = cur.fetchall()
    assert len(rows) == 0


@pytest.mark.skip
def test_forced_discard_changes_discarded_on_successful_exit(cxn):
    cur = cxn.cursor()
    with Transaction(cxn, force_disard = True):
        cur.execute("INSERT INTO tmp_table VALUES ('outer')")
    cur.execute("SELECT * FROM tmp_table")
    rows = cur.fetchall()
    assert len(rows) == 0


@pytest.mark.skip
def test_forced_discard_changes_discarded_on_exception(cxn):
    cur = cxn.cursor()
    with pytest.raises(ExpectedException):
        with Transaction(cxn, force_disard=True):
            cur.execute("INSERT INTO tmp_table VALUES ('outer')")
            raise ExpectedException('The insert should be discarded here regardless of any exceptions thrown')
    cur.execute("SELECT * FROM tmp_table")
    rows = cur.fetchall()
    assert len(rows) == 0


def test_inner_and_outer_changes_persisted_on_successful_exit(cxn):
    cur = cxn.cursor()
    with Transaction(cxn):
        cur.execute("INSERT INTO tmp_table VALUES ('outer-before')")
        with Transaction(cxn):
            cur.execute("INSERT INTO tmp_table VALUES ('inner')")
        cur.execute("INSERT INTO tmp_table VALUES ('outer-after')")
    cur.execute('SELECT * FROM tmp_table')
    rows = cur.fetchall()
    assert len(rows) == 3


def test_inner_and_outer_changes_discarded_on_outer_exception(cxn):
    cur = cxn.cursor()
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            cur.execute("INSERT INTO tmp_table VALUES ('outer')")
            with Transaction(cxn):
                cur.execute("INSERT INTO tmp_table VALUES ('inner')")
            raise ExpectedException()
    cur.execute('SELECT * FROM tmp_table')
    rows = cur.fetchall()
    assert len(rows) == 0


def test_inner_and_outer_changes_discarded_on_unhandled_inner_exception(cxn):
    cur = cxn.cursor()
    with pytest.raises(ExpectedException):
        with Transaction(cxn):
            cur.execute("INSERT INTO tmp_table VALUES ('outer')")
            with Transaction(cxn):
                cur.execute("INSERT INTO tmp_table VALUES ('inner')")
                raise ExpectedException()
    cur.execute('SELECT * FROM tmp_table')
    rows = cur.fetchall()
    assert len(rows) == 0


def test_inner_changes_discarded_on_exception_but_outer_changes_persisted_on_successful_exit(cxn):
    cur = cxn.cursor()
    with Transaction(cxn):
        cur.execute("INSERT INTO tmp_table VALUES ('outer-before')")
        with pytest.raises(ExpectedException):
            with Transaction(cxn):
                cur.execute("INSERT INTO tmp_table VALUES ('inner')")
                raise ExpectedException()
        cur.execute("INSERT INTO tmp_table VALUES ('outer-after')")
    cur.execute('SELECT * FROM tmp_table')
    rows = cur.fetchall()
    assert set(rows) == {('outer-before',), ('outer-after',)}


def test_explicit_rollback_discards_changes(cxn):
    cur = cxn.cursor()
    with Transaction(cxn) as txn:
        cur.execute("INSERT INTO tmp_table VALUES ('outer')")
        txn.rollback()
    cur.execute("SELECT * FROM tmp_table")
    rows = cur.fetchall()
    assert len(rows) == 0


def test_explicit_rollback_repeated_raises(cxn):
    cur = cxn.cursor()
    with Transaction(cxn) as txn:
        cur.execute("INSERT INTO tmp_table VALUES ('outer')")
        txn.rollback()
        with pytest.raises(Exception, match='Transaction already rolled back.'):
            txn.rollback()


def test_explicit_rollback_outside_context_raises(cxn):
    cur = cxn.cursor()
    with Transaction(cxn) as txn:
        cur.execute("INSERT INTO tmp_table VALUES ('outer')")
    with pytest.raises(Exception, match='Cannot rollback outside transaction context.'):
        txn.rollback()


def test_explicit_rollback_inner_discards_only_inner_changes(cxn):
    cur = cxn.cursor()
    with Transaction(cxn):
        cur.execute("INSERT INTO tmp_table VALUES ('outer-before')")
        with Transaction(cxn) as inner:
            cur.execute("INSERT INTO tmp_table VALUES ('inner')")
            inner.rollback()
        cur.execute("INSERT INTO tmp_table VALUES ('outer-after')")
    cur.execute('SELECT * FROM tmp_table')
    rows = cur.fetchall()
    assert set(rows) == {('outer-before',), ('outer-after',)}


def test_explicit_rollback_outer_discards_inner_and_outer_changes(cxn):
    cur = cxn.cursor()
    with Transaction(cxn) as outer:
        cur.execute("INSERT INTO tmp_table VALUES ('outer')")
        with Transaction(cxn):
            cur.execute("INSERT INTO tmp_table VALUES ('inner')")
        outer.rollback()
    cur.execute('SELECT * FROM tmp_table')
    rows = cur.fetchall()
    assert len(rows) == 0


def test_rollback_outer_transaction_while_inner_transaction_is_active(cxn):
    with Transaction(cxn) as outer:
        with Transaction(cxn):
            with pytest.raises(Exception, match='Cannot rollback outer transaction from nested transaction context.'):
                outer.rollback()


def test_transactions_on_multiple_connections_are_independent(cxn, other_cxn):
    with Transaction(cxn) as outer_txn:
        with Transaction(other_cxn):
            cxn.cursor().execute("INSERT INTO tmp_table VALUES ('outer')")
            other_cxn.cursor().execute("INSERT INTO tmp_table VALUES ('inner')")
            outer_txn.rollback()

    cur = cxn.cursor()
    cur.execute('SELECT * FROM tmp_table')
    rows = cur.fetchall()
    assert set(rows) == {('inner',)}


@pytest.mark.skip()
def test_transaction_already_in_progress_asserts_on_enter(cxn):
    cxn.autocommit = False
    cxn.cursor().execute("INSERT INTO tmp_table VALUES ('hello')")
    with pytest.raises(AssertionError, message='cxn is already in a transaction; Set cxn.autocommit = True'):
        Transaction(cxn).__enter__()


# --- Fixtures and helpers ---
@pytest.fixture(scope='module')
def db():
    """Create a fresh database and prepare a test table."""
    with testing.postgresql.Postgresql() as db:
        yield db


@pytest.fixture()
def cxn(db):
    """Prepare a test table and return a connection."""
    with _connect(db) as cxn:
        with cxn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS tmp_table")
            cur.execute("CREATE TABLE tmp_table(Id VARCHAR(80))")

    with _connect(db) as cxn:
        yield cxn
        #_assert_connection_is_clean(cxn)


@pytest.fixture()
def other_cxn(db):
    with _connect(db) as cxn:
        yield cxn
        #_assert_connection_is_clean(cxn)


def _connect(db):
    cxn = psycopg2.connect(**db.dsn())
    # cxn.autocommit = True
    _assert_connection_is_clean(cxn)
    return cxn


def _assert_connection_is_clean(cxn):
    assert cxn.status == STATUS_READY
    assert cxn.get_transaction_status() == TRANSACTION_STATUS_IDLE
    # assert cxn.autocommit


def assert_rows(cxn, *expected):
    with cxn:
        with cxn.cursor() as cur:
            cur.execute('SELECT * FROM tmp_table')
            rows = cur.fetchall()
            assert set(v for (v,) in rows) == set(expected)


def assert_in_transaction(cxn):
    assert cxn.status == STATUS_IN_TRANSACTION


def assert_not_in_transaction(cxn):
    assert cxn.status == STATUS_READY


class ExpectedException(Exception):
    pass
