import psycopg2, psycopg2.extensions
import pytest
import testing.postgresql

from nestedtransactions.transaction import Transaction


@pytest.fixture(scope='module', name='cxn')
def create_connection_and_db_table(request):
    postgresql = testing.postgresql.Postgresql()

    def fin():
        postgresql.stop()

    request.addfinalizer(fin)

    cxn = psycopg2.connect(**postgresql.dsn())
    cur = cxn.cursor()
    cur.execute("CREATE TABLE tmp_table(Id VARCHAR(80))")
    return cxn


def test_no_open_transactions_on_successful_exit(cxn):
    # commit to make this test independent of the order
    #   - should we solve this in a better way? do we even care about the first not in transaction assert?
    cxn.commit()

    assert_not_in_transaction(cxn)
    with Transaction(cxn):
        cxn.cursor().execute('SELECT 1')
        assert_in_transaction(cxn)
    assert_not_in_transaction(cxn)


def test_no_open_transactions_on_exception(cxn):
    # commit to make this test independent of the order
    #   - should we solve this in a better way? do we even care about the first not in transaction assert?
    cxn.commit()

    assert_not_in_transaction(cxn)
    try:
        with Transaction(cxn):
            cxn.cursor().execute('SELECT 1')
            assert_in_transaction(cxn)
            raise ExpectedException('This rolls back the transaction')
    except ExpectedException:
        pass
    assert_not_in_transaction(cxn)


def test_changes_applied_on_successful_exit(cxn):
    cur = cxn.cursor()
    with Transaction(cxn):
        cur.execute("INSERT INTO tmp_table VALUES ('test_changes_applied_on_successful_exit')")
    cur.execute("SELECT * FROM tmp_table WHERE Id = 'test_changes_applied_on_successful_exit'")
    rows = cur.fetchall()
    assert len(rows) == 1


def test_changes_discarded_on_exception(cxn):
    cur = cxn.cursor()
    try:
        with Transaction(cxn):
            cur.execute("INSERT INTO tmp_table VALUES ('test_changes_discarded_on_exception')")
            raise ExpectedException('This discards the insert')
    except ExpectedException:
        pass
    cur.execute("SELECT * FROM tmp_table WHERE Id = 'test_changes_discarded_on_exception'")
    rows = cur.fetchall()
    assert len(rows) == 0


def test_forced_discard_changes_discarded_on_successful_exit(cxn):
    pass


def test_forced_discard_changes_discarded_on_exception(cxn):
    pass


def test_inner_and_outer_changes_persisted_on_successful_exit(cxn):
    pass


def test_inner_and_outer_changes_discarded_on_outer_exception(cxn):
    pass


def test_inner_and_outer_changes_discarded_on_unhandled_inner_exception(cxn):
    pass


def test_inner_changes_discarded_on_exception_but_outer_changes_persisted_on_successful_exit(cxn):
    pass


def assert_in_transaction(cxn):
    assert cxn.status == psycopg2.extensions.STATUS_IN_TRANSACTION


def assert_not_in_transaction(cxn):
    assert cxn.status == psycopg2.extensions.STATUS_READY


class ExpectedException(Exception):
    pass
