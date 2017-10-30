import psycopg2, psycopg2.extensions
import pytest
import testing.postgresql

from nestedtransactions.transaction import Transaction


def test_no_open_transactions_on_successful_exit():
    with testing.postgresql.Postgresql() as postgresql:
        cxn = psycopg2.connect(**postgresql.dsn())
        assert cxn.status == psycopg2.extensions.STATUS_READY
        with Transaction(cxn):
            cxn.cursor().execute('SELECT 1')
            assert cxn.status == psycopg2.extensions.STATUS_IN_TRANSACTION
        assert cxn.status == psycopg2.extensions.STATUS_READY


def test_no_open_transactions_on_exception():
    with testing.postgresql.Postgresql() as postgresql:
        cxn = psycopg2.connect(**postgresql.dsn())
        cur = cxn.cursor()
        create_tmp_table(cur)
        # with pytest.raises(Exception, message='This rolls back the transaction'):
        with Transaction(cxn):
            cxn.cursor().execute('SELECT 1')
            raise Exception('This rolls back the transaction')
        assert cxn.status == psycopg2.extensions.STATUS_READY


def test_changes_applied_on_successful_exit():
    with testing.postgresql.Postgresql() as postgresql:
        cxn = psycopg2.connect(**postgresql.dsn())
        cur = cxn.cursor()
        create_tmp_table(cur)
        with Transaction(cxn):
            cur.execute("INSERT INTO tmp_table VALUES ('hello')")
        cur.execute("SELECT * FROM tmp_table")
        rows = cur.fetchall()
        assert len(rows) == 1


def test_changes_discarded_on_exception():
    with testing.postgresql.Postgresql() as postgresql:
        cxn = psycopg2.connect(**postgresql.dsn())
        cur = cxn.cursor()
        create_tmp_table(cur)
        try:
            with Transaction(cxn):
                cur.execute("INSERT INTO tmp_table VALUES ('hello')")
                raise Exception('This discards the insert')
        except:
            pass
        cur.execute("SELECT * FROM tmp_table")
        rows = cur.fetchall()
        assert len(rows) == 0


def test_forced_discard_changes_discarded_on_successful_exit():
    pass


def test_forced_discard_changes_discarded_on_exception():
    pass


def test_inner_and_outer_changes_persisted_on_successful_exit():
    pass


def test_inner_and_outer_changes_discarded_on_outer_exception():
    pass


def test_inner_and_outer_changes_discarded_on_unhandled_inner_exception():
    pass


def test_inner_changes_discarded_on_exception_but_outer_changes_persisted_on_successful_exit():
    pass


def create_tmp_table(cursor):
    cursor.execute("CREATE TABLE tmp_table(Id VARCHAR(80))")
