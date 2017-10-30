import psycopg2
import testing.postgresql

from nestedtransactions.transaction import Transaction


def test_changes_applied_on_successful_exit():
    with testing.postgresql.Postgresql() as postgresql:
        cxn = psycopg2.connect(**postgresql.dsn())
        with Transaction(cxn):
            cur = cxn.cursor()
            create_tmp_table(cur)
            cur.execute("INSERT INTO tmp_table VALUES ('hello')")
        cur.execute("SELECT * FROM tmp_table")
        rows = cur.fetchall()
        assert len(rows) == 1


def create_tmp_table(cur):
    cur.execute("CREATE TEMPORARY TABLE tmp_table(Id VARCHAR(80)) ON COMMIT PRESERVE ROWS")


def test_changes_discarded_on_exception():
    pass


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
