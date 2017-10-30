import psycopg2
import testing.postgresql

from nestedtransactions.transaction import Transaction


def test_connects_to_database():
    with testing.postgresql.Postgresql() as postgresql:
        cxn = psycopg2.connect(**postgresql.dsn())
        with Transaction(cxn):
            pass
