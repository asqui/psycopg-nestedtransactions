from contextlib import ContextDecorator

class Transaction(ContextDecorator):
    def __init__(self, cxn):
        self.cxn = cxn

    def __enter__(self):
        print('BEGIN')
        self.cxn.autocommit = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(exc_type)
        print(exc_val)
        print(exc_tb)
        if exc_type is not None:
            print('ROLLBACK')
            self.cxn.rollback()
        else:
            self.cxn.commit()
