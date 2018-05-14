class Transaction(object):
    def __init__(self, cxn):
        self.cxn = cxn

    def __enter__(self):
        print('BEGIN')
        if self.cxn.autocommit is True:
            self.cxn.autocommit = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print('ROLLBACK')
            self.cxn.rollback()
        else:
            print('COMMIT')
            self.cxn.commit()
