from contextlib import ContextDecorator


class Transaction(ContextDecorator):
    def __init__(self, cxn, force_disard=False):
        self.force_disard = force_disard
        self.cxn = cxn

    def __enter__(self):
        if self.cxn.autocommit is True:
            self.cxn.autocommit = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None or self.force_disard:
            self.cxn.rollback()
        else:
            self.cxn.commit()
