class Transaction(object):
    def __init__(self, cxn):
        self.cxn = cxn

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass