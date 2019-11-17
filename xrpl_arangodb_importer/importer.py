# coding=utf-8

class Importer():
    def __init__(self, connection=None, database=None, logger=None, startLedger=None):
        assert database , "Database class need to be pass as to the class"
        assert connection , "Connection class need to be pass as to the class"

        # set database instance
        self.db = database
        # set connection instance
        self.conn = connection

        # set starting ledger point
        if not startLedger or startLedger == -1:
            self.currentIndex = 32570 # first ledger
        else:
            self.currentIndex = startLedger

        self.lastIndex = None

        self.logger = logger


    def percent(self, end):
        p = 100 * float(self.currentIndex)/float(end)
        return int(p)

    def start(self):
        # as the each ledger index depends on another
        # we cannot use threading

        start, end = self.conn.get_ledger_range()

        print("")

        while True:
            print("[!] Proccessing Ledger %s%% (%s/%s)" % (self.percent(end) , self.currentIndex, end), end='\r')
            index, txs  = self.conn.get_transactions(self.currentIndex)

            for tx in txs:
                self.db.insert(tx)

            self.currentIndex += 1
