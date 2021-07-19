# coding=utf-8

from os import path
from benedict import benedict

from xrpl_websocket import Client
import sqlite3

from utils.factory import dict_factory


class Source():
    def __init__(self, source=None):

        # check if source is file or websocket
        self.is_locale = False

        # source connection
        self.connection = None

        if path.isfile(source):
            # set local flag
            self.is_locale = True

            # coonect to sqlite file
            self.connection = sqlite3.connect(source, check_same_thread=False)
            # self.connection.row_factory = sqlite3.Row
            self.connection.row_factory = dict_factory
        else:
            self.connection = Client(server=source)
            # connect to the websocket
            self.connection.connect(nowait=False)


    def get_connection(self):
        if self.is_locale:
            return self.connection.cursor()
        else:
            return self.connection

    def close(self):
        if self.is_locale:
            self.connection.close()
        else:
            self.connection.disconnect()

    def get_transactions_count(self, ledger_index):
        if not self.is_locale:
            return None

        return 8000000
        count = self.get_connection().execute("SELECT COUNT(*) FROM  Transactions WHERE LedgerSeq >= (?)", (ledger_index, )).fetchone()["COUNT(*)"]
        return count

    def get_ledger_range(self):
        start = 0
        end = 0

        if self.is_locale:
            end = self.get_connection().execute("SELECT LedgerSeq FROM  Transactions WHERE LedgerSeq = (SELECT MAX(LedgerSeq)  FROM Transactions) LIMIT 1").fetchone()["LedgerSeq"]
            start = self.get_connection().execute("SELECT LedgerSeq FROM  Transactions WHERE LedgerSeq = (SELECT MIN(LedgerSeq)  FROM Transactions) LIMIT 1").fetchone()["LedgerSeq"]
        else:
            r = self.get_connection().send(command="server_info")
            complete_ledgers= r['result']['info']['complete_ledgers']
            start, sp, end = complete_ledgers.partition('-')

        return int(start), int(end)

    def get_transactions(self, ledger_index):
        # if locale fetch from local
        if self.is_locale:
            txs = self.get_connection().execute("SELECT * FROM Transactions WHERE LedgerSeq = (?)", (ledger_index, )).fetchall()
            return ledger_index, txs

        # else fetch from remote

        # first get transaction counts
        raw = self.get_connection().send(
            {
            "ledger_index": ledger_index,
            "command": "ledger",
            "transactions": True,
            "expand": False
            }
        )

        raw = benedict(raw)
        tx_ids = raw['result.ledger.transactions']


        if not tx_ids and len(tx_ids) == 0:
            return ledger_index, []

        if len(tx_ids) > 200:
            txs = [self.get_connection().send(command="tx", transaction=tx) for tx in tx_ids]
            all_txs =dict((k, v) for (k, v) in txs.iteritems() if not v['error'] and v['meta'] and v['TransactionResult'])
            return ledger_index, all_txs
        else:
            expanded_txs = self.get_connection().send(
                {
                    "ledger_index": ledger_index,
                    "command": "ledger",
                    "transactions": True,
                    "expand": True
                }
            )

        expanded_txs = benedict(expanded_txs)

        return ledger_index, expanded_txs['result.ledger.transactions']
