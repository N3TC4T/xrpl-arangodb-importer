# coding=utf-8

from os import path
from benedict import benedict

import sqlite3
from xrpl_websocket import Client

from utils.factory import dict_factory

class Source():
    def __init__(self, source=None):
        # check if source is file or websocket
        self.is_locale = False

        self.transactions_database = 'transaction.db'
        self.ledger_database = 'ledger.db'

        # source connection
        self.connection = None
        self.ledger = None

        if path.isdir(source):
            # set local flag
            self.is_locale = True

            self.connection = sqlite3.connect(path.join(source, self.transactions_database), check_same_thread=False)
            self.connection.row_factory = dict_factory


            self.ledger = sqlite3.connect(path.join(source, self.ledger_database), check_same_thread=False)
            self.ledger.row_factory = dict_factory
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
            ledger_close_time = self.ledger.execute("SELECT * FROM Ledgers WHERE LedgerSeq = (?)", (ledger_index, )).fetchone()["ClosingTime"]
            return txs, ledger_close_time

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

        ledger_close_time = raw['result']['ledger']['close_time']
        tx_ids = raw['result']['ledger']['transactions']

        if not tx_ids and len(tx_ids) == 0:
            return [], ledger_close_time

        if len(tx_ids) > 200:
            txs = [self.get_connection().send(command="tx", transaction=tx) for tx in tx_ids]
            all_txs =dict((k, v) for (k, v) in txs.iteritems() if not v['error'] and v['meta'] and v['TransactionResult'])
            return all_txs, ledger_close_time
        else:
            expanded_txs = self.get_connection().send(
                {
                    "ledger_index": ledger_index,
                    "command": "ledger",
                    "transactions": True,
                    "expand": True
                }
            )

        return expanded_txs['result']['ledger']['transactions'], ledger_close_time
