# coding=utf-8
from xrpl_websocket import Client
from benedict import benedict

from .transaction import Transaction

class Connection(Client):
    def __init__(self, server=None):
        super(self.__class__, self).__init__(
            server=server
        )

        # connect to the websocket
        self.connect(nowait=False)

    def get_ledger_range(self):
        r = self.send(command="server_info")

        complete_ledgers= r['result']['info']['complete_ledgers']
        start, sp, end = complete_ledgers.partition('-')

        return start, end


    def get_transactions(self, ledger_index):
        # first get transaction counts
        raw = self.send(
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
            txs = [self.scoket.send(command="tx", transaction=tx) for tx in tx_ids]
            all_txs =dict((k, v) for (k, v) in txs.iteritems() if not v['error'] and v['meta'] and v['TransactionResult'])
            return ledger_index, all_txs
        else:
            expanded_txs = self.send(
                {
                    "ledger_index": ledger_index,
                    "command": "ledger",
                    "transactions": True,
                    "expand": True
                }
            )


            expanded_txs = benedict(expanded_txs)
            return ledger_index, expanded_txs['result.ledger.transactions']
