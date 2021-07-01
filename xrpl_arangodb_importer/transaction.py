# transaction class
import json
from decimal import Decimal

from xrpl.core.binarycodec import decode

from parser.flags import txFlags, AccountFlags
from parser.meta import Meta
from parser.hex import toHex, fromHex
from parser.deserialize import deser


class Transaction(dict):
    def __init__(self, tx, ledger_index):
        if 'RawTxn' in tx:
            self['hash'] = tx['TransID']

            for key, value in deser(tx["RawTxn"]).items():
                setattr(self, key, value)

            setattr(self, 'metaData', decode(toHex(tx["TxnMeta"])))
        else:
            for key, value in tx.items():
                setattr(self, key, value)

        setattr(self, 'LedgerIndex', ledger_index)


    def __getattr__(self,key):
        if hasattr(self, key):
            return getattr(self, key)()
        return self[key]

    def __setattr__(self,key,value):
        _property = getattr(self.__class__, key, None)
        if isinstance(_property, property):
            if _property.fset is None:
                raise AttributeError("can't set attribute, forgot to set setter?")
            _property.fset(self, value)
        else:
            self[key] = value


    @property
    def metaData(self):
        return self["metaData"]

    @metaData.setter
    def metaData(self, value):
        self["metaData"] = value


    @property
    def Memos(self):
        return self["Memos"]

    @Memos.setter
    def Memos(self, value):
        memos = []
        for m in value:
            memo = {k: fromHex(v)  for k, v in m["Memo"].items()}
            memos.append(memo)

        self["Memos"] = memos

    @property
    def Fee(self):
        return self["Fee"]

    @Fee.setter
    def Fee(self, value):
        self["Fee"] =  Decimal(value) / Decimal(1000000)

    @property
    def LedgerIndex(self):
        return self["LedgerIndex"]

    @LedgerIndex.setter
    def LedgerIndex(self, value):
        self["LedgerIndex"] =  int(value)



    @property
    def Amount(self):
        return self["Amount"]

    @Amount.setter
    def Amount(self, value):
        # xrp to drops
        if isinstance(value, str):
            self["Amount"] = {
                'currency': 'XRP',
                'value': str(Decimal(value) / Decimal(1000000))
            }
        else:
            self["Amount"] = value

    @property
    def Flags(self):
        return self["Flags"]

    @Flags.setter
    def Flags(self, value):
        flags = {}

        try:
            flagsList = {
                'Account': AccountFlags,
                'AccountSet': txFlags['AccountSet'],
                'TrustSet': txFlags['TrustSet'],
                'OfferCreate': txFlags['OfferCreate'],
                'Payment': txFlags['Payment'],
                'PaymentChannelClaim': txFlags['PaymentChannelClaim'],
            }[self["TransactionType"]]

            for flagName in flagsList:
                if value & flagsList[flagName]:
                    flags[flagName] = True
                else:
                    flags[flagName] = False

        except KeyError:
            pass

        for flagName in txFlags["Universal"]:
            if value & txFlags["Universal"][flagName] :
                flags[flagName] = True;
            else:
                flags[flagName] = False;

        self["Flags"] = flags


    def getTxOutput(self):

        balanceChanges = Meta(self.metaData).parseBalanceChanges()

        # remove fee from sender balance change
        for idx, changes in enumerate(balanceChanges):
            if changes["address"] == self["Account"] and changes["currency"] == 'XRP' and changes["value"].is_signed():
                balanceChanges[idx]["value"] = changes["value"] + self["Fee"]
                # if zero remove it
                if balanceChanges[idx]["value"].is_zero():
                    balanceChanges.remove(balanceChanges[idx])

        # check if any value changed
        if len(balanceChanges) == 0:
            return [{"_from": self["Account"]}]

        outputs = []

        for changes in balanceChanges:
            output = {}

            value = changes.get("value")
            currency = changes.get("currency")
            address = changes.get("address")

            output["value"] = str(abs(value))
            output["currency"] = currency

            if changes.get('issuer'):
                output['counterparty'] = changes.get('issuer')

            other = next((x for x in balanceChanges if x["value"] == Decimal(value * -1) and x["currency"] == currency), None)

            if other is not None:
                # remove from list
                balanceChanges.remove(other)

                output["_from"] = address if value.is_signed() else other["address"]
                output["_to"] = address if not value.is_signed() else other["address"]
            else:
                output["_from"] = address
                output["_to"] = address

            outputs.append(output)

        return outputs

    def json(self):
        return dict.copy(self)
        # return json.loads(json.dumps(self, ensure_ascii=False))


