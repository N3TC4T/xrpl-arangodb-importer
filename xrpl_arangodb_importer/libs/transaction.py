# transaction class
import json


class Transaction(object):
    def __init__(self, data):
        self.__dict__ = data

        self.__dict__['_key'] = self.hash

        if self.TransactionType == 'Payment':
            self.__dict__['_from'] = "accounts/%s" % self.Account
            self.__dict__['_to'] = "accounts/%s" % self.Destination


    def json(self):
        return self.__dict__
        # return json.dumps(self.__dict__, ensure_ascii=False)


