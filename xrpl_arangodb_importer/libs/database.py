import json
import sys
from pyArango.connection import Connection, CreationError
from pyArango.collection import Edges
from pyArango.theExceptions import DocumentNotFoundError, ConnectionError

from .transaction import Transaction


# database class
class Database(object):
    def __init__(self, host, username, password, fresh=False):

        # constants
        self.databaseName = 'XRP_Ledger'
        self.collectionsList = ['accounts']
        self.collections = {}
        self.edgeCollectionsList=['payment']
        self.edgeCollections = {}

        # create connection
        try:
            conn = Connection(
                arangoURL=host,
                username=username, password=password
            )
        except ConnectionError:
            print("Unable to establish connection to the database")
            sys.exit(1)


        # set database
        try:
            self.db = conn.createDatabase(name=self.databaseName)
        except CreationError:
            self.db = conn[self.databaseName]

        # set collections
        for collection in self.collectionsList:
            if not self.db.hasCollection(collection):
                self.collections[collection] = self.db.createCollection(name=collection)
            else:
                self.collections[collection] = self.db.collections[collection]



        # set edge collections
        for edge in self.edgeCollectionsList:
            if not self.db.hasCollection(edge):
                self.edgeCollections[edge] = self.db.createCollection(name=edge, className='Edges')
            else:
                self.edgeCollections[edge] = self.db.collections[edge]


    def save_account(self, address):
        try:
            acc = {}
            acc['_key'] = address
            self.collections['accounts'].createDocument(acc).save()
        except Exception:
            pass

    def save_payment(self, tx):
        self.save_account(tx.Account)
        self.save_account(tx.Destination)

        try:
            self.edgeCollections['payment'].createDocument(tx.json()).save()
        except Exception:
            pass



    def insert(self,_tx):

        try:
            tx = Transaction(_tx)

            # add to tranasctions collection
            if tx.TransactionType == 'Payment' :
                self.save_payment(tx)

        except Exception as e:
            print(e)



