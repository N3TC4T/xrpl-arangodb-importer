import json
import sys, time

from multiprocessing import Queue
from threading import Thread

from pyArango.connection import Connection, CreationError
from pyArango.collection import Edges
from pyArango.theExceptions import DocumentNotFoundError, ConnectionError
from pyArango.collection import BulkOperation as BulkOperation


# database class
class Database(object):
    def __init__(self, host = None, username = None, password=None, fresh=False):

        # constants
        self.databaseName = 'XRP_Ledger'
        self.collectionsList = ['accounts', 'transactions']
        self.collections = {}
        self.edgeCollectionsList=['transactionOutput']
        self.edgeCollections = {}

        self.batchSize = 500

        self.accountsQueue = Queue()
        self.transactionsQueue = Queue()
        self.transactionsOutputQueue = Queue()

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

        if fresh:
            for collection in self.collectionsList + self.edgeCollectionsList:
                if self.db.hasCollection(collection):
                    self.db.collections[collection].delete()
            self.db.reload()

        # set collections
        for collection in self.collectionsList:
            if not self.db.hasCollection(collection):
                self.collections[collection] = self.db.createCollection(name=collection, className='Collection')
            else:
                self.collections[collection] = self.db.collections[collection]

        # set edge collections
        for edge in self.edgeCollectionsList:
            if not self.db.hasCollection(edge):
                self.edgeCollections[edge] = self.db.createCollection(name=edge, className='Edges')
            else:
                self.edgeCollections[edge] = self.db.collections[edge]


        # run the threads
        bulkThreads = []

        bulkThreads.append(Thread(target=self.save_account, args=(self.accountsQueue, )))
        bulkThreads.append(Thread(target=self.save_tx_output, args=(self.transactionsOutputQueue, )))
        bulkThreads.append(Thread(target=self.save_transaction, args=(self.transactionsQueue, )))

        for t in bulkThreads:
            t.setDaemon(True)
            t.start()

    def last_saved_seq(self):
        aql = "FOR tx IN transactions SORT tx.LedgerIndex DESC LIMIT 1 RETURN tx.LedgerIndex"
        queryResult = self.db.AQLQuery(aql, rawResults=True)

        if len(queryResult) > 0:
            return queryResult[0]

        return None

    def save_account(self, q):
        with BulkOperation(self.collections['accounts'], batchSize=self.batchSize) as col:
            while True:
                if not q.empty():
                    try:
                        address = q.get()
                        acc = {}
                        acc['_key'] = address
                        col.createDocument(acc).save(overwriteMode="ignore")
                    except Exception as e:
                        print("save_account", e)
                        pass

    def save_tx_output(self, q):
        with BulkOperation(self.edgeCollections['transactionOutput'], batchSize=self.batchSize) as col:
            while True:
                if not q.empty():
                    try:
                        output = q.get()
                        col.createDocument(output).save()
                    except Exception as e:
                        print("save_tx_output", e)
                        pass

    def save_transaction(self, q):
        with BulkOperation(self.collections['transactions'], batchSize=self.batchSize) as col:
            while True:
                if not q.empty():
                    try:
                        tx = q.get()
                        col.createDocument(tx).save()
                    except Exception as e:
                        print("save_transaction", e)
                        pass

    def insert(self,tx):
        try:
            txJson = tx.json()
            txJson['_key'] = tx['hash']

            self.transactionsQueue.put(txJson)

            outputs = tx.getTxOutput()


            for output in outputs:
                # save accounts
                if "_from" in output:
                    self.accountsQueue.put(output["_from"])

                if "_to" in output:
                    self.accountsQueue.put(output["_to"])

                if "_to" in output and "_from" in output:
                    output["_from"] = "accounts/%s" % output["_from"]
                    output["_to"] = "accounts/%s" % output["_to"]
                    output["transaction"] = "transactions/%s" % tx['hash']

                    self.transactionsOutputQueue.put(output)


        except Exception as e:
            print(e)



