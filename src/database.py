import json
import sys
from time import sleep

import queue
from multiprocessing import Queue, Process, Manager
from threading import Thread

from pyArango.connection import Connection, CreationError
from pyArango.collection import Edges
from pyArango.theExceptions import DocumentNotFoundError, ConnectionError
from pyArango.collection import BulkOperation as BulkOperation


class BulkInsert(Process):
  def __init__(self, collection=None, queue=None, batchSize=0):
      super().__init__(daemon=True)

      self.queue = queue
      self.collection = collection
      self.batchSize = batchSize

  def run(self):
      with BulkOperation(self.collection, batchSize=self.batchSize) as col:
          while True:
              try:
                  col.createDocument(self.queue.get(block=True,timeout=10)).save(overwriteMode="ignore", waitForSync=False)
              except queue.Empty:
                  break
              except Exception as e:
                  pass

# database class
class Database(object):
    def __init__(self, host = None, username = None, password=None, fresh=False):

        # constants
        self.databaseName = 'XRP_Ledger'
        self.collectionsList = ['accounts', 'transactions']
        self.collections = {}
        self.edgeCollectionsList=['transactionOutput']
        self.edgeCollections = {}

        self.maxProcess = 4
        self.batchSize = 500
        self.maxQueueSize= self.batchSize * self.maxProcess

        self.accountsQueue = Manager().Queue(maxsize=self.maxQueueSize)
        self.transactionsQueue = Manager().Queue(maxsize=self.maxQueueSize)
        self.transactionsOutputQueue = Manager().Queue(maxsize=self.maxQueueSize)

        # create connection
        try:
            self.conn = Connection(
                arangoURL=host,
                username=username, password=password
            )
        except ConnectionError:
            print("Unable to establish connection to the database")
            sys.exit(1)


        # set database
        try:
            self.db = self.conn.createDatabase(name=self.databaseName)
        except CreationError:
            self.db = self.conn[self.databaseName]

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
        self.processes = []

        for i in range(self.maxProcess):
            self.processes.append(BulkInsert(self.collections['accounts'], self.accountsQueue, self.batchSize))
            self.processes.append(BulkInsert(self.collections['transactions'], self.transactionsQueue, self.batchSize))
            self.processes.append(BulkInsert(self.edgeCollections['transactionOutput'], self.transactionsOutputQueue, self.batchSize))

        for t in self.processes:
            t.start()

    def disconnect(self):
        for t in self.processes:
            t.terminate()

        self.conn.disconnectSession()

    def last_saved_seq(self):
        aql = "FOR tx IN transactions SORT tx.LedgerIndex DESC LIMIT 1 RETURN tx.LedgerIndex"
        queryResult = self.db.AQLQuery(aql, rawResults=True)

        if len(queryResult) > 0:
            return queryResult[0]

        return None

    def put(self, q, data):
        try:
          q.put(data)
        except queue.Full:
          sleep(0.1)
          self.put(q, data)

    def insert(self,tx):
        try:
            txJson = tx.json()
            txJson['_key'] = tx['hash']

            self.put(self.transactionsQueue, txJson)

            outputs = tx.getTxOutput()

            for output in outputs:
                # save accounts
                if "_from" in output:
                    self.put(self.accountsQueue, { '_key': output["_from"]})

                if "_to" in output:
                    self.put(self.accountsQueue, { '_key': output["_to"]})

                if "_to" in output and "_from" in output:
                    output["_from"] = "accounts/%s" % output["_from"]
                    output["_to"] = "accounts/%s" % output["_to"]
                    output["transaction"] = "transactions/%s" % tx['hash']

                    self.put(self.transactionsOutputQueue, output)


        except Exception as e:
            print(e)



