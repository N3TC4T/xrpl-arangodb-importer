import json
import sys
from time import sleep

import queue
from threading import Thread
from multiprocessing import Queue, Process, Manager, cpu_count

from utils.config import Config

try:
  from pyArango.connection import Connection, CreationError
  from pyArango.collection import Edges
  from pyArango.theExceptions import DocumentNotFoundError, ConnectionError
  from pyArango.collection import BulkOperation as BulkOperation
except ImportError:
  print("pyArango package is required!")
  sys.exit(1)

class BulkInsert(Process):
  def __init__(self, collection=None, queue=None, batchSize=500):
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
class Arangodb(object):
  def __init__(self, fresh=False):
    # get config instance
    self.config = Config()

    # database
    self.host = self.config.get_config_itme('arangodb', 'host')
    self.username = self.config.get_config_itme('arangodb', 'username')
    self.password = self.config.get_config_itme('arangodb', 'password')
    self.databaseName = self.config.get_config_itme('arangodb', 'database')

    self.collectionsList = ['accounts', 'transactions']
    self.collections = {}
    self.edgeCollectionsList=['transactionOutput']
    self.edgeCollections = {}

    # processes
    self.maxProcess = int(cpu_count() / 2)
    self.batchSize = 500
    self.maxQueueSize= self.batchSize * self.maxProcess

    # queue
    self.accountsQueue = Manager().Queue(maxsize=self.maxQueueSize)
    self.transactionsQueue = Manager().Queue(maxsize=self.maxQueueSize)
    self.transactionsOutputQueue = Manager().Queue(maxsize=self.maxQueueSize)

    # tracking
    self.lastStoredSeq = None

    # create connection
    try:
      conn = Connection(
        arangoURL=self.host,
        username=self.username, password=self.password
      )
    except ConnectionError:
      print("Unable to establish connection to the database")
      sys.exit(1)


    # setup database
    try:
      db = conn.createDatabase(name=self.databaseName)
    except CreationError:
      db = conn[self.databaseName]

    if fresh:
      for collection in self.collectionsList + self.edgeCollectionsList:
        if db.hasCollection(collection):
          db.collections[collection].delete()
        db.reload()

    # setup collections
    for collection in self.collectionsList:
      if not db.hasCollection(collection):
        db.createCollection(name=collection, className='Collection')

    # setup edge collections
    for edge in self.edgeCollectionsList:
      if not db.hasCollection(edge):
        db.createCollection(name=edge, className='Edges')


    # set last processed ledger seq
    aql = "FOR tx IN transactions SORT tx.LedgerIndex DESC LIMIT 1 RETURN tx.LedgerIndex"
    queryResult = db.AQLQuery(aql, rawResults=True)
    if len(queryResult) > 0:
      self.lastStoredSeq = queryResult[0]

    # run the threads
    self.processes = []

    for i in range(self.maxProcess):
      self.processes.append(BulkInsert(self.get_connection('accounts'), self.accountsQueue, self.batchSize))
      self.processes.append(BulkInsert(self.get_connection('transactions'), self.transactionsQueue, self.batchSize))
      self.processes.append(BulkInsert(self.get_connection('transactionOutput'), self.transactionsOutputQueue, self.batchSize))

    for t in self.processes:
      t.start()


  def get_connection(self, collection):
    try:
      conn = Connection(
        arangoURL=self.host,
        username=self.username, password=self.password
      )
      db = conn[self.databaseName]
      return db.collections[collection]
    except ConnectionError:
      print("Unable to create connection to the database")
      return None

  def disconnect(self):
    for t in self.processes:
      t.terminate()

  def last_stored_seq(self):
    return self.lastStoredSeq

  def put(self, q, data):
    try:
      q.put(data)
    except queue.Full:
      print("db queue full")
      sleep(0.1)
      self.put(q, data)

  def insert(self,tx):
    try:
      txJson = tx.json()
      txJson['_key'] = tx['hash']

      # put transaction to the queue to be inserted to the db
      self.put(self.transactionsQueue, txJson)

      # calcualte tranasction outputs
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

        # put transaction output to the queue to be inserted to the db
        self.put(self.transactionsOutputQueue, output)

    except Exception as e:
      print(e)



