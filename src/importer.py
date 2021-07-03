# coding=utf-8
import queue
from time import sleep
from tqdm import tqdm

from multiprocessing import Process, Pool, Queue, Manager

from transaction import Transaction

class FetchWorker(Process):
  def __init__(self, q, source, start_index, end_index):

    self.q = q

    self.source = source

    self.start_index = start_index
    self.end_index = end_index
    self.current_index = start_index

    super().__init__()

  def put(self, index, tx):
    try:
      self.q.put_nowait((index, tx))
    except queue.Full:
      print("Queue is full, try again in 0.1 secounds")
      sleep(0.1)
      self.put(index, tx)

  def run(self):
    while self.current_index < self.end_index:
      index, txs  = self.source.get_transactions(self.current_index)

      for tx in txs:
        self.put(index, tx)

      # process next ledger index
      self.current_index += 1


class ProcessWorker(Process):
  def __init__(self, q, db, tracker):
    self.tracker = tracker
    self.q = q
    self.db = db

    super().__init__()

  def run(self):
    while True:
      try:
        index, tx = self.q.get()
        self.db.insert(Transaction(tx, index))
        self.tracker()
      except Exception as e:
        print(e)
        pass

class Importer():
    def __init__(self, source=None, database=None, logger=None, startLedger=None):
        assert database , "Database need to be pass as to the class"
        assert source , "Source need to be pass as to the class"

        # set database instance
        self.db = database
        # set source instance
        self.source = source

        # number of workers
        self.max_workers = 5

        self.workers = []

        # set starting ledger point
        self.start_index = None
        self.end_index = None

        if startLedger:
            self.start_index = startLedger
        else:
            self.start_index = self.db.last_saved_seq()

        self.pbar = None

    def percent(self, current):
        p = ((self.end_index - current)/self.end_index) * 100
        return int(p)

    def tracker(self):
      if self.pbar:
        self.pbar.update(1)

    def stop(self):
        # terminate workers
        for worker in self.workers:
          worker.terminate()

        # close connection to source
        self.source.close()

    def start(self):
        start, end = self.source.get_ledger_range()

        all_count = self.source.get_transactions_count(start)

        if not self.start_index:
            self.start_index = start

        if not self.end_index:
            self.end_index = end

        print("[!] Start-End Ledger index %s-%s" % (self.start_index, end, ), end='\n')
        print("[!] Using %s workers" % self.max_workers, end='\n\n')


        if all_count:
          self.pbar = tqdm(desc = "[!]", unit="tx", total=all_count, bar_format="{desc}{percentage:3.0f}%|{bar}{r_bar}")

        manager = Manager()
        q = manager.Queue(maxsize=1000000)

        # start workers for processing the transactions from queue
        fetchWorker = FetchWorker(q, self.source, self.start_index, self.end_index)

        self.workers.append(fetchWorker)

        for n in range(self.max_workers):
            processWorker = ProcessWorker(q, self.db, self.tracker)
            self.workers.append(processWorker)


        for w in self.workers:
          w.start()

        for w in self.workers:
          w.join()
