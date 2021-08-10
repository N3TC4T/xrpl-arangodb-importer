# coding=utf-8
import queue
import progressbar
from time import sleep

from threading import Thread
from multiprocessing import Process, Pool, Queue, Manager, Value, cpu_count

from transaction import Transaction

class FetchWorker(Thread):
  def __init__(self, q, source, start_index, end_index):

    self.q = q

    self.source = source

    self.start_index = start_index
    self.end_index = end_index
    self.current_index = start_index

    super().__init__(daemon=True)

  def put(self, tx, ledger_index, close_time):
    try:
      self.q.put((tx, ledger_index, close_time))
    except queue.Full:
      sleep(0.1)
      self.put(index, tx)

  def run(self):
    while self.current_index <= self.end_index:
      txs, close_time  = self.source.get_transactions(self.current_index)

      for tx in txs:
        self.put(tx, self.current_index, close_time)

      # process next ledger index
      self.current_index += 1


class ProcessWorker(Process):
  def __init__(self, q, db, tracker):
    self.tracker = tracker
    self.q = q
    self.db = db

    super().__init__(daemon=True)

  def run(self):
    while True:
      try:
        tx, ledger_index,close_time = self.q.get(block=True)
        self.db.insert(Transaction(tx, ledger_index, close_time))
        self.tracker()
      except Exception as e:
        print(e)
        with open('errors.txt', 'a') as f:
          f.write(f'{e}\n')
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
        self.max_workers = cpu_count()

        self.queue_size = 10000

        self.workers = []

        # set starting ledger point
        self.start_index = None
        self.end_index = None

        if startLedger:
            self.start_index = startLedger
        else:
            self.start_index = self.db.last_saved_seq()

        self.progress = None
        self.counter = Value('i', 0)

    def tracker(self):
        with self.counter.get_lock():
            self.counter.value += 1
        self.progress.update(self.counter.value)

    def stop(self):
        # terminate workers
        for worker in self.workers:
            if not isinstance(worker, FetchWorker):
                worker.terminate()

        self.db.disconnect()
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

        self.progress = progressbar.ProgressBar(max_value=all_count or progressbar.UnknownLength)

        q = Manager().Queue(maxsize=self.queue_size)

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
