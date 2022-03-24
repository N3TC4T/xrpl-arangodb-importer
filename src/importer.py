# coding=utf-8
from time import sleep, time

import queue
from threading import Thread
from multiprocessing import Process, Pool, Queue, Manager, Value, cpu_count

import progressbar

from transaction import Transaction

class FetchWorker(Thread):
  def __init__(self, q, source, current_index, end_index):
    self.q = q

    self.source = source

    self.current_index = current_index
    self.end_index = end_index

    super().__init__(daemon=True)

  def put(self, tx, ledger_index, close_time):
    try:
      self.q.put((tx, ledger_index, close_time))
    except queue.Full:
      sleep(0.1)
      self.put(index, tx)

  def run(self):
    while True:
      if self.current_index.value > self.end_index:
        break

      txs, close_time  = self.source.get_transactions(self.current_index.value)

      for tx in txs:
        self.put(tx, self.current_index.value, close_time)

      with self.current_index.get_lock():
        self.current_index.value += 1


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
  def __init__(self, source=None, database=None, ledger=None):
    assert database , "Database need to be pass as to the class"
    assert source , "Source need to be pass as to the class"

    # set database instance
    self.db = database
    # set source instance
    self.source = source

    # number of workers
    self.max_workers = int(cpu_count() / 2)
    self.queue_size = 10000
    self.workers = []

    # set starting ledger point
    self.start_index = None
    self.end_index = None

    # tracking current index
    self.current_index = None

    if ledger:
      self.start_index = ledger
    else:
      self.start_index = self.db.last_stored_seq()

      # progress
      self.progress = None
      self.counter = Value('i', 0)

  def tracker(self):
    # with self.counter.get_lock():
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
    # get ledger range from source
    start, end = self.source.get_ledger_range()

    # calcualte all transactions count
    all_count = self.source.get_transactions_count(start)

    # if no ledger start index set use the first index from soruce
    if not self.start_index:
      self.start_index = start

    # if no ledger end index set use last index from soruce
    if not self.end_index:
      self.end_index = end


    print("[!] Start-End Ledger index %s-%s" % (self.start_index, end, ), end='\n')
    print("[!] Using %s workers" % self.max_workers, end='\n\n')

    self.progress = progressbar.ProgressBar(max_value=all_count or progressbar.UnknownLength)

    manager = Manager()
    q = manager.Queue(maxsize=self.queue_size)

    self.current_index = Value('i', self.start_index)

    # start workers for processing the transactions from queue
    for n in range(self.max_workers):
      self.workers.append(FetchWorker(q, self.source, self.current_index, self.end_index))
      self.workers.append(ProcessWorker(q, self.db, self.tracker))


    for w in self.workers:
      w.start()

    for w in self.workers:
      w.join()
