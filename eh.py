import multiprocessing
import Queue
import argparse
import os
import time
import shutil
import logging


class LogMe(object):
  def __init__(self, opts, suffix):
    prefix = opts.logs_dir.rstrip('/')
    self.fd = open("%s/%d.log" % (prefix, os.getpid()), 'w')
    self.suffix = suffix
    self('start')

  def __call__(self, fmt, *args):
    try:
      now = time.time()
      self.fd.write("%.6f %s.%s %5i %s " % (now, time.strftime('%I:%M:%S',
        time.gmtime(now)), ("%.6f" % (now-int(now)))[-6:], os.getpid(),
        self.suffix))
      if len(args) > 0:
        msg = fmt % args
      else:
        msg = fmt
      self.fd.write(msg)
      if not msg.endswith('\n'):
        self.fd.write('\n')
      self.fd.flush()
    except Exception as e:
      self.fd.write('oops %s\n' % e)

def qsize_real(queue):
  # Hi Mac!
  try:
    return queue.qsize()
  except:
    return -1






EXIT = 'exit'

class Task(object):
  def __init__(self, i):
    self.i = i
    # Simulate heavy object.
    self.attr = ['DEADBEAF'] * 128 * 200
  def __str__(self):
    return "T(%d)" % self.i
  def busy_wait(self, secs):
    end = time.time() + secs
    while end > time.time():
      for _ in xrange(1000):
        pass

def consume(queue, opts):
  logme = LogMe(opts, 'CONS')
  qsize = lambda: qsize_real(queue)

  while True:
    logme('queue size %d, fetching...', qsize())
    try:
      task = queue.get(timeout=0.1)
      logme('got from queue: %s (remaining: %d)', task, qsize())
    except Queue.Empty:
      logme('queue is empty? (remaining: %d)', qsize())
      continue
    if task == EXIT:
      logme('done because I got sentinel')
      return
    task.busy_wait(opts.cons_time)


def produce(queue, opts, producer_index):
  logme = LogMe(opts, 'PROD')
  qsize = lambda: qsize_real(queue)
  qNotFlushed = lambda: len(queue._buffer)
  for i in xrange(opts.tasks/opts.producers):
    task = Task(i * opts.producers + producer_index)
    task.busy_wait(opts.prod_time)
    logme('putting into queue (size: %d, unsent: %d)', qsize(), qNotFlushed())
    queue.put(task)
    logme('after put in queue (size: %d, unsent: %d)', qsize(), qNotFlushed())


def run(opts):
  assert not os.path.exists(opts.logs_dir)
  os.makedirs(opts.logs_dir)
  try:
    queue = multiprocessing.Queue()
    logging.info("starting %d producers and %d consumer processes",
                 opts.producers, opts.consumers)
    prods = [multiprocessing.Process(target=produce, args=(queue, opts, i))
             for i in xrange(opts.producers)]
    conss = [multiprocessing.Process(target=consume, args=(queue, opts))
             for _ in xrange(opts.consumers)]
    for p in prods + conss:
      p.daemon = True
      p.start()
    logging.info('waiting for producers to finish')
    for p in prods:
      p.join()
    logging.info('adding EXIT items for consumers to finish')
    for _ in conss:
      queue.put(EXIT)
    logging.info('waiting for consumers to finish')
    for p in conss:
      p.join()
    logging.info('all sub-processes stopped.')
    process_logs(opts)
  finally:
    shutil.rmtree(opts.logs_dir)


def process_logs(opts):
  def gen():
    for fname in os.listdir(opts.logs_dir):
      with open('%s/%s' % (opts.logs_dir, fname)) as f:
        for l in f:
          yield l.strip()

  logging.info('processing logs...')
  for l in sorted(gen()):
    print l[l.find(' ') + 1:]
  logging.info('done.')


def main():
  logging.basicConfig(level=logging.DEBUG)
  parser = argparse.ArgumentParser()
  parser.add_argument('--cons-time', dest='cons_time', default=0.015, type=float,
      help='consumption time per item in seconds')
  parser.add_argument('--prod-time', dest='prod_time', default=0.001, type=float,
      help='production time per item in seconds')
  parser.add_argument('-t', '--tasks', default=1000, type=int,
      help='total # of items to produce')
  parser.add_argument('-c', '--consumers', default=5, type=int,
      help='# of consumer processes')
  parser.add_argument('-p', '--producers', default=5, type=int,
      help='# of producer processes')
  parser.add_argument('-l', '--logs-dir', dest='logs_dir', default='/tmp/sux',
      help='dir to store logs; keep it in RAM!')
  opts = parser.parse_args()
  run(opts)

if __name__ == "__main__":
  main()
