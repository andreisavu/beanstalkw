
import sys
import logging
import time

from beanstalkw import Worker, AsyncWorker

class PingWorker(AsyncWorker):
    def on_job(self, job):
        print 'ping got:', job.body

        self.conn.use('pong')
        self.conn.put('ping')
        job.done()

        time.sleep(2)

class PongWorker(Worker):
    def on_job(self, job):
        print 'pong got:', job.body

        self.conn.use('ping')
        self.conn.put('pong')
        job.done()

        time.sleep(2)

    def start_game(self):
        self.conn.use('ping')
        self.conn.put('pong')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    PingWorker(tube='ping').start()

    w = PongWorker(tube='pong')
    w.start_game()
    w.run()

    sys.exit(0) # force exit

