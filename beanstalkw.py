""" Benstalkd Worker class 

An easy way to develop scripts for handling jobs put on a beanstalkd work queue server.

Beanstalk is a simple, fast workqueue service. Its interface is generic, 
but was originally designed for reducing the latency of page views in high-volume 
web applications by running time-consuming tasks asynchronously.

More about beanstlkd: http://kr.github.com/beanstalkd/

Basic Usage:

    from beanstalkw import Worker

    class MyWorker(Worker):
        def on_job(self, job):
            pass

    MyWorker(host='...', port='...', tube='...').run()

Check test.py for an working example.

"""

__license__ = '''
Copyright (C) 2010 Andrei Savu
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import threading
import logging
import time

from beanstalkc import Connection, SocketError

class Job(object):
    """ Wrapper around a job object. Adds done() method as synonym to delete() """
    def __init__(self, obj):
        self.obj = obj

    def __getattr__(self, name):
        return getattr(self.obj, name)

    def done(self):
        return self.obj.delete()


class Worker(object):
    log = logging.getLogger()

    def __init__(self, tube='default', host='localhost', port=11300):
        self.tube = tube
        self.host, self.port = host, port

        try:
            self._connect()

        except SocketError:
            self.log.error('start failed. unable to connect to queue.')
            self.reconnect()

    def _connect(self):
       self.conn = Connection(self.host, self.port)
       self._watch_only(self.tube)

    def _watch_only(self, tube):
        for t in self.conn.watching():
            if t != tube: self.conn.ignore(t)
        self.conn.watch(tube)

    def reconnect(self):
        while True:
            self.log.info('trying to reconnect to work queue.') 
            try:
                self._connect()

                self.log.info('reconnected to queue host=%s port=%d.' % (self.host, self.port))
                break

            except SocketError:
                self.log.error('reconnect failed. waiting 10 seconds before retrying.')
                time.sleep(10)

    def run(self):
        """ Worker main loop. Reserve job and execute. """
        try:
            while True:
                self.cycle()
        except KeyboardInterrupt:
            self.log.info('got exit request. bye bye!')
            pass

    def cycle(self):
        try:
            self.log.info('waiting for jobs.')

            job = self.conn.reserve()

            if job is not None:
                self.log.info('got job with id #%d' % job.jid)
                self.on_job(Job(job))
 
        except SocketError:
            self.reconnect()

        except Exception, e:
            self.log.exception('got unexpected exception when running job')
            pass # nothing else to do. the worker should keep running

    def on_job(self, job):
        """ Handles a new job

        When a new job arrives this method is called. You should override
        this method in the class implementing the worker.
        """
        pass


class AsyncWorker(Worker, threading.Thread):

    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)
        threading.Thread.__init__(self)


