from gevent import monkey
monkey.patch_all()

import gevent
import unittest
import time
import threading
import greenlet
from mongoengine.greenletutil import CLGreenlet
from mongoengine.tests.model.testdoc import TestDoc
from mongoengine.connection import connect

CONCURRENCY = 1000
LENGTH = 50000
SINGLE = False


def _read_task(index):
    start = time.time()
    it = TestDoc.find_iter({'test_pk':
                            {
                                '$gte': index * (LENGTH / CONCURRENCY),
                                '$lt': (index + 1) * (LENGTH / CONCURRENCY)
                            }
                            })
    for _ in it:
        pass
    # from pymongo.mongo_client import MongoClient
    # client = MongoClient()
    # coll = client.test['testdoc']
    # cur = coll.find({})
    # cur.batch_size(10000)
    # for _ in cur:
    #     pass
    # print 'Task %s done with time %s MS' % (index, (time.time() - start) * 1000)


def _read_task_concurrency(index):
    cur = greenlet.getcurrent()
    start = time.time()
    it = TestDoc.find_iter({'test_pk':
                            {
                                '$gte': index * (LENGTH / CONCURRENCY),
                                '$lt': (index + 1) * (LENGTH / CONCURRENCY)
                            }
                            })
    for _ in it:
        cur.parent.switch()
    # print 'Task %s done with time %s MS' % (index, (time.time() - start) * 1000)


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        connect(
            db_names=['test'],
        )
        self._clear()
        self._feed_data(LENGTH)

    def _clear(self):
        TestDoc.remove({'test_pk': {'$gt': -1000}})

    def _feed_data(self, limit, exception=False):
        with TestDoc.bulk():
            for i in range(limit):
                entry = TestDoc(test_int=i, test_str=str(i),
                                test_pk=i, test_list=[i])
                entry.bulk_save()
            if exception:
                raise Exception()

    @unittest.skipIf(SINGLE, 'single test')
    def test_gevent(self):
        jobs = [gevent.spawn(_read_task, i) for i in xrange(CONCURRENCY)]
        start = time.time()
        gevent.joinall(jobs)
        print 'All done with time %s MS' % ((time.time() - start) * 1000)

    @unittest.skipIf(SINGLE, 'single test')
    def test_thread(self):
        jobs = [threading.Thread(target=_read_task, args=[i])
                for i in xrange(CONCURRENCY)]
        start = time.time()
        [t.start() for t in jobs]
        [t.join() for t in jobs]
        print 'All done with time %s MS' % ((time.time() - start) * 1000)

    def test_greenlet(self):
        jobs = [CLGreenlet(_read_task_concurrency)
                for _ in xrange(CONCURRENCY)]
        stop = False
        start = time.time()
        while not stop:
            stop = True
            for index, job in enumerate(jobs):
                if not job.dead:
                    stop = False
                    job.switch(index)
        print 'All done with time %s MS' % ((time.time() - start) * 1000)

    def test_sync(self):
        start = time.time()
        for i in xrange(CONCURRENCY):
            _read_task(i)
        print 'All done with time %s MS' % ((time.time() - start) * 1000)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(ConcurrencyTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
