import unittest
import pymongo
import threading
import random
import time
from pymongo.write_concern import WriteConcern
from pymongo.errors import ConnectionFailure
from mongoengine.tests.model.testdoc import TestDoc
from mongoengine.connection import connect, clear_all

# fot test
SINGLE = False


class ReadTests(unittest.TestCase):
    def setUp(self):
        connect(db_names=['test'], conn_name='test')

    def _clear(self):
        TestDoc.remove({'test_pk': {'$gt': -1}})

    def _feed_data(self, limit, exception=False):
        with TestDoc.bulk():
            for i in range(limit):
                entry = TestDoc(test_int=i, test_str=str(i),
                                test_pk=i, test_list=[i])
                entry.bulk_save()
            if exception:
                raise Exception()

    @unittest.skipIf(SINGLE, 'single test')
    def test_find(self):
        limit = 10000
        self._clear()
        self._feed_data(limit)
        docs = TestDoc.find({}, limit=100)
        for doc in docs:
            self.assertEquals(doc.test_pk, doc.test_int)
            self.assertEquals(doc.test_pk, int(doc.test_str))
            self.assertEquals(doc.test_pk, doc.test_list[0])
        docs = TestDoc.find({}, limit=100, fields=['test_pk'])
        for doc in docs:
            self.assertEquals(getattr(doc, 'test_int'), None)
            self.assertEquals(getattr(doc, 'test_str'), None)
            self.assertEquals(getattr(doc, 'test_list'), [])
        docs = TestDoc.find({}, limit=100, excluded_fields=[
                            'test_pk', 'test_int', 'test_list'])
        for doc in docs:
            self.assertEquals(getattr(doc, 'test_int'), None)
            self.assertEquals(getattr(doc, 'test_pk'), None)
            self.assertTrue(getattr(doc, 'test_str') is not None)
            self.assertEquals(getattr(doc, 'test_list'), [])

        docs = TestDoc.find({}, skip=10, limit=10, sort=[('test_pk', -1)])
        for index, doc in enumerate(docs):
            self.assertEquals(doc.test_pk, limit - index - 11)

        docs_iter = TestDoc.find_iter({}, batch_size=10, max_time_ms=100)
        for doc in docs_iter:
            self.assertEquals(doc.test_pk, doc.test_int)

    @unittest.skipIf(SINGLE, 'single test')
    def test_distinct(self):
        limit = 100
        self._clear()
        self._feed_data(limit)
        docs = TestDoc.distinct({}, key='test_int')
        self.assertEquals(len(docs), limit)

        TestDoc.update({
            'test_pk': {'$lt': 10}
        }, {
            '$set': {
                'test_int': 1
            }
        })
        docs = TestDoc.distinct({}, key='test_int')
        self.assertEquals(len(docs), limit - 10 + 1)

    @unittest.skipIf(SINGLE, 'single test')
    def test_reload(self):
        self._clear()
        self._feed_data(1)
        doc = TestDoc.find_one({})
        TestDoc.update(
            doc._update_one_key(),
            {
                '$set': {
                    'test_int': 1000
                }
            })
        self.assertEquals(doc.test_int, 0)
        doc.reload()
        self.assertEquals(doc.test_int, 1000)

    @unittest.skipIf(SINGLE, 'single test')
    def test_aggregate(self):
        self._clear()
        self._feed_data(100)
        docs = TestDoc.aggregate([])['result']
        self.assertEquals(len(docs), 100)
        self.assertTrue(type(docs[0]) is dict)

    @unittest.skipIf(SINGLE, 'single test')
    def test_mongo_connect_and_pool(self):
        clear_all()
        TestDoc._pymongo_collection = {}
        import threading
        pool_size = 100
        client = connect(
            conn_name='test_connect',
            db_names=['test'],
            max_pool_size=pool_size,
            waitQueueTimeoutMS=1000,
            allow_async=True,
        ).sync
        self.assertEquals(client.write_concern, WriteConcern(w=1))
        self.assertEquals(client.max_pool_size, pool_size)

        self._clear()
        self._feed_data(50000)
        global total
        total = 0

        def thread_read():
            global total
            cur = threading.current_thread()
            try:
                it = TestDoc.find_iter({}, limit=1000)
                count = 0
                for x in it:
                    count += 1
                    pass
                total += 1
            except ConnectionFailure:
                return
        t_list = []
        for i in xrange(1000):
            t = threading.Thread(target=thread_read, name="%d" % i)
            t.start()
            t_list.append(t)
        for t in t_list:
            t.join()
        print '%d read threads end successfully' % total

    @unittest.skipIf(SINGLE, 'single test')
    def test_read_preference(self):
        self._clear()
        self._feed_data(100)
        TestDoc.find({}, slave_ok='offline')
        TestDoc.find({}, slave_ok=True)
        TestDoc.find({}, slave_ok=False)

    @unittest.skipIf(SINGLE, 'single test')
    def test_count(self):
        self._clear()
        self._feed_data(50000)
        self.assertEquals(TestDoc.count(), 50000)
        self.assertEquals(TestDoc.count({'test_pk': {'$lt': 10}}), 10)
        self.assertEquals(TestDoc.count({'test_pk': {'$lt': 10}}, skip=5), 5)
        self.assertEquals(TestDoc.count({'test_pk': {'$lt': 10}}, limit=3), 3)
        try:
            TestDoc.count({'test_int': {'$lt': 10000}}, max_time_ms=1)
        except Exception:
            pass
        else:
            self.fail()
        TestDoc.count({'test_int': {'$lt': 10}})
        # Should be warned
        # TestDoc.count({'test_int': {'$lt': 10}}, max_time_ms=-1)
        # TestDoc.count({'test_int': {'$lt': 10}}, max_time_ms=20000)
        # That's OK
        TestDoc.count({'test_int': {'$lt': 10}},
                      max_time_ms=-1, slave_ok='offline')

    @unittest.skipIf(SINGLE, 'single test')
    def test_timeout(self):
        self._clear()
        self._feed_data(50000)
        try:
            TestDoc.find_one({}, sort=[('test_int', -1)], max_time_ms=1)
        except Exception:
            pass
        else:
            self.fail()
        # Should be warned
        # TestDoc.find_one({}, sort=[('test_int', -1)], max_time_ms=-1)
        # TestDoc.find({}, sort=[('test_int', -1)], max_time_ms=20000)
        # it = TestDoc.find_iter({}, sort=[('test_int', -1)], max_time_ms=-1)
        # doc = it.next()
        # self.assertEquals(doc.test_int, 49999)

        # That's OK
        TestDoc.find_one({}, sort=[('test_int', -1)],
                         max_time_ms=-1, slave_ok='offline')
        doc = TestDoc.find_one({}, sort=[('test_int', -1)])
        self.assertEquals(doc.test_int, 49999)
        try:
            TestDoc.distinct({}, 'test_int', max_time_ms=1)
        except Exception:
            pass
        else:
            self.fail()

    @unittest.skipIf(SINGLE, 'single test')
    def test_find_batch(self):
        self._clear()
        self._feed_data(50000)
        start = time.time()
        it = TestDoc.find_iter({}, sort=[('test_int', -1)], batch_size=10)
        for _ in it:
            pass
        end = time.time()
        print 'FIND(batch size:10) takes %s MS' % ((end - start) * 1000)
        start = time.time()
        it = TestDoc.find_iter({}, sort=[('test_int', -1)], batch_size=10000)
        for _ in it:
            pass
        end = time.time()
        print 'FIND(batch size:10000) takes %s MS' % ((end - start) * 1000)

    @unittest.skipIf(SINGLE, 'single test')
    def test_retry_robust(self):
        global total
        total = 0

        def random_close():
            random.seed(time.time())
            for _ in xrange(200):
                time.sleep(0.1)
                if random.random() < 0.8:
                   # print 'Clear connection...'
                    clear_all()
                else:
                   # print 'Establish connection...'
                    connect(db_names=['test'])

        def robust_find():
            cur = threading.current_thread()
            start = time.time()
            index = int(cur.getName())
            count = 0
            try:
                TestDoc.find({}, limit=10000)
                # TestDoc.distinct([])
                # TestDoc.distinct({},'test_int')
                # TestDoc.find_one({})
                #TestDoc.count({'test_int': {'$lt': 20000}})
                end = time.time()
                global total
                total += 1
                print 'Thread %s finished in %s MS' % (cur.getName(),
                                                       (end - start) * 1000)
            except Exception:
                print 'Thread %s FIND failed' % cur.getName()

        self._clear()
        self._feed_data(50000)
        clear_all()
        connection_thread = threading.Thread(target=random_close)
        workers = []
        for i in xrange(100):
            t = threading.Thread(name=str(i), target=robust_find)
            time.sleep(0.05)
            t.start()
            workers.append(t)
        connection_thread.start()
        for t in workers:
            t.join()
        connection_thread.join()
        print '%d threads finished successfully' % total


if __name__ == '__main__':
    import logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(logging.StreamHandler())
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    root_logger.handlers[0].setFormatter(formatter)
    suite = unittest.TestLoader().loadTestsFromTestCase(ReadTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
