import unittest
import subprocess
from pymongo.write_concern import WriteConcern
from pymongo.errors import ConnectionFailure
from mongoengine.tests.model.testdoc import TestDoc
from mongoengine.connection import connect, clear_all


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

        TestDoc.ALLOW_TIMEOUT_RETRY = False
        docs = TestDoc.find({}, max_time_ms=1, timeout_value=TestDoc(
            test_pk=101, test_int=101))
        self.assertEquals(docs.test_pk, 101)

        docs_iter = TestDoc.find_iter({}, batch_size=10, max_time_ms=100)
        for doc in docs_iter:
            self.assertEquals(doc.test_pk, doc.test_int)
        TestDoc.ALLOW_TIMEOUT_RETRY = True

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

    def test_aggregate(self):
        self._clear()
        self._feed_data(100)
        docs = TestDoc.aggregate([])['result']
        self.assertEquals(len(docs), 100)
        self.assertTrue(type(docs[0]) is dict)

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

    def test_read_preference(self):
        self._clear()
        self._feed_data(100)
        TestDoc.find({}, slave_ok='offline')
        TestDoc.find({}, slave_ok=True)
        TestDoc.find({}, slave_ok=False)

    def test_count(self):
        self._clear()
        self._feed_data(100)
        self.assertEquals(TestDoc.count(), 100)
        self.assertEquals(TestDoc.count({'test_pk': {'$lt': 10}}), 10)
        self.assertEquals(TestDoc.count({'test_pk': {'$lt': 10}}, skip=5), 5)
        self.assertEquals(TestDoc.count({'test_pk': {'$lt': 10}}, limit=3), 3)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(ReadTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
