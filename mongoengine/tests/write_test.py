import unittest

from mongoengine.tests.model.testdoc import TestDoc
from mongoengine.connection import connect


class WriteTests(unittest.TestCase):
    def setUp(self):
        connect(
            db_names=['test'],
        )

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

    def test_save(self):
        self._clear()
        self._feed_data(10)
        self.assertEquals(TestDoc.count({}), 10)

    def test_delete(self):
        self._clear()
        self._feed_data(10)
        docs = TestDoc.find({})
        for doc in docs:
            if doc.test_pk < 5:
                doc.delete()
        self.assertEquals(TestDoc.count({}), 5)

    def test_update(self):
        self._clear()
        self._feed_data(100)
        result = TestDoc.update({'test_pk': {'$gt': -1}}, {
            '$set': {
                'test_int': 1000
            }
        })
        self.assertEquals(TestDoc.count({'test_int': 1000}), 100)
        self.assertEquals(result['nModified'], 100)
        result = TestDoc.update({'test_pk': {'$gt': -1}}, {
            '$set': {
                'test_int': 1000 * 2
            }
        }, multi=False)
        self.assertEquals(TestDoc.count({'test_int': 1000 * 2}), 1)
        self.assertEquals(result['nModified'], 1)
        result = TestDoc.update({'test_pk': 101}, {
            '$set': {
                'test_int': 1000 * 3
            }
        }, upsert=True)
        self.assertEquals(TestDoc.count({}), 101)

    def test_remove(self):
        self._clear()
        self._feed_data(100)
        result = TestDoc.remove({'test_pk': {'$lt': 50}})
        self.assertEquals(result['n'], 50)
        self.assertEquals(TestDoc.count({}), 50)
        result = TestDoc.remove({'test_pk': {'$gte': 50}}, multi=False)
        self.assertEquals(result['n'], 1)
        self.assertEquals(TestDoc.count({}), 49)

    def test_update_one(self):
        self._clear()
        self._feed_data(100)
        docs = TestDoc.find({})
        for doc in docs:
            if doc.test_pk < 10:
                doc.set(test_int=doc.test_pk * doc.test_pk)
            elif doc.test_pk < 20:
                doc.unset(test_int=True)
            elif doc.test_pk < 30:
                doc.inc(test_int=2)
            elif doc.test_pk < 40:
                doc.push(test_list=1000)
            elif doc.test_pk < 50:
                doc.pull(test_list=doc.test_pk)
            else:
                doc.add_to_set(test_list=doc.test_pk * doc.test_pk)
        docs = TestDoc.find({})
        count1 = count2 = count3 = count4 = count5 = count6 = 0
        for doc in docs:
            if doc.test_int == doc.test_pk * doc.test_pk:
                count1 += 1
            elif doc.test_int is None:
                count2 += 1
            elif doc.test_int == doc.test_pk + 2:
                count3 += 1
            elif 1000 in doc.test_list:
                count4 += 1
            elif len(doc.test_list) == 0:
                count5 += 1
            elif doc.test_pk * doc.test_pk in doc.test_list:
                count6 += 1
        self.assertEquals(count1, 10)
        self.assertEquals(count2, 10)
        self.assertEquals(count3, 10)
        self.assertEquals(count4, 10)
        self.assertEquals(count5, 10)
        self.assertEquals(count6, 50)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(WriteTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
