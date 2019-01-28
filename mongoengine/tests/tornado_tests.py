import tornado.ioloop
import tornado.web
import time
from tornado.gen import coroutine, Task
from mongoengine.tests.model.testdoc import TestDoc
from mongoengine.connection import connect
import boto3

class MainHandler(tornado.web.RequestHandler):
    @coroutine
    def _get_docs(self):
        it = TestDoc.find_iter({})
        count = 0
        for _ in it:
            count += 1
            if count % 10000 == 0:
                yield
                print count

    @coroutine
    def get(self):
        start = time.time()
        yield self._get_docs()
        end = time.time()
        self.write({'time': (end - start) * 1000})


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])


if __name__ == "__main__":
    connect(db_names=['test'])
    TestDoc.remove({'test_pk': {'$gt': -1}})
    with TestDoc.bulk():
        for i in xrange(50000):
            doc = TestDoc(test_pk=i, test_int=i,
                          test_str=str(i), test_list=[i])
            doc.bulk_save()
    app = make_app()
    app.listen(8888)
    print 'Starting server...'
    tornado.ioloop.IOLoop.current().start()
