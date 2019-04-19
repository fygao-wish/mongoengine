import collections
import greenlet

from pymongo.mongo_client import MongoClient
from motor.motor_tornado import MotorClient, MotorClientSession
from pymongo.read_preferences import ReadPreference

from connection import _get_db, _connections
from document import wait_for_future

__all__ = ['start_session', 'start_transaction']


class ClientSessionContext():
    def __init__(self, session):
        # self._session stores the original session returned from client (ClientSession or MotorClientSession,
        # depending on whether the client is async)
        # self.session stores pymongo.client_session.ClientSession
        self._session = session
        if session.__class__.__name__.endswith('MotorClientSession'):
            # unwrap MotorClientSesssion
            self.session = session.delegate
        else:
            self.session = session

    def __enter__(self):
        # always return/expose pymongo ClientSession
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        # use the original session
        wait_for_future(self._session.end_session())


def start_session(conn_name=None, causal_consistency=True,
            default_transaction_options=None, allow_async=True):
    """
        It's not recommanded to use allow_async parameter.
        sample usage:
        with start_session(conn_name) as session:
            User.update({"id":"1"}, {"$set": {"username": "1"}}, session=session)
    """
    allow_async &= bool(greenlet.getcurrent().parent)
    conn = _connections[conn_name]
    client = conn.async if allow_async else conn.sync
    session = wait_for_future(client.start_session(
        causal_consistency=causal_consistency,
        default_transaction_options=default_transaction_options))
    return ClientSessionContext(session)


class TransactionContext():
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session.__class__.__name__.endswith('MotorClientSession'):
            pymongo_session = self.session.delegate
        else:
            pymongo_session = self.session
        if pymongo_session._in_transaction:
            if exc_val is None:
                wait_for_future(self.session.commit_transaction())
            else:
                wait_for_future(self.session.abort_transaction())


def start_transaction(session, read_concern=None, write_concern=None,
            read_preference=None, allow_async=True):
    """
        It's not recommanded to use allow_async parameter.
        Use find_and_modify inside transaction to avoid stale reads. https://docs.mongodb.com/manual/core/transactions-production-consideration/
        sample usage:
        with start_session(db_name) as session:
            with start_transaction(session):
                User.update({"id":"1"}, {"$set": {"username": "1"}}, session=session)

    """
    allow_async &= bool(greenlet.getcurrent().parent)
    if allow_async:
        if not session.__class__.__name__.endswith('MotorClientSession'):
            # need to wrap pymongo ClientSession
            session = MotorClientSession(session)
    else:
        if session.__class__.__name__.endswith('MotorClientSession'):
            # need to unwrap
            session = session.delegate

    # no need to call wait_for_future
    session.start_transaction(
        read_concern=read_concern,
        write_concern=write_concern,
        read_preference=read_preference)
    return TransactionContext(session)