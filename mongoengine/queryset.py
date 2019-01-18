from connection import _get_db

import pprint
import pymongo
import pymongo.errors
import re
import copy
import itertools
import time
import greenlet

__all__ = ['InvalidQueryError', 'InvalidCollectionError']


class DoesNotExist(Exception):
    pass


class MultipleObjectsReturned(Exception):
    pass


class InvalidQueryError(Exception):
    pass


class OperationError(Exception):
    pass


class InvalidCollectionError(Exception):
    pass
