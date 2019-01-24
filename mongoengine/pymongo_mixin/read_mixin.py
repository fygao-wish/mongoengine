import logging
import traceback
import pymongo
import time
from retry import retry
from pymongo.read_preferences import ReadPreference
from .base import BaseMixin, RETRY_ERRORS,\
    RETRY_LOGGER
from ..connection import _get_slave_ok
from ..timer import log_slow_event
from ..base import MongoComment


class ReadMixin(BaseMixin):
    MAX_TIME_MS = 5000
    FIND_WARNING_DOCS_LIMIT = 10000

    @classmethod
    def _count(cls, slave_ok=False, filter=None, hint=None, limit=0,
               skip=0, max_time_ms=None):
        slave_ok = _get_slave_ok(slave_ok)
        pymongo_collection = cls._pymongo(read_preference=slave_ok.read_pref)
        max_time_ms = max_time_ms or cls.MAX_TIME_MS
        cls._check_read_max_time_ms(
            'count', max_time_ms, pymongo_collection.read_preference)
        if filter:
            filter = cls._transform_value(filter, cls)
        if hint:
            hint = cls._transform_hint(hint)
        with log_slow_event('find', cls._meta['collection'], filter):
            kwargs_dict = {
                'filter': filter,
                'skip': skip,
                'limit': limit,
            }
            if hint:
                kwargs_dict.update({
                    'hint': hint
                })
            if max_time_ms > 0:
                kwargs_dict.update({
                    'maxTimeMS': max_time_ms
                })
            return pymongo_collection.count(**kwargs_dict)

    @classmethod
    def _find_raw(cls, filter, fields=None, skip=0, limit=0, sort=None,
                  slave_ok=False, find_one=False, allow_async=True, hint=None,
                  batch_size=10000, excluded_fields=None, max_time_ms=None,
                  comment=None, from_mengine=True, **kwargs):
        is_scatter_gather = cls.is_scatter_gather(filter)
        set_comment = False
        # transform query
        filter = cls._transform_value(filter, cls)
        filter = cls._update_filter(filter, **kwargs)

        # transform fields to include
        projection = cls._transform_fields(fields, excluded_fields)

        # transform sort
        if sort:
            new_sort = []
            for f, dir in sort:
                f, _ = cls._transform_key(f, cls)
                new_sort.append((f, dir))
            sort = new_sort

        # grab read preference & tags from slave_ok value
        try:
            slave_ok = _get_slave_ok(slave_ok)
        except KeyError:
            raise ValueError("Invalid slave_ok preference: %s" % slave_ok)

        # do field name transformation on hints
        #
        # the mongodb index {f1: 1, f2: 1} is expressed to pymongo as
        # [ ("field1", 1), ("field2", 1) ]. here we need to transform
        # "field1" to its db_field, etc.
        if hint:
            hint = cls._transform_hint(hint)

        with log_slow_event('find', cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo(
                read_preference=slave_ok.read_pref)
            cur = pymongo_collection.find(filter, projection,
                                          skip=skip, limit=limit,
                                          sort=sort)

            max_time_ms = max_time_ms or cls.MAX_TIME_MS
            cls._check_read_max_time_ms(
                'find', max_time_ms, pymongo_collection.read_preference)

            if max_time_ms > 0:
                cur.max_time_ms(max_time_ms)

            if hint:
                cur.hint(hint)

            if not comment:
                comment = MongoComment.get_query_comment()

            set_comment = cls.attach_trace(comment, is_scatter_gather)

            cur.comment(comment)

            if find_one:
                for result in cur.limit(-1):
                    return result, set_comment
                return None, set_comment
            else:
                cur.batch_size(batch_size)

            return cur, set_comment

    @classmethod
    def explain(cls, filter, fields=None, skip=0, limit=0, sort=None,
                slave_ok=False, excluded_fields=None, max_time_ms=None, **kwargs):
        raise Exception("Explain not supported")

    
    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def find(cls, filter, fields=None, skip=0, limit=0, sort=None,
             slave_ok=False, excluded_fields=None, max_time_ms=None,
             **kwargs):
        cur, set_comment = cls._find_raw(filter, fields, skip, limit, sort,
                                         slave_ok=slave_ok,
                                         excluded_fields=excluded_fields,
                                         max_time_ms=max_time_ms, **kwargs)
        try:
            results = []
            total = 0
            for d in cls._iterate_cursor(cur):
                total += 1
                results.append(cls._from_augmented_son(
                    d, fields, excluded_fields))
                if total == cls.FIND_WARNING_DOCS_LIMIT + 1:
                    logging.getLogger('mongoengine.read.find_warning').warn(
                        'Collection %s: return more than %d docs in one FIND action, '
                        'consider to use FIND_ITER.',
                        cls.__name__,
                        cls.FIND_WARNING_DOCS_LIMIT,
                    )
            return results
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def find_iter(cls, filter, fields=None, skip=0, limit=0, sort=None,
                  slave_ok=False, batch_size=10000,
                  excluded_fields=None, max_time_ms=None, **kwargs):
        cur, set_comment = cls._find_raw(filter, fields, skip, limit,
                                         sort, slave_ok=slave_ok,
                                         batch_size=batch_size,
                                         excluded_fields=excluded_fields,
                                         max_time_ms=max_time_ms, **kwargs)
        try:
            last_doc = None
            for doc in cls._iterate_cursor(cur):
                last_doc = cls._from_augmented_son(
                    doc, fields, excluded_fields)
                yield last_doc
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def aggregate(cls, pipeline=None, slave_ok='offline', **kwargs):
        # TODO max_time_ms: timeout control needed
        slave_ok = _get_slave_ok(slave_ok)
        pymongo_collection = cls._pymongo(read_preference=slave_ok.read_pref)
        result_iter = pymongo_collection.aggregate(pipeline, cursor={})
        if result_iter:
            return {
                'result': [e for e in result_iter],
                'ok': 1.0
            }
        else:
            return {
                'result': [],
                'ok': 1.0
            }

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def distinct(cls, filter, key, fields=None, skip=0, limit=0, sort=None,
                 slave_ok=False, excluded_fields=None,
                 max_time_ms=None,
                 **kwargs):
        cur, set_comment = cls._find_raw(filter, fields, skip, limit,
                                         sort, slave_ok=slave_ok,
                                         excluded_fields=excluded_fields,
                                         max_time_ms=max_time_ms, **kwargs)
        try:
            return cur.distinct(cls._transform_key(key, cls)[0])
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def find_one(cls, filter, fields=None, skip=0, sort=None, slave_ok=False,
                 excluded_fields=None, max_time_ms=None, **kwargs):
        doc, set_comment = cls._find_raw(filter, fields, skip=skip, sort=sort,
                                         slave_ok=slave_ok, find_one=True,
                                         excluded_fields=excluded_fields,
                                         max_time_ms=max_time_ms, **kwargs)
        try:
            if doc:
                return cls._from_augmented_son(doc, fields, excluded_fields)
            else:
                return None
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def count(cls, filter=None, slave_ok=True, max_time_ms=None,
              skip=0, limit=0, hint=None, **kwargs):
        return cls._count(filter=filter, slave_ok=slave_ok,
                          max_time_ms=max_time_ms,
                          hint=hint,
                          skip=skip, limit=limit)

    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def reload(self, slave_ok=False):
        """Reloads all attributes from the database.
        .. versionadded:: 0.1.2
        """
        id_field = self._meta['id_field']
        obj = self.__class__.find_one(self._by_id_key(self[id_field]),
                                      slave_ok=slave_ok)
        for field in self._fields:
            setattr(self, field, obj[field])
