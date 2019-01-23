import logging
import traceback
import pymongo
import time
from .base import BaseMixin
from ..connection import _get_slave_ok
from ..timer import log_slow_event
from ..base import MongoComment

high_offset_logger = logging.getLogger('sweeper.prod.mongodb_high_offset')
execution_timeout_logger = logging.getLogger(
    'sweeper.prod.mongodb_execution_timeout')
_sleep = time.sleep


class ReadMixin(BaseMixin):
    NO_TIMEOUT_DEFAULT = object()
    MAX_AUTO_RECONNECT_TRIES = 6
    AUTO_RECONNECT_SLEEP = 5
    RETRY_MAX_TIME_MS = 5000
    MAX_TIME_MS = 2500
    ALLOW_TIMEOUT_RETRY = True
    INCLUDE_SHARD_KEY = []

    @classmethod
    def _find_raw(cls, filter, fields=None, skip=0, limit=0, sort=None,
                  slave_ok=True, find_one=False, allow_async=True, hint=None,
                  batch_size=10000, excluded_fields=None, max_time_ms=None,
                  comment=None, from_mengine=True, **kwargs):
        is_scatter_gather = cls.is_scatter_gather(filter)
        # HACK [adam May/2/16]: log high-offset queries with sorts to TD. these
        #      queries tend to cause significant load on mongo
        set_comment = False

        if sort and skip > 100000:
            trace = "".join(traceback.format_stack())
            high_offset_logger.info({
                'limit': limit,
                'skip': skip,
                'trace': trace
            })

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

        # in case count passed in instead of limit
        if 'count' in kwargs and limit == 0:
            limit = kwargs['count']

        for i in xrange(cls.MAX_AUTO_RECONNECT_TRIES):
            try:
                set_comment = False

                with log_slow_event('find', cls._meta['collection'], filter):
                    pymongo_collection = cls._pymongo(allow_async).with_options(
                        read_preference=slave_ok.read_pref,
                    )
                    cur = pymongo_collection.find(filter, projection,
                                                  skip=skip, limit=limit,
                                                  sort=sort)

                    # max_time_ms <= 0 means its disabled, None means
                    # use default value, otherwise use the value specified
                    if max_time_ms is None:
                        # if the default value is set to 0, then this feature is
                        # disabled by default
                        if cls.MAX_TIME_MS > 0:
                            cur.max_time_ms(cls.MAX_TIME_MS)
                    elif max_time_ms > 0:
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
                break
            # delay & retry once on AutoReconnect error
            except pymongo.errors.AutoReconnect:
                if i == (cls.MAX_AUTO_RECONNECT_TRIES - 1):
                    raise
                else:
                    _sleep(cls.AUTO_RECONNECT_SLEEP)

    @classmethod
    def explain(cls, filter, fields=None, skip=0, limit=0, sort=None,
                slave_ok=True, excluded_fields=None, max_time_ms=None,
                timeout_value=NO_TIMEOUT_DEFAULT, **kwargs):
        raise Exception("Explain not supported")

    @classmethod
    def find(cls, filter, fields=None, skip=0, limit=0, sort=None,
             slave_ok=True, excluded_fields=None, max_time_ms=None,
             timeout_value=NO_TIMEOUT_DEFAULT, **kwargs):
        for i in xrange(cls.MAX_AUTO_RECONNECT_TRIES):
            cur, set_comment = cls._find_raw(filter, fields, skip, limit, sort,
                                             slave_ok=slave_ok,
                                             excluded_fields=excluded_fields,
                                             max_time_ms=max_time_ms, **kwargs)

            try:
                return [
                    cls._from_augmented_son(d, fields, excluded_fields)
                    for d in cls._iterate_cursor(cur)
                ]
            except pymongo.errors.ExecutionTimeout:
                execution_timeout_logger.info({
                    '_comment': str(cur._Cursor__comment),
                    '_max_time_ms': cur._Cursor__max_time_ms,
                })
                if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or
                                                max_time_ms < cls.MAX_TIME_MS):
                    return cls.find(
                        filter, fields=fields,
                        skip=skip, limit=limit,
                        sort=sort, slave_ok=slave_ok,
                        excluded_fields=excluded_fields,
                        max_time_ms=cls.RETRY_MAX_TIME_MS,
                        timeout_value=timeout_value,
                        **kwargs
                    )
                if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                    return timeout_value
                raise
            except pymongo.errors.AutoReconnect:
                if i == (cls.MAX_AUTO_RECONNECT_TRIES - 1):
                    raise
                else:
                    _sleep(cls.AUTO_RECONNECT_SLEEP)
            finally:
                cls.cleanup_trace(set_comment)

    @classmethod
    def find_iter(cls, filter, fields=None, skip=0, limit=0, sort=None,
                  slave_ok=False, batch_size=10000,
                  excluded_fields=None, max_time_ms=0, **kwargs):
        def _old_find_iter():
            last_doc = None
            cur, set_comment = cls._find_raw(filter, fields, skip, limit,
                                             sort, slave_ok=slave_ok,
                                             batch_size=batch_size,
                                             excluded_fields=excluded_fields,
                                             max_time_ms=max_time_ms, **kwargs)
            try:
                for doc in cls._iterate_cursor(cur):
                    try:
                        last_doc = cls._from_augmented_son(
                            doc, fields, excluded_fields)
                        yield last_doc
                    except pymongo.errors.ExecutionTimeout:
                        execution_timeout_logger.info({
                            '_comment': str(cur._Cursor__comment),
                            '_max_time_ms': cur._Cursor__max_time_ms,
                        })
                        raise
            finally:
                cls.cleanup_trace(set_comment)

        # If the client has been initialized, use the proxy
        for doc in _old_find_iter():
            yield doc

    @classmethod
    def aggregate(cls, pipeline=None, slave_ok='offline', **kwargs):
        slave_ok = _get_slave_ok(slave_ok)
        pymongo_collection = cls._pymongo().with_options(
            read_preference=slave_ok.read_pref
        )
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
    def distinct(cls, filter, key, fields=None, skip=0, limit=0, sort=None,
                 slave_ok=True, excluded_fields=None,
                 max_time_ms=None, timeout_value=NO_TIMEOUT_DEFAULT,
                 **kwargs):
        cur, set_comment = cls._find_raw(filter, fields, skip, limit,
                                         sort, slave_ok=slave_ok,
                                         excluded_fields=excluded_fields,
                                         max_time_ms=max_time_ms, **kwargs)

        try:
            return cur.distinct(cls._transform_key(key, cls)[0])
        except pymongo.errors.ExecutionTimeout:
            execution_timeout_logger.info({
                '_comment': str(cur._Cursor__comment),
                '_max_time_ms': cur._Cursor__max_time_ms,
            })
            if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or
                                            max_time_ms < cls.MAX_TIME_MS):
                return cls.distinct(
                    filter, key, fields=fields,
                    skip=skip, limit=limit,
                    sort=sort, slave_ok=slave_ok,
                    excluded_fields=excluded_fields,
                    max_time_ms=cls.RETRY_MAX_TIME_MS,
                    timeout_value=timeout_value,
                    **kwargs
                )
            if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                return timeout_value
            raise
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def find_one(cls, filter, fields=None, skip=0, sort=None, slave_ok=True,
                 excluded_fields=None, max_time_ms=None,
                 timeout_value=NO_TIMEOUT_DEFAULT, **kwargs):

        cur, set_comment = cls._find_raw(filter, fields, skip=skip, sort=sort,
                                         slave_ok=slave_ok, find_one=True,
                                         excluded_fields=excluded_fields,
                                         max_time_ms=max_time_ms, **kwargs)

        try:
            if cur:
                return cls._from_augmented_son(cur, fields, excluded_fields)
            else:
                return None
        except pymongo.errors.ExecutionTimeout:
            execution_timeout_logger.info({
                '_comment': str(cur._Cursor__comment),
                '_max_time_ms': cur._Cursor__max_time_ms,
            })
            if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or
                                            max_time_ms < cls.MAX_TIME_MS):
                return cls.find_one(
                    filter, fields=fields,
                    skip=skip,
                    sort=sort, slave_ok=slave_ok,
                    excluded_fields=excluded_fields,
                    max_time_ms=cls.RETRY_MAX_TIME_MS,
                    timeout_value=timeout_value,
                    **kwargs
                )
            if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                return timeout_value
            raise
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def find_and_modify(cls, filter, update=None, sort=None, remove=False,
                        new=False, fields=None, upsert=False,
                        excluded_fields=None, skip_transform=False, **kwargs):
        if skip_transform:
            if update is None and not remove:
                raise ValueError("Cannot have empty update and no remove flag")
            if fields or excluded_fields:
                raise ValueError(
                    "Cannot specify fields or excluded fields when using skip_transform=True")
            transformed_fields = None
        else:
            filter = cls._transform_value(filter, cls)
            if update is not None:
                update = cls._transform_value(update, cls, op='$set')
            elif not remove:
                raise ValueError("Cannot have empty update and no remove flag")

            # handle queries with inheritance
            filter = cls._update_filter(filter, **kwargs)
            if sort is None:
                sort = {}
            else:
                new_sort = []
                for f, dir in sort.iteritems():
                    f, _ = cls._transform_key(f, cls)
                    new_sort.append((f, dir))

                sort = new_sort

            transformed_fields = cls._transform_fields(fields, excluded_fields)

        is_scatter_gather = cls.is_scatter_gather(filter)

        set_comment = cls.attach_trace(
            MongoComment.get_query_comment(), is_scatter_gather)

        try:
            with log_slow_event("find_and_modify", cls._meta['collection'], filter):
                result = cls._pymongo().find_and_modify(
                    filter, sort=sort, remove=remove, update=update, new=new,
                    fields=transformed_fields, upsert=upsert, **kwargs
                )
            if result:
                return cls._from_augmented_son(result, fields, excluded_fields)
            else:
                return None
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def count(cls, filter, slave_ok=True, max_time_ms=None,
              timeout_value=NO_TIMEOUT_DEFAULT, **kwargs):
        cur, set_comment = cls._find_raw(filter, slave_ok=slave_ok,
                                         max_time_ms=max_time_ms, **kwargs)
        try:
            for i in xrange(cls.MAX_AUTO_RECONNECT_TRIES):
                try:
                    return cur.count()
                except pymongo.errors.AutoReconnect:
                    if i == (cls.MAX_AUTO_RECONNECT_TRIES - 1):
                        raise
                    else:
                        _sleep(cls.AUTO_RECONNECT_SLEEP)
                except pymongo.errors.ExecutionTimeout:
                    execution_timeout_logger.info({
                        '_comment': str(cur._Cursor__comment),
                        '_max_time_ms': cur._Cursor__max_time_ms,
                    })
                    if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or
                                                    max_time_ms < cls.MAX_TIME_MS):
                        kwargs.pop('comment', None)
                        return cls.count(
                            filter, slave_ok=slave_ok,
                            max_time_ms=cls.RETRY_MAX_TIME_MS,
                            timeout_value=timeout_value,
                            **kwargs
                        )
                    if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                        return timeout_value
                    raise
        finally:
            cls.cleanup_trace(set_comment)

    def reload(self, slave_ok=False):
        """Reloads all attributes from the database.
        .. versionadded:: 0.1.2
        """
        id_field = self._meta['id_field']
        obj = self.__class__.find_one(self._by_id_key(self[id_field]),
                                      slave_ok=slave_ok)
        for field in self._fields:
            setattr(self, field, obj[field])
