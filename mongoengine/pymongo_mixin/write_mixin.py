import contextlib
import pymongo
import time
import greenlet
import smtplib
import socket
import sys
import traceback
import logging
import warnings
from bson import SON, ObjectId, DBRef
from .base import BaseMixin
from .bulk_mixin import BulkMixin
from ..base import (ValidationError, MongoComment, get_document,
                    get_embedded_doc_fields,
                    FieldStatus, FieldNotLoadedError)
from ..queryset import OperationError
from ..greenletutil import CLGreenlet, GreenletUtil
from ..timer import log_slow_event
from ..connection import _get_db, _get_slave_ok, _get_proxy_client,\
    _get_proxy_decider, OpClass

OPS_EMAIL = 'ops@wish.com'

class WriteMixin(BulkMixin, BaseMixin):
    @classmethod
    def drop_collection(cls):
        raise Exception('DANGER: drop collection is forbidden')

    @classmethod
    def update(cls, filter, document, upsert=False, multi=True,
               **kwargs):
        # updates behave like set instead of find (None)... this is relevant for
        # setting list values since you're setting the value of the whole list,
        # not an element inside the list (like in find)
        is_scatter_gather = cls.is_scatter_gather(filter)

        document = cls._transform_value(document, cls, op='$set')
        filter = cls._transform_value(filter, cls)

        if cls._bulk_op is not None:
            warnings.warn('Non-bulk update inside bulk operation')

        if not document:
            raise ValueError("Cannot do empty updates")

        if not filter:
            # send email to ops
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.connect(('8.8.8.8', 80))
                ip_address = sock.getsockname()[0]

                # send email to ops
                tb_msg = []
                for file_name, line_num, call_fn, fn_line in \
                        traceback.extract_stack():
                    tb_msg.append('%s / %d / %s / %s' % (file_name, line_num,
                                                         call_fn, fn_line))
                trace_msg = "Subject: [M`ongo] Empty filter Update\n\n%s\n%s" % \
                    (ip_address, '\n'.join(tb_msg))
                smtpObj = smtplib.SMTP('localhost')
                smtpObj.sendmail('ubuntu@localhost', OPS_EMAIL, trace_msg)
            except:
                pass

            raise ValueError("Cannot do empty filters")

        filter = cls._update_filter(filter, **kwargs)
        set_comment = cls.attach_trace(
            MongoComment.get_query_comment(), is_scatter_gather)

        try:
            with log_slow_event("update", cls._meta['collection'], filter):
                result = cls._pymongo().update(filter,
                                               document,
                                               upsert=upsert,
                                               multi=multi,
                                               w=cls._meta['write_concern'],
                                               )
            return result
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def remove(cls, filter, multi=True, **kwargs):
        if not filter:
            raise ValueError("Cannot do empty filters")

        is_scatter_gather = cls.is_scatter_gather(filter)
        set_comment = cls.attach_trace(
            MongoComment.get_query_comment(), is_scatter_gather)
        # transform query
        filter = cls._transform_value(filter, cls)
        filter = cls._update_filter(filter, **kwargs)
        try:
            with log_slow_event("remove", cls._meta['collection'], filter):
                result = cls._pymongo().remove(
                    filter,
                    w=cls._meta['write_concern'],
                    multi=multi
                )
            return result
        finally:
            cls.cleanup_trace(set_comment)

    def save(self, safe=True, force_insert=None, validate=True):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        If ``safe=True`` and the operation is unsuccessful, an
        :class:`~mongoengine.OperationError` will be raised.

        :param safe: check if the operation succeeded before returning
        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
        :param validate: validates the document; set to ``False`` to skip.
        """
        if self.__class__._bulk_op is not None:
            warnings.warn('Non-bulk update inside bulk operation')
        if self._meta['hash_field']:
            # if we're hashing the ID and it hasn't been set yet, autogenerate it
            from ..fields import ObjectIdField
            if self._meta['hash_field'] == self._meta['id_field'] and \
               not self.id and isinstance(self._fields['id'], ObjectIdField):
                self.id = ObjectId()

            self['shard_hash'] = self._hash(self[self._meta['hash_field']])

        if force_insert is None:
            force_insert = self._meta['force_insert']

        if validate:
            self.validate()
        doc = self.to_mongo()
        try:
            w = self._meta.get('write_concern', 1)
            if force_insert or "_id" not in doc:
                collection = self._pymongo()
                object_id = collection.insert(doc, w=w)
            else:
                collection = self._pymongo()
                object_id = collection.save(doc, w=w)
        except (pymongo.errors.OperationFailure), err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)

    def delete(self, safe=True):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param safe: check if the operation succeeded before returning
        """
        id_field = self._meta['id_field']
        object_id = self._fields[id_field].to_mongo(self[id_field])
        try:
            self.remove({id_field: object_id})
        except (pymongo.errors.OperationFailure), err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

    def update_one(self, document, filter=None, upsert=False,
                   criteria=None, comment=None,
                   **kwargs):
        ops = {}

        if not document:
            raise ValueError("Cannot do empty updates")

        # only do in-memory updates if criteria is None since the updates may
        # not be correct otherwise (since we don't know if the criteria is
        # matched)
        if not criteria:
            for operator, operand in document.iteritems():
                # safety check - these updates should only have atomic ops
                if operator[0] != '$':
                    raise ValueError("All updates should be atomic operators")

                if '.' not in operand:
                    for field, new_val in operand.iteritems():
                        # for now, skip doing in-memory sets on dicts
                        if '.' in field:
                            continue

                        try:
                            field_loaded = self.field_is_loaded(field)
                        except KeyError:
                            raise ValueError('Field does not exist')

                        if operator == '$set':
                            ops[field] = new_val
                        elif operator == '$unset':
                            ops[field] = None
                        elif operator == '$inc':
                            if field_loaded:
                                if self[field] is None:
                                    ops[field] = new_val
                                else:
                                    ops[field] = self[field] + new_val
                        elif operator == '$push':
                            if field_loaded:
                                ops[field] = self[field][:] + [new_val]
                        elif operator == '$pushAll':
                            if field_loaded:
                                ops[field] = self[field][:] + new_val
                        elif operator == '$addToSet':
                            if field_loaded:
                                if isinstance(new_val, dict) and '$each' in \
                                   new_val:
                                    vals_to_add = new_val['$each']
                                else:
                                    vals_to_add = [new_val]

                                for val in vals_to_add:
                                    if self[field] is not None and new_val not in self[field]:
                                        ops[field] = self[field][:] + [val]
                                    elif self[field] is None:
                                        ops[field] = [val]

        document = self._transform_value(document, type(self))
        query_filter = self._update_one_key()

        # add in extra criteria, if it exists
        self._allow_unloaded = True
        try:
            for field in self.INCLUDE_SHARD_KEY:
                value = getattr(self, field)
                if value:
                    if criteria is None:
                        criteria = {}
                    criteria[field] = value
        finally:
            self._allow_unloaded = False

        if criteria:
            query_filter.update(criteria)

        if filter:
            query_filter.update(filter)

        if not comment:
            comment = MongoComment.get_query_comment()

        is_scatter_gather = self.is_scatter_gather(
            query_filter)

        query_filter['$comment'] = comment
        query_filter = self._transform_value(query_filter, type(self))
        set_comment = self.attach_trace(comment, is_scatter_gather)
        try:
            with log_slow_event("update_one", self._meta['collection'], filter):
                result = self._pymongo().update(query_filter,
                                                document,
                                                upsert=upsert,
                                                multi=False,
                                                w=self._meta['write_concern'],
                                                )

            # do in-memory updates on the object if the query succeeded
            if result['n'] == 1:
                for field, new_val in ops.iteritems():
                    self[field] = new_val

            return result
        finally:
            self.cleanup_trace(set_comment)

    def set(self, **kwargs):
        return self.update_one({'$set': kwargs})

    def unset(self, **kwargs):
        return self.update_one({'$unset': kwargs})

    def inc(self, **kwargs):
        return self.update_one({'$inc': kwargs})

    def push(self, **kwargs):
        return self.update_one({'$push': kwargs})

    def pull(self, **kwargs):
        return self.update_one({'$pull': kwargs})

    def add_to_set(self, **kwargs):
        return self.update_one({'$addToSet': kwargs})
