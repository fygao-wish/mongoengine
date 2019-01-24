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
from pymongo.write_concern import WriteConcern
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
                pymongo_collection = cls._pymongo(
                    write_concern=WriteConcern(w=cls._meta['write_concern']))
                if multi:
                    result = pymongo_collection.update_many(
                        filter, document, upsert=upsert)
                else:
                    result = pymongo_collection.update_one(
                        filter, document, upsert=upsert)
            result_dict = {
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'upserted_id': result.upserted_id,
            }
            result_dict.update(result.raw_result)
            return result_dict
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

        from pymongo.collection import ReturnDocument
        try:
            with log_slow_event("find_and_modify", cls._meta['collection'], filter):
                pymongo_collection = cls._pymongo(
                    write_concern=WriteConcern(w=cls._meta['write_concern']))
                if remove:
                    result = pymongo_collection.find_one_and_delete(
                        filter,
                        sort=sort,
                        projection=transformed_fields
                    )
                else:
                    result = pymongo_collection.find_one_and_update(
                        filter, update,
                        sort=sort,
                        projection=transformed_fields,
                        upsert=upsert,
                        return_document=ReturnDocument.AFTER if new else
                        ReturnDocument.BEFORE
                    )
            if result:
                return cls._from_augmented_son(result, fields, excluded_fields)
            else:
                return None
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
                pymongo_collection = cls._pymongo(
                    write_concern=WriteConcern(w=cls._meta['write_concern']))
                if multi:
                    result = pymongo_collection.delete_many(filter)
                else:
                    result = pymongo_collection.delete_one(filter)
            result_dict = {
                'deleted_count': result.deleted_count,
            }
            result_dict.update(result.raw_result)
            return result_dict
        finally:
            cls.cleanup_trace(set_comment)

    def save(self, force_insert=None, validate=True, **kwargs):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
        :param validate: validates the document; set to ``False`` to skip.
        """
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
            collection = self._pymongo(write_concern=WriteConcern(w=w))
            if force_insert or "_id" not in doc:
                pk_value = collection.insert_one(doc).inserted_id
            else:
                collection.replace_one({'_id': doc['_id']}, doc)
                pk_value = doc['_id']
        except (pymongo.errors.OperationFailure), err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(pk_value)
        return pk_value

    def delete(self, **kwargs):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

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
                        elif operator == '$pull':
                            if field_loaded and new_val in self[field][:]:
                                ops[field] = self[field][:]
                                ops[field].remove(new_val)
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
                pymongo_collection = self._pymongo(
                    write_concern=WriteConcern(w=self._meta['write_concern']))
                result = pymongo_collection.update_one(query_filter,
                                                       document,
                                                       upsert=upsert)
            result_dict = {
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
            }
            result_dict.update(result.raw_result)
            # do in-memory updates on the object if the query succeeded
            if result_dict['n'] == 1:
                for field, new_val in ops.iteritems():
                    self[field] = new_val

            return result_dict
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
