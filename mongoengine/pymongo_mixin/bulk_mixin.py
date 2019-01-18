import greenlet
import contextlib
import pymongo
import warnings
from bson import ObjectId

from .base import BaseMixin
from ..queryset import OperationError


class WrappedCounter(object):
    def __init__(self):
        self.value = 0

    def inc(self):
        self.value += 1

    def get(self):
        return self.value


class BulkOperationError(OperationError):
    pass


class BulkMixin(BaseMixin):
    BULK_INDEX = "bulk_index"
    BULK_SAVE_OBJECTS = "bulk_save_objects"
    BULK_OP = "bulk_op"

    @classmethod
    def bulk_name(cls, name):
        return "_bulk_%s_%s" % (name, cls.__name__)

    @classmethod
    def get_bulk_attr(cls, name):
        current_greenlet = greenlet.getcurrent()
        if hasattr(current_greenlet, cls.bulk_name(name)):
            return getattr(current_greenlet, cls.bulk_name(name))
        return None

    @classmethod
    def init_bulk_attr(cls, name, default):
        current_greenlet = greenlet.getcurrent()
        if not hasattr(current_greenlet, cls.bulk_name(name)) or \
                getattr(current_greenlet, cls.bulk_name(name)) is None:
            setattr(current_greenlet, cls.bulk_name(name), default)
        return getattr(current_greenlet, cls.bulk_name(name))

    @classmethod
    def clear_bulk_attr(cls, name):
        current_greenlet = greenlet.getcurrent()
        setattr(current_greenlet, cls.bulk_name(name), None)

    @classmethod
    @contextlib.contextmanager
    def bulk(cls, allow_empty=True, unordered=False):
        if cls.get_bulk_attr(cls.BULK_OP) is not None:
            raise RuntimeError('Cannot nest bulk operations')
        try:
            cls.init_bulk_attr(cls.BULK_INDEX, WrappedCounter())
            cls.init_bulk_attr(cls.BULK_SAVE_OBJECTS, dict())
            if unordered:
                cls.init_bulk_attr(
                    cls.BULK_OP, cls._pymongo().initialize_unordered_bulk_op())
            else:
                cls.init_bulk_attr(
                    cls.BULK_OP, cls._pymongo().initialize_ordered_bulk_op())
            yield
            try:
                w = cls._meta.get('write_concern', 1)
                cls.get_bulk_attr(cls.BULK_OP).execute(write_concern={'w': w})

                for object_id, props in cls.get_bulk_attr(cls.BULK_SAVE_OBJECTS).iteritems():
                    instance = props['obj']
                    if instance.id is None:
                        id_field = cls.pk_field()
                        id_name = id_field.name or 'id'
                        instance[id_name] = id_field.to_python(object_id)
            except pymongo.errors.BulkWriteError as e:
                wc_errors = e.details.get('writeConcernErrors')
                # only one write error should occur for an ordered op
                w_error = e.details['writeErrors'][0] \
                    if e.details.get('writeErrors') else None
                if wc_errors:
                    messages = '\n'.join(_['errmsg'] for _ in wc_errors)
                    message = 'Write concern errors for bulk op: %s' % messages
                elif w_error:
                    for object_id, props in cls.get_bulk_attr(cls.BULK_SAVE_OBJECTS):
                        if props['index'] < w_error['index']:
                            instance = props['obj']
                            if instance.id is None:
                                id_field = cls.pk_field()
                                id_name = id_field.name or 'id'
                                instance[id_name] = \
                                    id_field.to_python(object_id)
                    message = 'Write errors for bulk op: %s' % \
                        w_error['errmsg']

                bo_error = BulkOperationError(message)
                bo_error.details = e.details
                if w_error:
                    bo_error.op = w_error['op']
                    bo_error.index = w_error['index']
                raise bo_error
            except pymongo.errors.InvalidOperation as e:
                if 'No operations' in e.message:
                    if allow_empty is None:
                        warnings.warn('Empty bulk operation; use allow_empty')
                    elif allow_empty is False:
                        raise
                    else:
                        pass
                else:
                    raise
            except (pymongo.errors.OperationFailure), err:
                message = u'Could not perform bulk operation (%s)' % err.message
                raise OperationError(message)
        finally:
            cls.clear_bulk_attr(cls.BULK_OP)
            cls.clear_bulk_attr(cls.BULK_INDEX)
            cls.clear_bulk_attr(cls.BULK_SAVE_OBJECTS)

    @classmethod
    def bulk_update(cls, filter, document, upsert=False, multi=True, **kwargs):
        if cls.get_bulk_attr(cls.BULK_OP) is None:
            raise RuntimeError(
                'Cannot do bulk operation outside of bulk context')

        document = cls._transform_value(document, cls, op='$set')
        filter = cls._transform_value(filter, cls)

        if not document:
            raise ValueError("Cannot do empty updates")

        if not filter:
            raise ValueError("Cannot do empty filters")

        filter = cls._update_filter(filter, **kwargs)

        bulk_op = cls.get_bulk_attr(cls.BULK_OP)
        op = bulk_op.find(filter)
        # pymongo's bulk operation support is based on chaining
        if upsert:
            op = op.upsert()
        if multi:
            op.update(document)
        else:
            op.update_one(document)
        cls.get_bulk_attr(cls.BULK_INDEX).inc()

    @classmethod
    def bulk_remove(cls, filter, multi=True, **kwargs):
        if cls.get_bulk_attr(cls.BULK_OP) is None:
            raise RuntimeError(
                'Cannot do bulk operation outside of bulk context')

        filter = cls._transform_value(filter, cls)

        if not filter:
            raise ValueError("Cannot do empty filters")

        filter = cls._update_filter(filter, **kwargs)

        bulk_op = cls.get_bulk_attr(cls.BULK_OP)
        op = bulk_op.find(filter)
        if multi:
            op.remove()
        else:
            op.remove_one()
        cls.get_bulk_attr(cls.BULK_INDEX).inc()

    def bulk_save(self, validate=True):
        cls = self.__class__
        if cls.get_bulk_attr(cls.BULK_OP) is None:
            raise RuntimeError(
                'Cannot do bulk operation outside of bulk context')
        if validate:
            self.validate()
        doc = self.to_mongo()
        id_field = cls.pk_field()
        id_name = id_field.name or 'id'
        bulk_op = cls.get_bulk_attr(cls.BULK_OP)

        if self[id_name] is None:
            object_id = ObjectId()
            doc[id_field.db_field] = id_field.to_mongo(object_id)
        else:
            object_id = self[id_name]

        if cls._meta['hash_field']:
            # id is not yet set on object
            if cls._meta['hash_field'] == cls._meta['id_field']:
                hash_value = object_id
            else:
                hash_value = self[cls._meta['hash_field']]

            self['shard_hash'] = cls._hash(hash_value)
            hash_field = cls._fields[cls._meta['hash_field']]
            doc[hash_field.db_field] = hash_field.to_mongo(self['shard_hash'])

        bulk_op.insert(doc)
        cls.get_bulk_attr(cls.BULK_SAVE_OBJECTS)[object_id] = {
            'index': cls.get_bulk_attr(cls.BULK_INDEX).get(),
            'obj': self
        }
        cls.get_bulk_attr(cls.BULK_INDEX).inc()
        return object_id

    def bulk_update_one(self, document):
        self.bulk_update({self.pk_field().name: getattr(
            self, self.pk_field().name)}, document)

    def bulk_set(self, **kwargs):
        return self.bulk_update_one({'$set': kwargs})

    def bulk_unset(self, **kwargs):
        return self.bulk_update_one({'$unset': kwargs})

    def bulk_inc(self, **kwargs):
        return self.bulk_update_one({'$inc': kwargs})

    def bulk_push(self, **kwargs):
        return self.bulk_update_one({'$push': kwargs})

    def bulk_pull(self, **kwargs):
        return self.bulk_update_one({'$pull': kwargs})

    def bulk_add_to_set(self, **kwargs):
        return self.bulk_update_one({'$addToSet': kwargs})
