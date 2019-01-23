import contextlib
import greenlet
import warnings
import time
import pymongo
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference
from bson import SON, DBRef, ObjectId
from ..base import FieldStatus, BaseDocument, MongoComment, get_document, \
    ValidationError, get_embedded_doc_fields
from ..greenletutil import CLGreenlet, GreenletUtil
from ..timer import log_slow_event

RETRY_PYMONGO_ERRORS = (pymongo.errors.PyMongoError,)


class CLSContext(object):
    pass


class BaseMixin(object):
    @classmethod
    def pk_field(cls):
        return cls._fields[cls._meta['id_field']]

    def _update_one_key(self):
        """
            Designed to be overloaded in children when a shard key needs to be
            included in update_one() queries
        """
        key = {'_id': self.id}

        if self._meta['hash_field'] and self._meta['sharded']:
            key['shard_hash'] = self._hash(self[self._meta['hash_field']])

        return key

    @classmethod
    def _iterate_cursor(cls, cur):
        """
            Iterates over a cursor, gracefully handling AutoReconnect exceptions
        """
        while True:
            with log_slow_event('getmore', cur.collection.name, None):
                # the StopIteration from .next() will bubble up and kill
                # this while loop
                doc = cur.next()

                # handle pymongo letting an error document slip through
                # (T18431 / CS-22167). convert it into an exception
                if '$err' in doc:
                    err_code = None
                    if 'code' in doc:
                        err_code = doc['code']

                    raise pymongo.errors.OperationFailure(doc['$err'],
                                                          err_code)
            yield doc

    @classmethod
    def _by_id_key(cls, doc_id):
        """
            Designed to be overloaded in children when a shard key needs to be
            included in a by_id()
        """
        key = {'_id': doc_id}

        if cls._meta['hash_field'] == cls._meta['id_field'] \
           and cls._meta['sharded']:
            key['shard_hash'] = cls._hash(doc_id)

        return key

    @classmethod
    def _by_ids_key(cls, doc_ids):
        key = {'_id': {'$in': doc_ids}}

        if cls._meta['hash_field'] == cls._meta['id_field'] \
           and cls._meta['sharded']:
            key['shard_hash'] = {'$in': [cls._hash(doc_id)
                                         for doc_id in doc_ids]}

        return key

    @classmethod
    def _pymongo(cls, use_async=True, read_preference=None, write_concern=None):
        from ..connection import _get_db
        # we can't do async queries if we're on the root greenlet since we have
        # nothing to yield back to
        use_async &= bool(greenlet.getcurrent().parent)

        if not hasattr(cls, '_pymongo_collection'):
            cls._pymongo_collection = {}

        if use_async not in cls._pymongo_collection:
            cls._pymongo_collection[use_async] = \
                _get_db(cls._meta['db_name'],
                        allow_async=use_async)[cls._meta['collection']]

        return cls._pymongo_collection[use_async].with_options(
            read_preference=read_preference,
            write_concern=write_concern)

    @classmethod
    def _hash(cls, value):
        # chances are this is a mistake and we didn't mean to hash "None"...
        # protect ourselves from our potential future stupidity
        if value is None:
            raise ValueError("Shard hash key is None")

        return hash(str(value))

    @classmethod
    def is_scatter_gather(cls, filter):
        '''
            Determine if the query is a scatter gather. Allow the caller
            to override this logic with is_scatter_gather since this method
            isn't fullproof
        '''
        is_scatter_gather = False
        try:
            shard_keys = cls.__dict__['meta']['shard_key']

            # if shard_keys is false, the collection is not sharded
            if not shard_keys:
                return False

            shard_keys = shard_keys.split(',')
            shard_keys = [s.split(':')[0] for s in shard_keys]

            # For custom primary key fields, convert them all to id here
            id_field = cls._meta['id_field']
            filter_keys = set(filter.keys())

            if id_field in filter_keys:
                filter_keys.remove(id_field)
                filter_keys.add('id')

            for sk in shard_keys:
                if sk == id_field:
                    sk = 'id'

                if sk == 'id' or sk == '_id':
                    if 'id' not in filter_keys and '_id' not in filter_keys:
                        is_scatter_gather = True
                        break
                else:
                    if sk not in filter_keys:
                        is_scatter_gather = True
                        break
        except Exception as e:
            pass
        return is_scatter_gather

    @classmethod
    def _update_spec(cls, *args, **kwargs):
        warnings.warn('use _update_filter instead of _update_spec',
                      DeprecationWarning)
        return cls._update_filter(*args, **kwargs)

    @classmethod
    def _update_filter(cls, filter, cursor_comment=True, comment=None, **kwargs):
        # handle queries with inheritance
        if cls._meta.get('allow_inheritance'):
            filter['_types'] = cls._class_name
        if cursor_comment is True and filter:  # comment doesn't with empty filter..
            if not comment:
                comment = MongoComment.get_query_comment()
            filter['$comment'] = comment
        return filter

    @classmethod
    def attach_trace(cls, comment, is_scatter_gather):
        set_comment = False
        current_greenlet = greenlet.getcurrent()
        if isinstance(current_greenlet, CLGreenlet):
            if not hasattr(current_greenlet,
                           '__mongoengine_comment__'):
                trace_comment = comment  # '%f:%s' % (time.time(), comment)
                setattr(current_greenlet,
                        '__mongoengine_comment__', trace_comment)
                set_comment = True
                current_greenlet.add_mongo_start(
                    trace_comment, time.time())
            else:
                trace_comment = getattr(
                    current_greenlet, '__mongoengine_comment__')
                current_greenlet.add_mongo_start(
                    trace_comment, time.time())
            setattr(current_greenlet,
                    '__scatter_gather__', is_scatter_gather)
        return set_comment

    @classmethod
    def cleanup_trace(cls, set_comment):
        current = greenlet.getcurrent()
        if hasattr(current, '__mongoengine_comment__'):
            is_scatter_gather = False
            if hasattr(current, '__scatter_gather__'):
                is_scatter_gather = current.__scatter_gather__
            current.add_mongo_end(
                current.__mongoengine_comment__, time.time(),
                is_scatter_gather)

        if hasattr(
                current, '__mongoengine_comment__'):
            delattr(current,
                    '__mongoengine_comment__')

    def _transform_query(self, query, validate=True):
        cls = type(self)
        return cls._transform_value(query, cls, validate=validate)

    @staticmethod
    def _transform_value(value, context, op=None, validate=True, fields=False,
                         embeddeddoc=False):
        from ..fields import DictField, EmbeddedDocumentField, ListField, \
            ArbitraryField
        from ..document import Document
        VALIDATE_OPS = ['$set', '$inc', None, '$eq', '$gte', '$lte', '$lt',
                        '$gt', '$ne']
        SINGLE_LIST_OPS = [None, '$gt', '$lt', '$gte', '$lte', '$ne']
        LIST_VALIDATE_OPS = ['$addToSet', '$push', '$pull']
        LIST_VALIDATE_ALL_OPS = ['$pushAll', '$pullAll', '$each', '$in',
                                 '$nin', '$all']
        NO_VALIDATE_OPS = ['$unset', '$pop', '$rename', '$bit',
                           '$all', '$and', '$or', '$exists', '$mod',
                           '$elemMatch', '$size', '$type', '$not', '$returnKey',
                           '$maxScan', '$orderby', '$explain', '$snapshot',
                           '$max', '$min', '$showDiskLoc', '$hint', '$comment',
                           '$slice', '$options', '$regex', '$position']

        # recurse on list, unless we're at a ListField
        if isinstance(value, list) and not isinstance(context, ListField):
            transformed_list = []
            for listel in value:
                if isinstance(listel, dict) and not isinstance(context, DictField):
                    transformed_value = SON()

                    for key, subvalue in listel.iteritems():
                        if key[0] == '$':
                            op = key

                        new_key, value_context = Document._transform_key(key, context,
                                                                         is_find=(op is None))

                        transformed_value[new_key] = \
                            Document._transform_value(subvalue, value_context,
                                                      op, validate, fields)

                        transformed_list.append(transformed_value)
                else:
                    transformed_list.append(listel)
            value = transformed_list

        # recurse on dict, unless we're at a DictField
        if isinstance(value, dict) and not isinstance(context, DictField):
            transformed_value = SON()

            for key, subvalue in value.iteritems():
                embeddeddoc = False
                if key[0] == '$':
                    op = key

                if isinstance(context, ListField):
                    if isinstance(context.field, EmbeddedDocumentField):
                        context = context.field
                        embeddeddoc = True

                new_key, value_context = Document._transform_key(key, context,
                                                                 is_find=(op is None))

                transformed_value[new_key] = \
                    Document._transform_value(subvalue, value_context,
                                              op, validate, fields, embeddeddoc=embeddeddoc)

            return transformed_value
        # if we're in a dict field and there's operations on it, recurse
        elif isinstance(value, dict) and value and value.keys()[0][0] == '$':
            transformed_value = SON()

            for key, subvalue in value.iteritems():
                op = key

                new_key, value_context = Document._transform_key(key, context,
                                                                 is_find=(op is None))

                transformed_value[new_key] = \
                    Document._transform_value(subvalue, value_context,
                                              op, validate, fields)

            return transformed_value
        # else, validate & return
        else:
            if isinstance(context, CLSContext):
                return value
            op_type = None
            # there's a special case here, since some ops on lists
            # behaves like a LIST_VALIDATE_OP (i.e. it has "x in list" instead
            # of "x = list" semantics or x not in list, etc).
            if op in LIST_VALIDATE_ALL_OPS or \
                    (op is None and
                     context._in_list and
                     (isinstance(value, list) or
                      isinstance(value, tuple))):
                op_type = 'list_all'
            elif op in LIST_VALIDATE_OPS or \
                    (op in SINGLE_LIST_OPS and isinstance(context, ListField)):
                op_type = 'list'
            elif op in VALIDATE_OPS:
                op_type = 'value'

            if op_type in ('list',) and embeddeddoc:
                op_type = 'value'

            value = Document._transform_id_reference_value(value, context,
                                                           op_type)

            if validate and not isinstance(context, ArbitraryField):
                # the caveat to the above is that those semantics are modified if
                # the value is a list. technically this isn't completely correct
                # since passing a list has a semantic of field == value OR value
                # IN field (the underlying implementation is probably that all
                # queries have (== or IN) semantics, but it's only relevant for
                # lists). so, this code won't work in a list of lists case where
                # you want to match lists on value
                if op in LIST_VALIDATE_ALL_OPS or \
                        (op is None and
                         context._in_list and
                         (isinstance(value, list) or
                          isinstance(value, tuple))):
                    for entry in value:
                        if isinstance(context, ListField):
                            context.field.validate(entry)
                        else:
                            context.validate(entry)
                # same special case as above (for {list: x} meaning "x in list")
                elif op in LIST_VALIDATE_OPS or \
                        (op in SINGLE_LIST_OPS and isinstance(context, ListField)):
                    if not isinstance(context, ListField):
                        context.validate(value)
                    else:
                        context.field.validate(value)
                elif op in VALIDATE_OPS:
                    context.validate(value)
                elif op not in NO_VALIDATE_OPS:
                    raise ValidationError("Unknown atomic operator %s" % op)

            # handle $slice by enforcing negative int
            if op == '$slice':
                if fields:
                    if not ((isinstance(value, list) or
                             isinstance(value, tuple)) and len(value) == 2) \
                            and not isinstance(value, int):
                        raise ValidationError("Projection slices must be "
                                              "2-lists or ints")
                elif not isinstance(value, int) or value > 0:
                    raise ValidationError("Slices must be negative ints")

            # handle EmbeddedDocuments
            elif isinstance(value, BaseDocument):
                value = value.to_mongo()

            # handle EmbeddedDocuments in lists
            elif isinstance(value, list):
                value = [v.to_mongo() if isinstance(v, BaseDocument) else v
                         for v in value]

            # handle lists (force to_mongo() everything if it's a list of docs)
            elif isinstance(context, ListField) and \
                    isinstance(context.field, EmbeddedDocumentField):
                value = [d.to_mongo() for d in value]

            # handle dicts (just blindly to_mongo() anything that'll take it)
            elif isinstance(context, DictField):
                for k, v in value.iteritems():
                    if isinstance(v, BaseDocument):
                        value[k] = v.to_mongo()

            return value

    @staticmethod
    def _transform_id_reference_value(value, context, op_type):
        """
            Transform strings/documents into ObjectIds / DBRefs when appropriate

            This is slightly tricky because there are List(ReferenceField) and
            List(ObjectIdField) and you sometimes get lists of documents/strings
            that need conversion.

            op_type is 'value' (if it's an individual value), 'list_all' (if it's a
            list of values), 'list' (if it's going into a list but is an individual
            value), or None (if it's neither).

            If no conversion is necessary, just return the original value
        """

        from ..fields import ReferenceField, ObjectIdField, ListField
        from ..document import Document

        # not an op we can work with
        if not op_type:
            return value

        if isinstance(context, ListField):
            f = context.field
        else:
            f = context

        if not isinstance(f, ObjectIdField) and \
           not isinstance(f, ReferenceField):
            return value

        # the logic is a bit complicated here. there are a few different
        # variables at work. the op can be value, list, or list_all and it can
        # be done on a list or on a single value. the actions are either we do
        # single conversion or we need to convert each element in a list.
        #
        # see _transform_value for the logic on what's a value, list, or
        # list_all.
        #
        # here's the matrix:
        #
        # op         type     outcome
        # --------   ------   -----------
        # value      list     convert all
        # list       list     convert one
        # list_all   list     convert all
        # value      single   convert one
        # list       single   invalid
        # list_all   single   convert all

        if not isinstance(context, ListField) and op_type == 'list':
            raise ValidationError("Can't do list operations on non-lists")

        if op_type == 'list_all' or \
           (isinstance(context, ListField) and op_type == 'value'):
            if not isinstance(value, list) and not isinstance(value, tuple):
                raise ValidationError("Expecting list, not value")

            if isinstance(f, ReferenceField):
                new_value = []

                for v in value:
                    if isinstance(v, DBRef):
                        new_value.append(v)
                    elif isinstance(v, Document):
                        new_value.append(
                            DBRef(type(v)._meta['collection'], v.id))
                    else:
                        raise ValidationError("Invalid ReferenceField value")

                return new_value
            else:
                return [ObjectId(v) for v in value]
        else:
            if isinstance(value, list) or isinstance(value, tuple):
                raise ValidationError("Expecting value, not list")

            if isinstance(f, ReferenceField):
                if isinstance(value, DBRef):
                    return value
                return DBRef(type(value)._meta['collection'], value.id)
            else:
                if value is None and not f.primary_key:
                    return value
                return ObjectId(value)

        raise AssertionError("Failed to convert")

    @staticmethod
    def _transform_key(key, context, prefix='', is_find=False):
        from ..fields import BaseField, DictField, ListField, \
            EmbeddedDocumentField, ArbitraryField
        from ..document import Document

        parts = key.split('.', 1)
        first_part = parts[0]

        if len(parts) > 1:
            rest = parts[1]
        else:
            rest = None

        # a key as a digit means a list index... set context as the list's value
        if first_part.isdigit() or first_part == '$':
            if isinstance(context, DictField):
                context = ArbitraryField()
            elif isinstance(context.field, basestring):
                context = get_document(context.field)
            elif isinstance(context.field, BaseField):
                context = context.field

        if first_part == '_id':
            context = context.pk_field()

        # atomic ops, digits (list indexes), or _ids get no processing
        if first_part[0] == '$' or first_part.isdigit() or first_part == '_id':
            if prefix:
                new_prefix = "%s.%s" % (prefix, first_part)
            else:
                new_prefix = first_part

            if rest:
                return Document._transform_key(rest, context, prefix=new_prefix, is_find=is_find)
            else:
                return new_prefix, context

        def is_subclass_or_instance(obj, parent):
            try:
                if issubclass(obj, parent):
                    return True
            except TypeError:
                if isinstance(obj, parent):
                    return True

            return False

        field = None

        if is_subclass_or_instance(context, BaseDocument):
            field = context._fields.get(first_part, None)
        elif is_subclass_or_instance(context, EmbeddedDocumentField):
            field = context.document_type._fields.get(first_part, None)
        elif is_subclass_or_instance(context, ListField):
            if is_subclass_or_instance(context.field, basestring):
                field = get_document(context.field)
            elif is_subclass_or_instance(context.field, BaseField):
                field = context.field
            else:
                raise ValueError("Can't parse field %s" % first_part)
            field._in_list = True

        # if we hit a DictField, values can be anything, so use the sentinal
        # ArbitraryField value (I prefer this over None, since None can be
        # introduced in other ways that would be considered errors & should not
        # be silently ignored)
        elif is_subclass_or_instance(context, DictField):
            field = ArbitraryField()
        elif is_subclass_or_instance(context, ArbitraryField):
            field = context

        if not field:
            # This is a hack for subclassed EmbeddedDocuments. Since we validate
            # on the top level EmbeddedDocument, and it might not have all the fields
            # pull the info from the registry and use that instead.
            if isinstance(context, EmbeddedDocumentField):
                potential_fields = get_embedded_doc_fields(
                    context.document_type)
                if first_part in potential_fields:
                    return ".".join([prefix, potential_fields[first_part].db_field]), potential_fields[first_part]
                if first_part == '_cls':
                    return ".".join([prefix, '_cls']), CLSContext()
            raise ValueError("Can't find field %s" % first_part)

        # another unfortunate hack... in find queries "list.field_name" means
        # field_name inside of the list's field... but in updates,
        # list.0.field_name means that... need to differentiate here
        list_field_name = None
        if is_subclass_or_instance(field, ListField) and is_find:
            list_field_name = field.db_field
            if is_subclass_or_instance(field.field, basestring):
                field = get_document(field.field)
            elif is_subclass_or_instance(field.field, BaseField):
                field = field.field
            else:
                raise ValueError("Can't parse field %s" % first_part)
            field._in_list = True

        if is_subclass_or_instance(field, ArbitraryField):
            db_field = first_part
        elif list_field_name:
            db_field = list_field_name
        else:
            db_field = field.db_field

        if prefix:
            if db_field is not None:
                result = "%s.%s" % (prefix, db_field)
            else:
                result = prefix
                rest = key

        else:
            result = db_field

        if rest:
            return Document._transform_key(rest, field, prefix=result, is_find=is_find)
        else:
            return result, field

    @classmethod
    def _transform_hint(cls, hint_doc):
        new_hint_doc = []
        for i, index_field in enumerate(hint_doc):
            field, direction = hint_doc[i]
            db_field, context = cls._transform_key(field, cls)
            new_hint_doc.append((db_field, direction))

        return new_hint_doc

    @classmethod
    def _transform_fields(cls, fields=None, excluded_fields=None):
        if fields is not None and excluded_fields is not None:
            raise ValueError(
                'Cannot specify both included and excluded fields.'
            )
        if isinstance(fields, dict):
            new_fields = {}
            for key, val in fields.iteritems():
                db_key, field = cls._transform_key(key, cls, is_find=True)
                if isinstance(val, dict):
                    if val.keys() not in (['$elemMatch'], ['$slice']):
                        raise ValueError('Invalid field value')
                    new_fields[db_key] = cls._transform_value(val, field,
                                                              fields=True)
                else:
                    if val not in [0, 1]:
                        raise ValueError('Invalid field value')

                    new_fields[db_key] = val
            fields = new_fields
        elif isinstance(fields, (list, tuple)):
            fields = {
                cls._transform_key(f, cls, is_find=True)[0]: 1 for f in fields
            }
        elif isinstance(excluded_fields, (list, tuple)):
            fields = {
                cls._transform_key(f, cls, is_find=True)[0]: 0
                for f in excluded_fields
            }

        return fields

    @classmethod
    def _from_augmented_son(cls, d, fields, excluded_fields=None):
        # load from son, and set field status correctly
        obj = cls._from_son(d)
        if obj is None:
            return None

        fields = cls._transform_fields(fields, excluded_fields)

        if fields is None:
            obj._all_loaded = True
            obj._default_load_status = FieldStatus.LOADED
            return obj

        # _id is always loaded unless it is specifically excluded
        obj._fields_status['_id'] = FieldStatus.LOADED
        if '_id' in fields:
            value = fields.pop('_id')
            if value == 0:
                obj._fields_status['_id'] = FieldStatus.NOT_LOADED
            if not fields:
                obj._default_load_status = FieldStatus.LOADED if value == 0 \
                    else FieldStatus.NOT_LOADED
                return obj

        # fields is now a dict of {db_field: (VALUE|<projection operator>)}
        #   where VALUE is always 1 (include) or always 0 (exclude)
        #   semantics are as follows:
        #       dict contains a 0/1 (exclude/include mode respectively)
        #       dict contains a $elemMatch (forces include mode)
        #       dict contains a $slice (forces exclude mode)
        #       otherwise include mode

        if 0 in fields.itervalues():
            dflt_load_status = FieldStatus.LOADED
        elif 1 in fields.itervalues():
            dflt_load_status = FieldStatus.NOT_LOADED
        elif len(fields) > 0:
            # true if there are any '$elemMatch's
            dflt_load_status = FieldStatus.NOT_LOADED if \
                any(isinstance(v, dict) and '$elemMatch' in v
                    for v in fields.itervalues()) \
                else FieldStatus.LOADED
        else:
            dflt_load_status = FieldStatus.NOT_LOADED

        for (field, val) in fields.iteritems():
            status = FieldStatus.NOT_LOADED if val == 0 \
                else FieldStatus.LOADED
            cls._set_field_status(field, obj, status, dflt_load_status)

        return obj

    @staticmethod
    def _set_field_status(field_name, context, status, dflt_status):
        from ..document import Document
        if isinstance(context, BaseDocument):
            parts = field_name.split('.')
            first_part = parts[0]
            rest = parts[1] if len(parts) > 1 else None

            context._default_load_status = dflt_status
            context._all_loaded = False

            if rest:
                # if the field is recursive the parent field must be loaded
                context._fields_status[first_part] = FieldStatus.LOADED
                name = [n for (n, f) in context._fields.iteritems()
                        if f.db_field == first_part][0]

                Document._set_field_status(rest, getattr(context, name),
                                           status, dflt_status)
            else:
                context._fields_status[first_part] = status
        elif isinstance(context, list):
            for el in context:
                Document._set_field_status(field_name, el, status, dflt_status)
        else:
            raise ValueError("Invalid field name %s in context %s" %
                             (field_name, context))
