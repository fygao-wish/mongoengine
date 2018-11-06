class PyEnumMixin(object):
    
    '''
        Verify ids are conseucetive. If there are duplicate ids, then
        ids will be overwritten and there will be holes in the numbers
        Specify __VERIFYIDS__ to starting id
        Specify __SKIPIDS__ as an array of ids to skip over
    '''
    @classmethod
    def verify_ids(cls,ids):
        if not ids or len(ids) == 0:
            return
        ids.sort()

        start = getattr(cls,'__VERIFYIDS__')
        assert isinstance(start,int), "__VERIFYIDS__ must be integer"

        skip = getattr(cls,'__SKIPIDS__',[])
        for num in ids:
            while start in skip:
                start += 1
            assert start == num,"Missing ID: %d"%start
            start += 1

    @classmethod
    def _build_cache(cls):
        id_to_name = {}
        name_to_id = {}
        attributes = list(dir(cls))

        verify = False

        for attr in attributes:
            attr_val = getattr(cls, attr)

            if attr=='__VERIFYIDS__':
                verify = True

            if attr == '_cache_1' or attr == '_cache_2' or \
                    attr.startswith('__') or callable(attr_val) or \
                    isinstance(attr_val, dict) or isinstance(attr_val, list):
                continue
            assert getattr(cls, attr) not in id_to_name, \
                'DUP VAL: %s in %s' % (getattr(cls,attr), cls.__name__)

            id_to_name[getattr(cls, attr)] = attr
            name_to_id[attr] = getattr(cls, attr)

        if verify:
            cls.verify_ids(id_to_name.keys())

        cls._cache_1 = id_to_name
        cls._cache_2 = name_to_id

    @classmethod
    def num_to_name(cls):
        if not getattr(cls, '_cache_1', None):
            cls._build_cache()
        return cls._cache_1

    @classmethod
    def num_to_friendly_name(cls):
        name_dict = {}
        for num, name in  cls.num_to_name().iteritems():
            name_dict[num] = name.replace('_', ' ').title()
        return name_dict

    @classmethod
    def get_titled_name(cls, key):
        return cls.get_name(key).replace('_', ' ').title()

    @classmethod
    def get_name(cls, key):
        assert key in cls.num_to_name(), "Not valid key: %s" % key
        return cls.num_to_name()[key]

    @classmethod
    def get_ids(cls):
        return cls.num_to_name().keys()

    @classmethod
    def get_names(cls):
        return cls.num_to_name().values()

    @classmethod
    def is_key(cls, val):
        return val in cls.num_to_name()

    @classmethod
    def is_name(cls, val):
        return val in cls.to_dict()

    @classmethod
    def name_to_num(cls, name):
        if not getattr(cls, '_cache_2', None):
            cls._build_cache()
        return cls._cache_2[name] if name in cls._cache_2 else None

    @classmethod
    def to_dict(cls):
        if not getattr(cls, '_cache_2', None):
            cls._build_cache()
        return cls._cache_2


