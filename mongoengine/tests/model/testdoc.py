from ...document import Document
from ...fields import IntField, StringField, ListField


class TestDoc(Document):
    meta = {}
    meta['db_name'] = 'test'
    meta['force_insert'] = True
    test_int = IntField(db_field="ti")
    test_str = StringField(db_field="ts")
    test_pk = IntField(db_field="tpk", required=True, primary_key=True)
    test_list = ListField(IntField(), db_field="tli")
