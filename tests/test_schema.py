import pytest

import pemi
from pemi.fields import *


class TestSchema:
    def test_create_schema_from_list(self):
        '''
        Creating a schema from a list of fields
        '''
        field1 = IntegerField('id')
        field2 = StringField('name')
        field3 = DateField('sell_at', format='%m/%d/%Y')


        schema = pemi.Schema(field1, field2, field3)
        expected_fields = {
            'id': field1,
            'name': field2,
            'sell_at': field3
        }
        assert schema.fields == expected_fields

    def test_create_schema_from_keywords(self):
        '''
        Creating a schema from field keywords
        '''
        field1 = IntegerField()
        field2 = StringField()
        field3 = DateField(format='%m/%d/%Y')


        schema = pemi.Schema(
            id=field1,
            name=field2,
            sell_at=field3
        )
        expected_fields = {
            'id': field1,
            'name': field2,
            'sell_at': field3
        }
        assert schema.fields == expected_fields


    def test_get_metadata_for_a_field(self):
        '''
        Metadata for a field can be retrieved
        '''

        schema = pemi.Schema(
            DateField('sell_at', format='%m/%d/%Y')
        )

        assert schema['sell_at'].metadata['format'] == '%m/%d/%Y' #pylint: disable=no-member


    def test_schema_merge(self):
        '''
        Schemas and the field metadata can be merged
        '''

        src1field1 = IntegerField('id')
        src1field2 = StringField('name', from_src1='yep', whoami='src1')
        src1 = pemi.Schema(src1field1, src1field2)

        src2field2 = StringField('name', from_src2='certainly', whoami='src2')
        src2field3 = DateField('sell_at', format='%m/%d/%Y')
        src2 = pemi.Schema(src2field2, src2field3)

        merged = src1.merge(src2)
        expected = pemi.Schema(
            src1field1,
            StringField('name', from_src1='yep', from_src2='certainly', whoami='src2'),
            src2field3
        )
        assert merged == expected

    def test_schema_subset(self):
        '''
        Given a list, generates a new schema containing just the fields in the list
        '''

        schema = pemi.Schema(
            field1=StringField(),
            field2=StringField(),
            field3=StringField()
        )

        actual = schema[['field1', 'field3']]
        expected = pemi.Schema(
            field1=StringField(),
            field3=StringField()
        )

        assert actual == expected

    def test_schema_subset_error(self):
        '''
        Given a list, raises an error if a field name is not found
        '''

        schema = pemi.Schema(
            field1=StringField(),
            field2=StringField(),
            field3=StringField()
        )

        with pytest.raises(KeyError):
            assert schema[['field1', 'field4']]

    def test_schema_rename(self):
        '''
        Given a dict map, returns a new schema with renamed fields
        '''

        schema = pemi.Schema(
            field1=StringField(),
            field2=StringField(),
            field3=StringField()
        )

        actual = schema.rename({
            'field1': 'new_field1',
            'field3': 'new_field3'
        })

        expected = pemi.Schema(
            new_field1=StringField(),
            field2=StringField(),
            new_field3=StringField()
        )

        assert actual == expected

    def test_select(self):
        '''
        Given a metadata selector function, returns a subset of fields as a new schema
        '''

        schema = pemi.Schema(
            field1=StringField(awesome=True),
            field2=StringField(),
            field3=StringField(awesome=True),
            field4=StringField(awesome=False),
        )

        actual = schema.select(lambda m: m.metadata.get('awesome', False))

        expected = pemi.Schema(
            field1=StringField(awesome=True),
            field3=StringField(awesome=True)
        )

        assert actual == expected
