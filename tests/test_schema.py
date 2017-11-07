import unittest

import pemi
from pemi.fields import *


class TestSchema(unittest.TestCase):
    def test_create_schema_from_list(self):
        '''
        Creating a schema from a list of fields
        '''
        f1 = IntegerField('id')
        f2 = StringField('name')
        f3 = DateField('sell_at', format='%m/%d/%Y')


        schema = pemi.Schema(f1, f2, f3)
        expected_fields = {
            'id': f1,
            'name': f2,
            'sell_at': f3
        }
        self.assertEqual(schema.fields, expected_fields)

    def test_create_schema_from_keywords(self):
        '''
        Creating a schema from field keywords
        '''
        f1 = IntegerField()
        f2 = StringField()
        f3 = DateField(format='%m/%d/%Y')


        schema = pemi.Schema(
            id=f1,
            name=f2,
            sell_at=f3
        )
        expected_fields = {
            'id': f1,
            'name': f2,
            'sell_at': f3
        }
        self.assertEqual(schema.fields, expected_fields)


    def test_get_metadata_for_a_field(self):
        '''
        Metadata for a field can be retrieved
        '''

        schema = pemi.Schema(
            DateField('sell_at', format='%m/%d/%Y')
        )

        self.assertEqual(schema['sell_at'].metadata['format'], '%m/%d/%Y')


    def test_schema_merge(self):
        '''
        Schemas and the field metadata can be merged
        '''

        s1f1 = IntegerField('id')
        s1f2 = StringField('name', from_s1='yep', whoami='s1')
        s1 = pemi.Schema(s1f1, s1f2)

        s2f2 = StringField('name', from_s2='certainly', whoami='s2')
        s2f3 = DateField('sell_at', format='%m/%d/%Y')
        s2 = pemi.Schema(s2f2, s2f3)

        merged = s1.merge(s2)
        expected = pemi.Schema(
            s1f1,
            StringField('name', from_s1='yep', from_s2='certainly', whoami='s2'),
            s2f3
        )
        self.assertEqual(merged, expected)

    def test_schema_subset(self):
        '''
        Given a list, generates a new schema containing just the fields in the list
        '''

        schema = pemi.Schema(
            f1 = StringField(),
            f2 = StringField(),
            f3 = StringField()
        )

        actual = schema[['f1','f3']]
        expected = pemi.Schema(
            f1 = StringField(),
            f3 = StringField()
        )

        self.assertEqual(actual, expected)

    def test_schema_subset_error(self):
        '''
        Given a list, raises an error if a field name is not found
        '''

        schema = pemi.Schema(
            f1 = StringField(),
            f2 = StringField(),
            f3 = StringField()
        )

        self.assertRaises(KeyError, lambda: schema[['f1', 'f4']])
