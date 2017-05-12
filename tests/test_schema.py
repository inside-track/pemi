import unittest

import pemi.schema

class TestSchema(unittest.TestCase):
    def setUp(self):
        self.schema = pemi.schema.Schema(
            ('id'         , 'integer', {'required': True}),
            ('name'       , 'string', {'length': 80}),
            ('is_awesome' , 'boolean', {}),
            ('price'      , 'decimal', {'precision': 4, 'scale': 2}),
            ('details'    , 'json', {}),
            ('sell_date'  , 'date', {'in_format': '%m/%d/%Y', 'to_create': True, 'to_update': False}),
            ('updated_at' , 'datetime', {'in_format': '%m/%d/%Y %H:%M:%S', 'to_create': True, 'to_update': False}),
        )
