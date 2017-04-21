import unittest
import datetime
import decimal

import pemi.field

class TestField(unittest.TestCase):
    def test_basic_usage(self):
        field = pemi.field.Field()

        converted = field.in_converter('something')
        self.assertEqual(converted, 'something')

    def test_required_when_missing(self):
        '''
        Raise an error if a field is required but missing
        '''
        field = pemi.field.Field(required=True)
        self.assertRaises(pemi.field.RequiredValueError, field.in_converter, None)

    def test_type_in_metadata(self):
        '''
        Type is available in the metadata
        '''
        field = pemi.field.Field(ftype='customtype')
        self.assertEqual(field.metadata['ftype'], 'customtype')

    def test_required_in_metadata(self):
        '''
        Required is available in the metadata
        '''
        field = pemi.field.Field()
        self.assertEqual(field.metadata['required'], False)

    def test_custom_medata(self):
        '''
        Users can add arbitary metadata
        '''
        field = pemi.field.Field(dumpster='fire')
        self.assertEqual(field.metadata['dumpster'], 'fire')


class TestStringField(unittest.TestCase):
    def test_required_when_missing(self):
        '''
        When the field is required, an empty string raises a RequiredValueError
        '''
        field = pemi.field.StringField(required=True)
        self.assertRaises(pemi.field.RequiredValueError, field.in_converter, '')

    def test_use_empty_string_as_null(self):
        '''
        It converts any missing values to an empty string.
        '''
        field = pemi.field.StringField(required=False)
        converted = field.in_converter(None)
        self.assertEqual(converted, '')


class TestIntegerField(unittest.TestCase):
    def test_convert_to_integers(self):
        '''
        String values should be converted into integers
        '''

        field = pemi.field.IntegerField()
        converted = field.in_converter('42')
        self.assertEqual(converted, 42)


class TestDateField(unittest.TestCase):
    def test_convert_to_date(self):
        '''
        String values should convert to Python dates
        '''
        field = pemi.field.DateField()
        converted = field.in_converter('2016-02-14')
        self.assertEqual(converted, datetime.date(2016,2,14))

    def test_custom_format(self):
        '''
        String values should convert to Python dates using a custom format
        '''
        field = pemi.field.DateField(in_format='%d/%m/%Y')
        converted = field.in_converter('14/02/2016')
        self.assertEqual(converted, datetime.date(2016,2,14))


class TestDateTimeField(unittest.TestCase):
    def test_convert_to_datetime(self):
        '''
        String values should convert to Python datetimes
        '''
        field = pemi.field.DateTimeField()
        converted = field.in_converter('2016-02-14 04:33:00')
        self.assertEqual(converted, datetime.datetime(2016,2,14,4,33,0))

    def test_custom_format(self):
        '''
        String values should convert to Python datetimes using a custom format
        '''
        field = pemi.field.DateTimeField(in_format='%d/%m/%YT%H%M%S')
        converted = field.in_converter('14/02/2016T043300')
        self.assertEqual(converted, datetime.datetime(2016,2,14,4,33,0))


class TestDecimalField(unittest.TestCase):
    def test_convert_to_decimal(self):
        '''
        String values should convert to a decimal given an expected precision and scale
        '''
        field = pemi.field.DecimalField(precision=6, scale=5)
        converted = field.in_converter('3.14159')
        self.assertEqual(converted, decimal.Decimal((0, (3,1,4,1,5,9), -5 )))

    def test_raise_precision(self):
        '''
        Raise a conversion error if the precision is too small for the string
        '''
        field = pemi.field.DecimalField(precision=5, scale=5)
        self.assertRaises(pemi.field.ConversionError, field.in_converter, '3.14159')

    def test_raise_scale(self):
        '''
        Raise a conversion error if the scale is too small for the string
        '''
        field = pemi.field.DecimalField(precision=6, scale=4)
        self.assertRaises(pemi.field.ConversionError, field.in_converter, '3.14159')


class TestBooleanField(unittest.TestCase):
    def test_convert_to_true(self):
        '''
        Convert a truthy string value to True
        '''
        field = pemi.field.BooleanField()
        converted = field.in_converter('y')
        self.assertEqual(converted, True)

    def test_convert_to_false(self):
        '''
        Convert a falsey string value to False
        '''
        field = pemi.field.BooleanField()
        converted = field.in_converter('0')
        self.assertEqual(converted, False)

    def test_empty_is_ok(self):
        '''
        Empty strings convert to None by default
        '''
        field = pemi.field.BooleanField()
        converted = field.in_converter('')
        self.assertEqual(converted, None)

    def test_raise_unknown(self):
        '''
        Raise a conversion error if the truthiness is unknown
        '''
        field = pemi.field.BooleanField()
        self.assertRaises(pemi.field.ConversionError, field.in_converter, 'non-heinous')

    def test_unknown_is_null_option(self):
        '''
        Unknown truthiness optionally converts to None
        '''
        field = pemi.field.BooleanField(unknown_truthiness=None)
        converted = field.in_converter('non-non-heinous')
        self.assertEqual(converted, None)

    def test_unknown_is_false_option(self):
        '''
        Unknown truthiness optionally converts to False
        '''
        field = pemi.field.BooleanField(unknown_truthiness=False)
        converted = field.in_converter('non-non-non-heinous')
        self.assertEqual(converted, False)

    def test_custom_true(self):
        '''
        Custom truthy map converts both custom and standard truthy values
        '''
        field = pemi.field.BooleanField(map_true=['oui'])
        converted = field.in_converter('oui')
        self.assertEqual(converted, True)

        converted = field.in_converter('t')
        self.assertEqual(converted, True)

    def test_custom_false(self):
        '''
        Custom falsey map converts both custom and standard falsey values
        '''
        field = pemi.field.BooleanField(map_false=['non'])
        converted = field.in_converter('non')
        self.assertEqual(converted, False)

        converted = field.in_converter('f')
        self.assertEqual(converted, False)
