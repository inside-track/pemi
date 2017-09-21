import unittest
import datetime
import decimal

import pemi.fields
from pemi.fields import *

class TestField(unittest.TestCase):
    def test_it_accepts_metadata(self):
        '''
        Users can define custom metadata to add to fields
        '''
        field = pemi.fields.Field(bill='S. Preston', ted='Theodore Logan')
        self.assertEqual(field.metadata['bill'], 'S. Preston')

    def test_it_defaults_to_none_for_null(self):
        '''
        The null representation for a field is None by default
        '''
        field = pemi.fields.Field()
        self.assertEqual(field.metadata['null'], None)

    def test_null_can_be_overridden(self):
        '''
        The null representation for a field can be overridden
        '''
        field = pemi.fields.Field(null='#N/A')
        self.assertEqual(field.null, '#N/A')

class TestStringField(unittest.TestCase):
    def test_convert_a_string(self):
        '''
        It takes a string and outputs a string
        '''
        field = StringField()
        coerced = field.coerce('bodacious')
        self.assertEqual(coerced, 'bodacious')

    def test_it_stringifies_non_strings(self):
        '''
        It takes something that is not a string and returns the string representation
        '''
        field = StringField()
        coerced = field.coerce(3.14)
        self.assertEqual(coerced, '3.14')

    def test_use_empty_string_as_null(self):
        '''
        It converts any missing values to an empty string.
        '''
        field = StringField(required=False)
        coerced = field.coerce(None)
        self.assertEqual(coerced, '')

    def test_change_definition_of_null(self):
        '''
        The definition of null can be changed to something other than ''
        '''
        field = StringField(required=False, null=None)
        coerced = field.coerce(None)
        self.assertEqual(coerced, None)




class TestIntegerField(unittest.TestCase):
    def test_convert_to_integers(self):
        '''
        String values should be converted into integers
        '''

        field = IntegerField()
        coerced = field.coerce('42')
        self.assertEqual(coerced, 42)

    def test_it_fails_to_convert_floats(self):
        '''
        Raises an exception if given a float
        '''

        field = IntegerField()
        self.assertRaises(pemi.fields.CoercionError, field.coerce, '42.3')

    def test_it_optionally_converts_floats(self):
        '''
        Will truncate floats when provided with coerce_float metadata
        '''

        field = IntegerField(coerce_float=True)
        coerced = field.coerce('42.3')
        self.assertEqual(coerced, 42)


class TestFloatField(unittest.TestCase):
    def test_convert_to_float(self):
        '''
        String values should be converted into floats
        '''

        field = FloatField()
        coerced = field.coerce('42.3')
        self.assertEqual(coerced, 42.3)


class TestDateField(unittest.TestCase):
    def test_convert_to_date(self):
        '''
        String values should convert to Python dates
        '''
        field = DateField()
        coerced = field.coerce('2016-02-14')
        self.assertEqual(coerced, datetime.date(2016,2,14))

    def test_custom_format(self):
        '''
        String values should convert to Python dates using a custom format
        '''
        field = DateField(format='%d/%m/%Y')
        coerced = field.coerce('14/02/2016')
        self.assertEqual(coerced, datetime.date(2016,2,14))

    def test_convert_from_date(self):
        '''
        Just return the date if it's already a date
        '''
        field = DateField()
        coerced = field.coerce(datetime.date(2016,2,14))
        self.assertEqual(coerced, datetime.date(2016,2,14))

    def test_convert_from_datetime(self):
        '''
        Return the date part if it's already a datetime
        '''
        field = DateField()
        coerced = field.coerce(datetime.datetime(2016,2,14,1,2,3))
        self.assertEqual(coerced, datetime.date(2016,2,14))

    def test_convert_invalid_date(self):
        '''
        Raise an exception if the date doesn't match the expected format
        '''
        field = DateField()
        coerce = lambda: field.coerce('2/14/2016')
        self.assertRaises(pemi.fields.CoercionError, coerce)

    def test_convert_inferred_date(self):
        '''
        String values should convert to Python dates using an inferred format
        '''
        field = DateField(infer_format=True)
        coerced = field.coerce('14/02/2016')
        self.assertEqual(coerced, datetime.date(2016,2,14))

    def test_convert_invalid_inferred_date(self):
        '''
        Raise an exception if the date format cannot be inferred
        '''
        field = DateField(infer_format=True)
        coerce = lambda: field.coerce('2/14:2016')
        self.assertRaises(pemi.fields.CoercionError, coerce)


class TestDateTimeField(unittest.TestCase):
    def test_convert_to_datetime(self):
        '''
        String values should convert to Python datetimes
        '''
        field = DateTimeField()
        coerced = field.coerce('2016-02-14 04:33:00')
        self.assertEqual(coerced, datetime.datetime(2016,2,14,4,33,0))

    def test_custom_format(self):
        '''
        String values should convert to Python datetimes using a custom format
        '''
        field = DateTimeField(format='%d/%m/%Y%H%M%S')
        coerced = field.coerce('14/02/2016043300')
        self.assertEqual(coerced, datetime.datetime(2016,2,14,4,33,0))

    def test_convert_from_date(self):
        '''
        Dates should be converted to datetimes with zeroes appended
        '''
        field = DateTimeField()
        coerced = field.coerce(datetime.date(2016,2,14))
        self.assertEqual(coerced, datetime.datetime(2016,2,14,0,0,0))

    def test_convert_from_datetime(self):
        '''
        Just return the datetime if it's already a datetime
        '''
        field = DateTimeField()
        coerced = field.coerce(datetime.datetime(2016,2,14,4,33,0))
        self.assertEqual(coerced, datetime.datetime(2016,2,14,4,33,0))

    def test_convert_invalid_datetime(self):
        '''
        Raise an exception if the datetime doesn't match the expected format
        '''
        field = DateTimeField()
        coerce = lambda: field.coerce('2/14/2016043300')
        self.assertRaises(pemi.fields.CoercionError, coerce)

    def test_convert_inferred_datetime(self):
        '''
        String values should convert to Python datetimes using an inferred format
        '''
        field = DateTimeField(infer_format=True)
        coerced = field.coerce('14/02/2016 04:33:00')
        self.assertEqual(coerced, datetime.datetime(2016,2,14,4,33,0))

    def test_convert_invalid_inferred_datetime(self):
        '''
        Raise an exception if the datetime format cannot be inferred
        '''
        field = DateTimeField(infer_format=True)
        coerce = lambda: field.coerce('2/14:2016043300')
        self.assertRaises(pemi.fields.CoercionError, coerce)


class TestBooleanField(unittest.TestCase):
    def test_convert_to_true(self):
        '''
        Convert a truthy string value to True
        '''
        field = BooleanField()
        coerced = field.coerce('y')
        self.assertEqual(coerced, True)

    def test_convert_to_false(self):
        '''
        Convert a falsey string value to False
        '''
        field = BooleanField()
        coerced = field.coerce('0')
        self.assertEqual(coerced, False)

    def test_empty_is_ok(self):
        '''
        Empty strings convert to None by default
        '''
        field = BooleanField()
        coerced = field.coerce('')
        self.assertEqual(coerced, None)

    def test_raise_unknown(self):
        '''
        Raise a conversion error if the truthiness is unknown
        '''
        field = BooleanField()
        self.assertRaises(pemi.fields.CoercionError, field.coerce, 'non-heinous')

    def test_unknown_is_null_option(self):
        '''
        Unknown truthiness optionally converts to None
        '''
        field = BooleanField(unknown_truthiness=None)
        coerced = field.coerce('non-non-heinous')
        self.assertEqual(coerced, None)

    def test_unknown_is_false_option(self):
        '''
        Unknown truthiness optionally converts to False
        '''
        field = BooleanField(unknown_truthiness=False)
        coerced = field.coerce('non-non-non-heinous')
        self.assertEqual(coerced, False)

    def test_custom_true(self):
        '''
        Custom truthy map converts both custom and standard truthy values
        '''
        field = BooleanField(true_values=['oui', 't'])
        coerced = field.coerce('oui')
        self.assertEqual(coerced, True)

        coerced = field.coerce('t')
        self.assertEqual(coerced, True)

    def test_custom_false(self):
        '''
        Custom falsey map converts both custom and standard falsey values
        '''
        field = BooleanField(false_values=['non', 'f'])
        coerced = field.coerce('non')
        self.assertEqual(coerced, False)

        coerced = field.coerce('f')
        self.assertEqual(coerced, False)


class TestDecimalField(unittest.TestCase):
    def test_convert_to_decimal(self):
        '''
        String values should convert to a decimal given an expected precision and scale
        '''
        field = DecimalField(precision=6, scale=5)
        coerced = field.coerce('3.14159')
        self.assertEqual(coerced, decimal.Decimal('3.14159'))

    def test_raise_precision(self):
        '''
        Raise en error if the precision is too small for the string
        '''
        field = DecimalField(precision=5, scale=5)
        self.assertRaises(pemi.fields.CoercionError, field.coerce, '3.14159')

    def test_raise_scale(self):
        '''
        Raise an error if the scale is too small for the string
        '''
        field = DecimalField(precision=6, scale=4)
        self.assertRaises(pemi.fields.CoercionError, field.coerce, '3.14159')

    def test_does_not_raise_if_not_enforced(self):
        '''
        Does not raise an error if the precision is too small when specified
        '''
        field = DecimalField(precision=1, scale=1, enforce_decimal=False)
        coerced = field.coerce('3.14159')
        self.assertEqual(coerced, decimal.Decimal('3.14159'))

    def test_truncates_decimal(self):
        '''
        Truncates decimals to the specified scale by rounding half even
        '''
        field = DecimalField(precision=5, scale=1, truncate_decimal=True)
        coerced = field.coerce('3.45')
        self.assertEqual(coerced, decimal.Decimal('3.4'))

        coerced = field.coerce('3.55')
        self.assertEqual(coerced, decimal.Decimal('3.6'))
