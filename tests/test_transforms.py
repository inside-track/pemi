import unittest

import numpy as np
import pandas as pd

import pemi
import pemi.transforms
from pemi.fields import *

class TestValidateNoNull(unittest.TestCase):
    def setUp(self):
        field = StringField('Warf')
        self.transform = pemi.transforms.validate_no_null(field)

    def test_it_raises_an_error_if_null(self):
        self.assertRaises(ValueError, self.transform, '')

    def test_it_raises_no_error_if_not_null(self):
        self.assertEqual(self.transform('hello'), 'hello')

class TestIsBlank(unittest.TestCase):
    def test_np_nan(self):
        self.assertEqual(pemi.transforms.isblank(np.nan), True)

    def test_float_nan(self):
        self.assertEqual(pemi.transforms.isblank(float('NaN')), True)

    def test_empty_str(self):
        self.assertEqual(pemi.transforms.isblank(''), True)

    def test_none(self):
        self.assertEqual(pemi.transforms.isblank(None), True)

    def test_string(self):
        self.assertEqual(pemi.transforms.isblank('Something'), False)

    def test_float(self):
        self.assertEqual(pemi.transforms.isblank(3.2), False)

    def test_true(self):
        self.assertEqual(pemi.transforms.isblank(True), False)

    def test_false(self):
        self.assertEqual(pemi.transforms.isblank(False), False)

class TestConcatenate(unittest.TestCase):
    def test_it_concatenates(self):
        row = pd.Series(['ab', 'c', 'd'])
        self.assertEqual(pemi.transforms.concatenate()(row), 'abcd')

    def test_it_concatenates_w_delimiter(self):
        row = pd.Series(['ab', 'c', 'd'])
        self.assertEqual(pemi.transforms.concatenate('-')(row), 'ab-c-d')
