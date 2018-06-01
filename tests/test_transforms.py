import pytest

import numpy as np
import pandas as pd

import pemi
import pemi.transforms
from pemi.fields import *

class TestValidateNoNull:
    @pytest.fixture
    def transform(self):
        field = StringField('Warf')
        return pemi.transforms.validate_no_null(field)

    def test_it_raises_an_error_if_null(self, transform):
        with pytest.raises(ValueError):
            transform('')

    def test_it_raises_no_error_if_not_null(self, transform):
        assert transform('hello') == 'hello'

class TestIsBlank:
    def test_np_nan(self):
        assert pemi.transforms.isblank(np.nan) is True

    def test_float_nan(self):
        assert pemi.transforms.isblank(float('NaN')) is True

    def test_empty_str(self):
        assert pemi.transforms.isblank('') is True

    def test_none(self):
        assert pemi.transforms.isblank(None) is True

    def test_string(self):
        assert pemi.transforms.isblank('Something') is False

    def test_float(self):
        assert pemi.transforms.isblank(3.2) is False

    def test_float_zero(self):
        assert pemi.transforms.isblank(0.0) is False

    def test_int(self):
        assert pemi.transforms.isblank(1) is False

    def test_int_zero(self):
        assert pemi.transforms.isblank(0) is False

    def test_true(self):
        assert pemi.transforms.isblank(True) is False

    def test_false(self):
        assert pemi.transforms.isblank(False) is False


class TestConcatenate:
    def test_it_concatenates(self):
        row = pd.Series(['ab', 'c', 'd'])
        assert pemi.transforms.concatenate()(row) == 'abcd'

    def test_it_concatenates_w_delimiter(self):
        row = pd.Series(['ab', 'c', 'd'])
        assert pemi.transforms.concatenate('-')(row) == 'ab-c-d'

class TestNvl:
    def test_it_picks_the_first_non_blank(self):
        row = pd.Series([None, '', 'three', 'four', None])
        assert pemi.transforms.nvl()(row) == 'three'

    def test_it_uses_a_default_if_all_are_blank(self):
        row = pd.Series([None, '', np.nan, None])
        assert pemi.transforms.nvl('ALL BLANK')(row) == 'ALL BLANK'
