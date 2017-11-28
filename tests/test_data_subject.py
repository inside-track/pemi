import unittest

import pandas as pd
from pandas.util.testing import assert_frame_equal

import pemi
from pemi.fields import *

class TestPdDataSubject(unittest.TestCase):
    def test_it_creates_an_empty_dataframe(self):
        ds = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f2=StringField()
        ))

        assert_frame_equal(ds.df, pd.DataFrame(columns=['f1', 'f2']))

    def test_it_raises_schema_invalid_on_connection(self):
        ds1 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f2=StringField()
        ))

        ds2 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f3=StringField()
        ))

        ds1.df = pd.DataFrame({
            'f1': [1,2,3],
            'f2': [4,5,6]
        })

        self.assertRaises(pemi.data_subject.MissingFieldsError, lambda: ds2.connect_from(ds1))

    def test_it_creates_an_empty_df_with_schema_when_connected_to_empty(self):
        ds1 = pemi.PdDataSubject()

        ds2 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f3=StringField()
        ))

        ds2.connect_from(ds1)
        assert_frame_equal(ds2.df, pd.DataFrame(columns=['f1','f3']))
