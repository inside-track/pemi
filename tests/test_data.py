import unittest

import pandas as pd
from pandas.util.testing import assert_frame_equal
from pandas.util.testing import assert_series_equal

import pemi
import pemi.testing


class TestTableConvertMarkdown(unittest.TestCase):
    'Scenarios that involve converting a Markdown table into a pandas dataframe'

    def test_convert_table_to_df(self):
        given_table = pemi.data.Table(
            '''
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | two   |
            | 3  | three |
            ''',
            schema=pemi.Schema({
                'id':   { 'ftype': 'integer' },
                'name': { 'ftype': 'string' }
            })
        )

        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['one', 'two', 'three']
        })

        assert_frame_equal(given_table.df, expected_df)


    def test_convert_table_to_df_with_blanks(self):
        given_table = pemi.data.Table(
            '''
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  |       |
            | 3  | three |
            ''',
            schema=pemi.Schema({
                'id':   { 'ftype': 'integer' },
                'name': { 'ftype': 'string' }
            })
        )

        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['one', '', 'three']
        })

        assert_frame_equal(given_table.df, expected_df)
