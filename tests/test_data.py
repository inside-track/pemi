import math

import pandas as pd
from pandas.testing import assert_frame_equal

import pemi
import pemi.testing
from pemi.fields import *

class TestTableConvertMarkdown():
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
            schema=pemi.Schema(
                id=IntegerField(),
                name=StringField()
            )
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
            schema=pemi.Schema(
                id=IntegerField(),
                name=StringField()
            )
        )

        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['one', '', 'three']
        })

        assert_frame_equal(given_table.df, expected_df)


    def test_convert_table_and_fake_unspecified_columns(self):
        # Deterministic fakery
        fake_data = (x for x in ['blerp', 'erp', 'doop'])

        given_table = pemi.data.Table(
            '''
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | two   |
            | 3  | three |
            ''',
            schema=pemi.Schema(
                id=IntegerField(),
                name=StringField(),
                name_alt=StringField(faker=lambda: next(fake_data))
            )
        )

        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['one', 'two', 'three'],
            'name_alt': ['blerp', 'erp', 'doop']
        })

        assert_frame_equal(given_table.df, expected_df)

    def test_build_fake_data(self):
        # Deterministic fakery
        fake_name = (x for x in ['one', 'two', 'three'])
        fake_name_alt = (x for x in ['blerp', 'erp', 'doop'])

        given_table = pemi.data.Table(
            nrows=3,
            schema=pemi.Schema(
                name=StringField(faker=lambda: next(fake_name)),
                name_alt=StringField(faker=lambda: next(fake_name_alt))
            )
        )

        expected_df = pd.DataFrame({
            'name': ['one', 'two', 'three'],
            'name_alt': ['blerp', 'erp', 'doop']
        })

        assert_frame_equal(given_table.df, expected_df)


    def test_build_fake_unique_data(self):
        given_table = pemi.data.Table(
            nrows=10,
            schema=pemi.Schema(
                id=IntegerField(faker=pemi.data.UniqueIdGenerator()),
                name=StringField()
            )
        )

        ids = [id for id in given_table.df['id'] if not math.isnan(id)]
        assert len(ids) == len(set(ids))

    def test_custom_coercions(self):
        actual = pemi.data.Table(
            '''
            | something     | awesome       |
            | -             | -             |
            | I am NOT here | I am NOT here |
            | Here I am     | Here I am not |
            ''',
            coerce_with={
                'something': lambda v: v.replace('NOT ', '')
            }
        )

        expected = pemi.data.Table(
            '''
            | something | awesome       |
            | -         | -             |
            | I am here | I am NOT here |
            | Here I am | Here I am not |
            '''
        )

        assert_frame_equal(actual.df, expected.df)

    def test_ignores_trailing_comments(self):

        given_table = pemi.data.Table(
            '''
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | two   | # Some comment
            | 3  | three |
            ''',
            schema=pemi.Schema(
                id=IntegerField(),
                name=StringField()
            )
        )

        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['one', 'two', 'three']
        })

        assert_frame_equal(given_table.df, expected_df)

    def test_honors_embedded_octothorpes(self):

        given_table = pemi.data.Table(
            '''
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | #2    |
            | 3  | three |
            ''',
            schema=pemi.Schema(
                id=IntegerField(),
                name=StringField()
            )
        )

        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['one', '#2', 'three']
        })

        assert_frame_equal(given_table.df, expected_df)
