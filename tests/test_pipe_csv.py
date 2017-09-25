import unittest
import datetime
import decimal
import tempfile
from pathlib import Path

import pandas as pd
from pandas.util.testing import assert_frame_equal

import pemi
import pemi.testing
import pemi.pipes.csv
from pemi.fields import *

class TestLocalCsvFileSourcePipe(unittest.TestCase):
    def test_it_parses_a_complex_csv(self):
        schema = pemi.Schema(
            id         = StringField(),
            name       = StringField(allow_null=False),
            is_awesome = BooleanField(),
            price      = DecimalField(precision=4, scale=2),
            sell_date  = DateField(format='%m/%d/%Y'),
            updated_at = DateTimeField(format='%m/%d/%Y %H:%M:%S')
        )

        pipe = pemi.pipes.csv.LocalCsvFileSourcePipe(
            schema=schema,
            paths=[Path(__file__).parent / Path('fixtures') / Path('beers.csv')]
        )
        pipe.flow()
        actual_df = pipe.targets['main'].df
        actual_errors_df = pipe.targets['errors'].df


        expected_df = pemi.data.Table(
            '''
            | id | name         | is_awesome  | price | sell_date  | updated_at          |
            | -  | -            | -           | -     | -          | -                   |
            | 01 | DBIRA        | True        | 10.83 | 2017-03-27 | 2017-01-01 23:39:39 |
            | 03 | Berber       | False       | 10.83 | 2017-03-22 | 2017-01-01 23:39:39 |
            | 04 | SoLikey      | False       | 10.83 | 2017-03-22 | 2017-01-01 23:39:39 |
            | 05 | Perfecticon  | False       |       | 2017-03-22 | 2017-01-01 23:39:39 |
            | 06 | Hopticular   | True        | 10.83 | 2017-03-22 | 2017-01-01 23:39:39 |
            | 07 | Malted       |             | 10.83 | 2017-03-22 | 2017-01-01 23:39:39 |
            | 08 | Foamy        | True        | 10.83 | 2017-03-22 | 2017-01-01 23:39:39 |
            ''',
            schema=schema.merge(pemi.Schema(
                sell_date  = DateField(format='%Y-%m-%d'),
                updated_at = DateTimeField(format='%Y-%m-%d %H:%M:%S')
            ))
        ).df

        expected_errors_df = pemi.data.Table(
            '''
            | id | __error_message__                    |
            | -  | -                                    |
            | 02 | null is not allowed for field 'name' |
            ''',
            schema=pemi.Schema(
                id                = StringField(),
                __error_message__ = StringField()
            )
        ).df

        expected_df.reset_index(drop=True, inplace=True)
        actual_df.reset_index(drop=True, inplace=True)
        pemi.testing.assert_frame_equal(actual_df, expected_df, check_names=False)


        expected_errors_df.reset_index(drop=True, inplace=True)
        actual_errors_df.reset_index(drop=True, inplace=True)
        pemi.testing.assert_frame_equal(actual_errors_df[expected_errors_df.columns], expected_errors_df, check_names=False)


    def test_it_combines_multiple_csvs(self):
        pipe = pemi.pipes.csv.LocalCsvFileSourcePipe(
            schema=pemi.Schema(
                id   = StringField(),
                name = StringField()
            ),
            paths=[
                Path(__file__).parent / Path('fixtures') / Path('id_name_1.csv'),
                Path(__file__).parent / Path('fixtures') / Path('id_name_2.csv')
            ]
        )
        pipe.flow()

        df_1 = pd.DataFrame(
            {
                0: ['1', '1'],
                1: ['2', 'two'],
                2: ['3', 'tres']
            },
            index=['id', 'name']
        ).transpose()

        df_2 = pd.DataFrame(
            {
                0: ['4', 'delta'],
                1: ['5', 'epsilon'],
            },
            index=['id', 'name']
        ).transpose()

        expected_df = pd.concat([df_1, df_2])
        assert_frame_equal(pipe.targets['main'].df, expected_df)


class TestLocalCsvFileTargetPipe(unittest.TestCase):
    def test_it_writes_a_csv(self):
        tmp_file = tempfile.NamedTemporaryFile()

        pipe = pemi.pipes.csv.LocalCsvFileTargetPipe(
            schema=pemi.Schema(
                id   = StringField(),
                name = StringField()
            ),
            path=tmp_file.name,
            csv_opts={'sep': '|'}
        )

        given_df = pd.DataFrame(
            {
                0: ['1', 'one'],
                1: ['2', 'two'],
                2: ['3', 'three']
            },
            index=['id', 'name']#pipe.schema.keys#
        ).transpose()

        pipe.sources['main'].df = given_df
        pipe.flow()

        expected_csv = '\n'.join([
            'id|name',
            '1|one',
            '2|two',
            '3|three',
            ''
        ]).encode('utf-8')

        actual_csv = tmp_file.read()
        self.assertEqual(actual_csv, expected_csv)
