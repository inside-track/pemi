import unittest
import datetime
import decimal
import tempfile
from pathlib import Path

import pandas as pd
from pandas.util.testing import assert_frame_equal


import pemi
from pemi import DataSubject
from pemi import SourcePipe
from pemi import TargetPipe
import pemi.pipes.csv


class TestLocalCsvFileSourcePipe(unittest.TestCase):
    def test_it_parses_a_complex_csv(self):
        pipe = pemi.pipes.csv.LocalCsvFileSourcePipe(
            schema={
                'id':         {'type': 'string', 'required': True},
                'name':       {'type': 'string', 'length': 80, 'required': True},
                'is_awesome': {'type': 'boolean'},
                'price':      {'type': 'decimal', 'precision': 4, 'scale': 2},
                'details':    {'type': 'json'},
                'sell_date':  {'type': 'date', 'in_format': '%m/%d/%Y', 'to_create': True, 'to_update': False},
                'updated_at': {'type': 'datetime', 'in_format': '%m/%d/%Y %H:%M:%S', 'to_create': True, 'to_update': False}
            },
            paths=[Path(__file__).parent / Path('fixtures') / Path('beers.csv')]
        )
        pipe.execute()

        fdt = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        fd  = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()
        fdc = lambda x: decimal.Decimal(x)

        expected_df = pd.DataFrame(
            {
                0: ['01',        'DBIRA',        True, fdc('10.83'),       {},  fd('2017-03-27'), fdt('2017-01-01 23:39:39')],
                2: ['03',       'Berber',       False, fdc('10.83'),       {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                3: ['04',      'SoLikey',       False, fdc('10.83'),       {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                4: ['05',  'Perfecticon',       False,         None,       {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                5: ['06',   'Hopticular',        True, fdc('10.83'),       {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                6: ['07',       'Malted',        None, fdc('10.83'),       {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                7: ['08',        'Foamy',        True, fdc('10.83'),       {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')]
            },
            index= ['id',         'name','is_awesome',      'price','details',       'sell_date',               'updated_at']
        ).transpose()

        assert_frame_equal(pipe.targets['standard'].data, expected_df)

    def test_it_combines_multiple_csvs(self):
        pipe = pemi.pipes.csv.LocalCsvFileSourcePipe(
            schema={
                'id':   {'type': 'string'},
                'name': {'type': 'string'}
            },
            paths=[
                Path(__file__).parent / Path('fixtures') / Path('id_name_1.csv'),
                Path(__file__).parent / Path('fixtures') / Path('id_name_2.csv')
            ]
        )
        pipe.execute()

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
        assert_frame_equal(pipe.targets['standard'].data, expected_df)


class TestLocalCsvFileTargetPipe(unittest.TestCase):
    def test_it_writes_a_csv(self):
        tmp_file = tempfile.NamedTemporaryFile()

        pipe = pemi.pipes.csv.LocalCsvFileTargetPipe(
            schema={
                'id':   {'type': 'string'},
                'name': {'type': 'string'}
            },
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

        pipe.sources['standard'].data = given_df
        pipe.execute()

        expected_csv = '\n'.join([
            'id|name',
            '1|one',
            '2|two',
            '3|three',
            ''
        ]).encode('utf-8')

        actual_csv = tmp_file.read()
        self.assertEqual(actual_csv, expected_csv)
