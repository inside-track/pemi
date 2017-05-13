import unittest
from pandas.util.testing import assert_frame_equal

import datetime
import decimal

import pandas as pd

import pemi
import pemi.subjects.files
import pemi.interfaces.csv

import os
from pathlib import Path

# Very high-level feature-test style
# TODO: Substantial refactoring to make it easier to follow given-when-then style
class TestDataSource(unittest.TestCase):
    def setUp(self):
        self.fixtures_path = Path(__file__).parent / Path('fixtures')

    def test_read_csv(self):
        my_schema = pemi.Schema(
            ('id'         , 'string', {'required': True}),
            ('name'       , 'string', {'length': 80, 'required': True}),
            ('is_awesome' , 'boolean', {}),
            ('price'      , 'decimal', {'precision': 4, 'scale': 2}),
            ('details'    , 'json', {}),
            ('sell_date'  , 'date', {'in_format': '%m/%d/%Y', 'to_create': True, 'to_update': False}),
            ('updated_at' , 'datetime', {'in_format': '%m/%d/%Y %H:%M:%S', 'to_create': True, 'to_update': False}),
        )

        ds_beers = pemi.subjects.files.LocalFilesSource(
            my_schema,
            pemi.interfaces.csv.CsvFilesInterface,
            path=self.fixtures_path / Path('beers.csv'),
            csv_opts={}
        )
        pd_beers = ds_beers.to_pandas()
        beers_df = pd_beers.df
        beers_df_errors = pd_beers.df_errors

        fdt = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        fd  = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()
        fdc = lambda x: decimal.Decimal(x)

        expected_df = pd.DataFrame(
            {
                0: ['01',        'DBIRA',       True,  fdc('10.83'),      {},  fd('2017-03-27'), fdt('2017-01-01 23:39:39')],
                2: ['03',       'Berber',      False,  fdc('10.83'),      {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                3: ['04',      'SoLikey',      False,  fdc('10.83'),      {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                4: ['05',  'Perfecticon',      False,          None,      {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                5: ['06',   'Hopticular',       True,  fdc('10.83'),      {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                6: ['07',       'Malted',       None,  fdc('10.83'),      {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')],
                7: ['08',        'Foamy',       True,  fdc('10.83'),      {},  fd('2017-03-22'), fdt('2017-01-01 23:39:39')]
            },
            index= ['id',         'name','is_awesome',      'price','details',     'sell_date',               'updated_at'],
        ).transpose()

        assert_frame_equal(beers_df, expected_df)
