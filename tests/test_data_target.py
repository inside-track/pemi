import unittest
from pandas.util.testing import assert_frame_equal

import datetime

import pandas as pd

import pemi
import pemi.subjects.files
import pemi.interfaces.csv

import os
from pathlib import Path

import tempfile

# Very high-level feature-test style
# TODO: Substantial refactoring to make it easier to follow given-when-then style
class TestDataTarget(unittest.TestCase):
    def setUp(self):
        self.fixtures_path = Path(__file__).parent / Path('fixtures')

    def test_write_csv(self):
        my_schema = pemi.Schema(
            ('id'         , 'string', {'required': True}),
            ('name'       , 'string', {'length': 80, 'required': True}),
            ('is_awesome' , 'boolean', {}), # could I add a lambda to format these on output?
            ('sell_date'  , 'date', {'in_format': '%m/%d/%Y', 'to_create': True, 'to_update': False})
        )

        fd  = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()

        some_df = pd.DataFrame(
            {
                0: ['01',        'DBIRA',       True,  fd('2017-03-27')],
                2: ['03',       'Berber',      False,  fd('2017-03-22')],
                3: ['04',      'SoLikey',      False,  fd('2017-03-22')],
                4: ['05',  'Perfecticon',      False,  fd('2017-03-22')],
                5: ['06',   'Hopticular',       True,  fd('2017-03-22')],
                6: ['07',       'Malted',       None,  fd('2017-03-22')],
                7: ['08',        'Foamy',       True,  fd('2017-03-22')]
            },
            index= ['id',         'name','is_awesome',      'sell_date']
        ).transpose()

        tmp_file = tempfile.NamedTemporaryFile()
        dt_beers = pemi.subjects.files.LocalFilesTarget(
            my_schema,
            pemi.interfaces.csv.CsvFilesInterface,
            path=tmp_file.name,
            csv_opts={'sep': '|'}
        )
        dt_beers.from_engine(pemi.engines.PandasEngine(dt_beers.schema, some_df))

        expected_csv = '\n'.join([
            'id|name|is_awesome|sell_date',
            '01|DBIRA|True|2017-03-27',
            '03|Berber|False|2017-03-22',
            '04|SoLikey|False|2017-03-22',
            '05|Perfecticon|False|2017-03-22',
            '06|Hopticular|True|2017-03-22',
            '07|Malted||2017-03-22',
            '08|Foamy|True|2017-03-22\n'
        ]).encode('utf-8')

        actual_csv = tmp_file.read()
        self.assertEqual(actual_csv, expected_csv)
