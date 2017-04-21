import unittest
from pandas.util.testing import assert_frame_equal

import pandas as pd

import pemi.mappers.pandas
from pemi.mappers.pandas import PandasMapper
from pemi.mappers.handler import Handler
from pemi.mappers.field_map import FieldMap



def translate(val):
    if val == 1:
        return 'UNO'
    elif val == 2:
        return 'DOS'
    elif val == 3:
        return 'TRES'
    else:
        raise ValueError('Unknown translation: {}'.format(val))

def concatenate(delim=''):
    def doit(row):
        return delim.join(list(row.apply(str)))
    return doit

def mysplit(inrow):
    split_values = inrow['num_name'].split('-')
    outrow = { 'split_num': split_values[0], 'split_name': split_values[1] }
    return outrow

def unknown_recoder(val):
    return 'Unknown: {}'.format(val)


class TestCardinalities(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                'num': [1,2,3],
                'name': ['one', 'two', 'three'],
                'num_name': ['1-one', '2-two', '3-three']
            }
        )


    def test_one_to_one_map(self):
        '''
        One-to-one mapping
        '''
        mapper = PandasMapper(self.df,
            FieldMap(source='num', target='translated', transform=translate)
        )

        mapper.execute()
        result_df = mapper.mapped_df
        expected_df = pd.DataFrame({'translated': ['UNO', 'DOS', 'TRES']})

        assert_frame_equal(result_df, expected_df)

    def test_many_to_one_map(self):
        '''
        Many-to-one mapping
        '''

        mapper = PandasMapper(self.df,
            FieldMap(source=('name', 'num'), target='concatenated', transform=concatenate('-'))
        )

        mapper.execute()
        result_df = mapper.mapped_df
        expected_df = pd.DataFrame({'concatenated': ['one-1', 'two-2', 'three-3']})

        assert_frame_equal(result_df, expected_df)

    def test_one_to_many_map(self):
        '''
        One-to-many mapping
        '''

        mapper = PandasMapper(self.df,
            FieldMap(source='num_name', target=('split_name', 'split_num'), transform=mysplit)
        )

        mapper.execute()
        result_df = mapper.mapped_df
        expected_df = pd.DataFrame({
            'split_num': ['1', '2', '3'],
            'split_name': ['one', 'two', 'three']
        })

        assert_frame_equal(result_df, expected_df)

    def test_multiple_maps(self):
        '''
        Multiple mappings of various cardinalities
        '''
        mapper = PandasMapper(self.df,
            FieldMap(source='num', target='translated', transform=translate),
            FieldMap(source=('name', 'num'), target='concatenated', transform=concatenate('-')),
            FieldMap(source='num_name', target=('split_name', 'split_num'), transform=mysplit)
        )

        mapper.execute()
        result_df = mapper.mapped_df
        expected_df = pd.DataFrame(
            {
                'translated': ['UNO', 'DOS', 'TRES'],
                'concatenated': ['one-1', 'two-2', 'three-3'],
                'split_name': ['one', 'two', 'three'],
                'split_num': ['1', '2', '3']
            },
            columns=['translated', 'concatenated', 'split_name', 'split_num']
        )
        assert_frame_equal(result_df, expected_df)


class TestHandlerModes(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                'num': [1,20,3,40]
            }
        )

    def test_raise_mode(self):
        '''
        Raises an error immediately (default handler)
        '''
        mapper = PandasMapper(self.df,
            FieldMap(source='num', target='translated', transform=translate)
        )

        self.assertRaises(pemi.mappers.pandas.UncaughtMappingError, mapper.execute)


    def test_catch_mode(self):
        '''
        Errors are captured in an errors dataframe
        '''
        mapper = PandasMapper(self.df,
            FieldMap(source='num', target='translated', transform=translate, handler=Handler('catch'))
        )
        mapper.execute(raise_errors=False)
        mapped_df = mapper.mapped_df
        errors_df = mapper.errors_df

        expected_mapped_df = pd.DataFrame(
            {
                'translated': ['UNO','TRES']
            },
            index=[0,2]
        )

        expected_errors_df = pd.DataFrame(
            {
                'num': [20,40],
                '__mapping_errors__': [
                    [{'mode': 'catch', 'type': 'ValueError', 'message': 'Unknown translation: 20', 'index': 1}],
                    [{'mode': 'catch', 'type': 'ValueError', 'message': 'Unknown translation: 40', 'index': 3}]
                ]
            },
            index=[1,3],
            columns = ['num', '__mapping_errors__']
        )
        assert_frame_equal(mapped_df, expected_mapped_df)
        assert_frame_equal(errors_df, expected_errors_df)


    def test_catch_mode_raises(self):
        '''
        The catch handler will raise an error if errors are caught but not used
        '''
        mapper = PandasMapper(self.df,
            FieldMap(source='num', target='translated', transform=translate, handler=Handler('catch'))
        )
        self.assertRaises(pemi.mappers.pandas.UncaughtMappingError, mapper.execute)


    def test_recode_mode_one_to_one(self):
        '''
        The recode handler can be used to supply defaults for errors (one-to-one)
        '''
        def recoder(val):
            return 'Unknown: {}'.format(str(val))

        mapper = PandasMapper(self.df,
            FieldMap(source='num', target='translated', transform=translate, handler=Handler('recode', recode=recoder))
        )
        mapper.execute(raise_errors=False)
        mapped_df = mapper.mapped_df

        expected_mapped_df = pd.DataFrame(
            {
                'translated': ['UNO', 'Unknown: 20', 'TRES', 'Unknown: 40']
            },
        )
        assert_frame_equal(mapped_df, expected_mapped_df)


    def test_recode_mode_many_to_one(self):
        '''
        The recode handler can be used to supply defaults for errors (many-to-one)
        '''

        given_df = pd.DataFrame(
            {
                'num': ['1',2,'3'],
                'name': ['one', 'two', 'three']
            }
        )
        concatenate = lambda row: '='.join(list(row))
        recoder = lambda row: 'idk'

        mapper = PandasMapper(given_df,
            FieldMap(source=('num','name'), target=('combined'), transform=concatenate, handler=Handler('recode', recode=recoder))
        )
        mapper.execute(raise_errors=False)
        mapped_df = mapper.mapped_df

        expected_mapped_df = pd.DataFrame(
            {
                'combined': ['1=one', 'idk', '3=three']
            }
        )
        assert_frame_equal(mapped_df, expected_mapped_df)


    def test_recode_mode_many_to_many(self):
        '''
        The recode handler can be used to supply defaults for errors (many-to-many)
        '''
        given_df = pd.DataFrame(
            {
                'num_name': ['1-one', '2two', '3-three']
            }
        )

        def recoder(inrow):
            return {'split_num': None, 'split_name': None}

        mapper = PandasMapper(given_df,
            FieldMap(source='num_name', target=('split_num','split_name'), transform=mysplit, handler=Handler('recode', recode=recoder))
        )
        mapper.execute(raise_errors=False)
        mapped_df = mapper.mapped_df
        errors_df = mapper.errors_df

        expected_mapped_df = pd.DataFrame(
            {
                'split_num': ['1', None, '3'],
                'split_name': ['one', None, 'three']
            },
            columns = ['split_num', 'split_name']
        )
        assert_frame_equal(mapped_df, expected_mapped_df)

    def test_warn_mode(self):
        '''
        The warning handler effectively replaces errors with None
        '''
        mapper = PandasMapper(self.df,
            FieldMap(source='num', target='translated', transform=translate, handler=Handler('warn'))
        )
        mapper.execute(raise_errors=False)
        mapped_df = mapper.mapped_df
        errors_df = mapper.errors_df

        expected_mapped_df = pd.DataFrame(
            {
                'translated': ['UNO',None,'TRES',None]
            }
        )

        assert_frame_equal(mapped_df, expected_mapped_df)
