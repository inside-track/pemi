import unittest

import pandas as pd

import pemi
import pemi.pd_mapper
import pemi.testing
from pemi.pd_mapper import *

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

def mysplit(row):
    split_values = row['num_name'].split('-')
    row['split_num'] = split_values[0]
    row['split_name'] = split_values[1]
    return row

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
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate)
        ]).apply()

        result_df = mapper.mapped_df
        expected_df = pd.DataFrame({'translated': ['UNO', 'DOS', 'TRES']})

        pemi.testing.assert_frame_equal(result_df, expected_df)

    def test_many_to_one_map(self):
        '''
        Many-to-one mapping
        '''

        mapper = PdMapper(self.df, maps=[
            PdMap(source=('name', 'num'), target='concatenated', transform=concatenate('-'))
        ]).apply()

        result_df = mapper.mapped_df
        expected_df = pd.DataFrame({'concatenated': ['one-1', 'two-2', 'three-3']})

        pemi.testing.assert_frame_equal(result_df, expected_df)

    def test_one_to_many_map(self):
        '''
        One-to-many mapping
        '''

        mapper = PdMapper(self.df, maps=[
            PdMap(source='num_name', target=('split_name', 'split_num'), transform=mysplit)
        ]).apply()

        result_df = mapper.mapped_df
        expected_df = pd.DataFrame({
            'split_num': ['1', '2', '3'],
            'split_name': ['one', 'two', 'three']
        })

        pemi.testing.assert_frame_equal(result_df, expected_df)

    def test_multiple_maps(self):
        '''
        Multiple mappings of various cardinalities
        '''
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate),
            PdMap(source=('name', 'num'), target='concatenated', transform=concatenate('-')),
            PdMap(source='num_name', target=('split_name', 'split_num'), transform=mysplit)
        ]).apply()

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
        pemi.testing.assert_frame_equal(result_df, expected_df)


    def test_one_to_zero_map(self):
        '''
        One-to-zero mapping
        '''

        def recorder(values):
            def _recorder(value):
                values.append(value)
            return _recorder

        saved = []
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', transform=recorder(saved)),
        ]).apply()

        self.assertEqual(saved, [1,2,3])


    def test_many_to_zero_map(self):
        '''
        Many-to-zero mapping
        '''

        def recorder(values):
            def _recorder(row):
                values.append('-'.join([str(v) for v in row.values]))
            return _recorder

        saved = []
        mapper = PdMapper(self.df, maps=[
            PdMap(source=['num', 'name'], transform=recorder(saved)),
        ]).apply()

        self.assertEqual(saved, ['1-one','2-two','3-three'])


    def test_zero_to_one_map(self):
        '''
        Zero-to-one mapping
        '''
        mapper = PdMapper(self.df, maps=[
            PdMap(target='my_constant', transform=lambda v: 5)
        ]).apply()

        result_df = mapper.mapped_df
        expected_df = pd.DataFrame({'my_constant': [5,5,5]})

        pemi.testing.assert_frame_equal(result_df, expected_df)



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
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate)
        ])

        self.assertRaises(ValueError, mapper.apply)


    def test_warn_mode(self):
        '''
        The warning handler replaces errors with None
        '''
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate, handler=RowHandler('warn'))
        ]).apply()

        mapped_df = mapper.mapped_df

        expected_mapped_df = pd.DataFrame(
            {
                'translated': ['UNO',None,'TRES',None]
            }
        )

        pemi.testing.assert_frame_equal(mapped_df, expected_mapped_df)


    def test_warn_mode_catch(self):
        '''
        The warning handler records warning records in the errors dataframe
        '''
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate, handler=RowHandler('warn'))
        ]).apply()

        errors_df = mapper.errors_df

        expected_errors_df = pd.DataFrame(
            {
                '__error_index__': [1,3],
                '__error_message__': ['Unknown translation: 20', 'Unknown translation: 40']
            },
            index=[1,3]
        )

        pemi.testing.pemi.testing.assert_frame_equal(errors_df[expected_errors_df.columns], expected_errors_df)


    def test_exclude_mode(self):
        '''
        Errors are excluded from the mapped dataframe
        '''
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate, handler=RowHandler('exclude'))
        ]).apply()

        mapped_df = mapper.mapped_df
        errors_df = mapper.errors_df

        expected_mapped_df = pd.DataFrame(
            {
                'translated': ['UNO','TRES']
            },
            index=[0,2]
        )

        pemi.testing.assert_frame_equal(mapped_df, expected_mapped_df)


    def test_exclude_mode_catch(self):
        '''
        Errors are captured in an errors dataframe
        '''
        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate, handler=RowHandler('exclude'))
        ]).apply()

        mapped_df = mapper.mapped_df
        errors_df = mapper.errors_df


        expected_errors_df = pd.DataFrame(
            {
                '__error_index__': [1,3],
                '__error_message__': ['Unknown translation: 20', 'Unknown translation: 40']
            },
            index=[1,3]
        )

        pemi.testing.assert_frame_equal(errors_df[expected_errors_df.columns], expected_errors_df)



    def test_recode_mode_one_to_one(self):
        '''
        The recode handler can be used to supply defaults for errors (one-to-one)
        '''
        def recoder(val):
            return 'Unknown: {}'.format(str(val))

        mapper = PdMapper(self.df, maps=[
            PdMap(source='num', target='translated', transform=translate, handler=RowHandler('recode', recode=recoder))
        ]).apply()
        mapped_df = mapper.mapped_df

        expected_mapped_df = pd.DataFrame(
            {
                'translated': ['UNO', 'Unknown: 20', 'TRES', 'Unknown: 40']
            },
        )
        pemi.testing.assert_frame_equal(mapped_df, expected_mapped_df)


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

        mapper = PdMapper(given_df, maps=[
            PdMap(source=('num','name'), target=('combined'), transform=concatenate, handler=RowHandler('recode', recode=recoder))
        ]).apply()
        mapped_df = mapper.mapped_df

        expected_mapped_df = pd.DataFrame(
            {
                'combined': ['1=one', 'idk', '3=three']
            }
        )
        pemi.testing.assert_frame_equal(mapped_df, expected_mapped_df)


    def test_recode_mode_many_to_many(self):
        '''
        The recode handler can be used to supply defaults for errors (many-to-many)
        '''
        given_df = pd.DataFrame(
            {
                'num_name': ['1-one', '2two', '3-three']
            }
        )

        def recoder(row):
            return {'split_num': None, 'split_name': None}

        mapper = PdMapper(given_df, maps=[
            PdMap(source='num_name', target=('split_num','split_name'), transform=mysplit, handler=RowHandler('recode', recode=recoder))
        ]).apply()

        mapped_df = mapper.mapped_df
        errors_df = mapper.errors_df

        expected_mapped_df = pd.DataFrame(
            {
                'split_num': ['1', None, '3'],
                'split_name': ['one', None, 'three']
            },
            columns = ['split_num', 'split_name']
        )
        pemi.testing.assert_frame_equal(mapped_df, expected_mapped_df)


class TestPassthrough(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                'field1': [1,2,3],
                'field2': [1,2,3],
                'field3': [1,2,3],
                'field4': [1,2,3]
            }
        )

    def test_fields_can_be_passed_through(self):
        mapper = PdMapper(self.df, mapped_df=self.df.copy(), maps=[
            PdMap(source='field3', target='field3', transform=lambda v: v + 10),
            PdMap(source='field2', target='field2p', transform=lambda v: v + 10)
        ]).apply()

        expected_mapped_df = pd.DataFrame(
            {
                'field1': [1,2,3],
                'field2': [1,2,3],
                'field3': [11,12,13],
                'field4': [1,2,3],
                'field2p': [11,12,13]
            },
            columns = ['field1', 'field2', 'field3', 'field4', 'field2p']
        )

        pemi.testing.assert_frame_equal(mapper.mapped_df, expected_mapped_df)
