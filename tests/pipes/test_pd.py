import unittest

import pandas as pd
import numpy as np
import pytest

import pemi
import pemi.data
import pemi.testing
import pemi.pipes.pd
from pemi.fields import *
from pemi.pd_mapper import *

class TestPdLookupJoinPipe(unittest.TestCase):
    def ex_main(self):
        return pemi.data.Table(
            '''
            | key | words |
            | -   | -     |
            | k1  | words |
            | k1  | words |
            | k3  | more  |
            | k7  | words |
            | k4  | even  |
            | k4  | more  |
            ''',
            schema=pemi.Schema(
                key=StringField(),
                words=StringField()
            )
        )

    def ex_lookup(self):
        return pemi.data.Table(
            '''
            | lkey | values | words  |
            | -    | -      | -      |
            | k1   | one    | I      |
            | k4   | four   | people |
            | k1   | ONE    | fnord  |
            | k3   | three  | dead   |
            | k4   | FOUR   | fnord  |
            | k2   | two    | see    |
            ''',
            schema=pemi.Schema(
                lkey=StringField(),
                values=StringField(),
                words=StringField()
            )
        )

    def rules(self, pipe):
        return pemi.testing.Rules(
            source_subjects=[
                pipe.sources['main'],
                pipe.sources['lookup']
            ],
            target_subjects=[
                pipe.targets['main'],
                pipe.targets['errors']
            ]
        )

    def scenario(self, pipe, rules):
        return pemi.testing.Scenario(
            runner = pipe.flow,
            source_subjects=[
                pipe.sources['main'],
                pipe.sources['lookup']
            ],
            target_subjects=[
                pipe.targets['main'],
                pipe.targets['errors']
            ],
            givens=[
                rules.when_example_for_source(
                    self.ex_main(),
                    source_subject=pipe.sources['main']
                ),
                rules.when_example_for_source(
                    self.ex_lookup(),
                    source_subject=pipe.sources['lookup']
                )
            ]
        )

    def test_it_performs_the_lookup(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey']
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)

        expected = pemi.data.Table(
            '''
            | key | words | lkey | values  | words_lkp |
            | -   | -     | -    | -       | -         |
            | k1  | words | k1   | one     | I         |
            | k1  | words | k1   | one     | I         |
            | k3  | more  | k3   | three   | dead      |
            | k4  | even  | k4   | four    | people    |
            | k4  | more  | k4   | four    | people    |
            '''
        )

        scenario.then(
            rules.then_target_matches_example(
                expected,
                target_subject = pipe.targets['main']
            )
        ).run()

    def test_it_redirects_errors(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey']
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)

        expected = pemi.data.Table(
            '''
            | key | words |
            | -   | -     |
            | k7  | words |
            '''
        )

        scenario.then(
            rules.then_target_matches_example(
                expected,
                target_subject = pipe.targets['errors']
            )
        ).run()

    def test_it_does_not_redirect_errors(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey'],
            missing_handler=RowHandler('ignore')
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)

        scenario.then(
            rules.then_target_is_empty(
                target_subject = pipe.targets['errors']
            )
        ).run()


    def test_it_fills_in_missing_values(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey'],
            missing_handler=RowHandler('ignore'),
            fillna={'value': 'EMPTY'}
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)

        expected = pemi.data.Table(
            '''
            | key | words | lkey  | values  | words_lkp |
            | -   | -     | -     | -       | -         |
            | k1  | words | k1    | one     | I         |
            | k1  | words | k1    | one     | I         |
            | k3  | more  | k3    | three   | dead      |
            | k7  | words | EMPTY | EMPTY   | EMPTY     |
            | k4  | even  | k4    | four    | people    |
            | k4  | more  | k4    | four    | people    |
            '''
        )

        scenario.then(
            rules.then_target_matches_example(
                expected,
                target_subject = pipe.targets['main']
            )
        ).run()

    def test_it_prefixes_lookup_fields(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey'],
            lookup_prefix='existing_'
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)

        expected = pemi.data.Table(
            '''
            | key | words | lkey | existing_values | existing_words |
            | -   | -     | -    | -               | -              |
            | k1  | words | k1   | one             | I              |
            | k1  | words | k1   | one             | I              |
            | k3  | more  | k3   | three           | dead           |
            | k4  | even  | k4   | four            | people         |
            | k4  | more  | k4   | four            | people         |
            '''
        )

        scenario.then(
            rules.then_target_matches_example(
                expected,
                target_subject = pipe.targets['main']
            )
        ).run()

    def test_it_prefixes_lookup_fields_when_lookup_is_empty(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey'],
            lookup_prefix='existing_',
            missing_handler=RowHandler('ignore')
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)


        lookup = pemi.data.Table(
            '''
            | lkey | values | words  |
            | -    | -      | -      |
            ''',
            schema=pemi.Schema(
                lkey=StringField(),
                values=StringField(),
                words=StringField()
            )
        )

        expected = pemi.data.Table(
            '''
            | key | words | lkey | existing_values | existing_words |
            | -   | -     | -    | -               | -              |
            | k1  | words |      |                 |                |
            | k1  | words |      |                 |                |
            | k3  | more  |      |                 |                |
            | k7  | words |      |                 |                |
            | k4  | even  |      |                 |                |
            | k4  | more  |      |                 |                |
            ''',
            schema=pemi.Schema(
                values=StringField(),
                words=StringField()
            )
        )

        scenario.when(
            rules.when_example_for_source(lookup, source_subject=pipe.sources['lookup'])
        ).then(
            rules.then_target_matches_example(
                expected,
                target_subject = pipe.targets['main']
            )
        ).run()


    def test_it_adds_an_indicator(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey'],
            missing_handler = RowHandler('ignore'),
            indicator = 'lkp_found'
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)

        expected = pemi.data.Table(
            '''
            | key | words | lkp_found |
            | -   | -     | -         |
            | k1  | words | True      |
            | k1  | words | True      |
            | k3  | more  | True      |
            | k7  | words | False     |
            | k4  | even  | True      |
            | k4  | more  | True      |
            '''
        )

        scenario.then(
            rules.then_target_matches_example(
                expected,
                target_subject = pipe.targets['main']
            )
        ).run()

    def test_it_works_when_lookup_is_empty(self):
        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['lkey'],
            missing_handler = RowHandler('ignore')
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)

        ex_lookup = pemi.data.Table(
            '''
            | lkey | values | words  |
            | -    | -      | -      |
            ''',
            schema=pemi.Schema(
                lkey=StringField(),
                values=StringField(),
                words=StringField()
            )
        )

        expected = pemi.data.Table(
            '''
            | key | words |
            | -   | -     |
            | k1  | words |
            | k1  | words |
            | k3  | more  |
            | k7  | words |
            | k4  | even  |
            | k4  | more  |
            '''
        )

        scenario.when(
            rules.when_example_for_source(ex_lookup, source_subject=pipe.sources['lookup'])
        ).then(
            rules.then_target_matches_example(
                expected,
                target_subject = pipe.targets['main']
            )
        ).run()



class TestPdLookupJoinPipeOnBlanks(unittest.TestCase):
    def rules(self, pipe):
        return pemi.testing.Rules(
            source_subjects=[
                pipe.sources['main'],
                pipe.sources['lookup']
            ],
            target_subjects=[
                pipe.targets['main'],
                pipe.targets['errors']
            ]
        )

    def scenario(self, pipe, rules):
        return pemi.testing.Scenario(
            runner = pipe.flow,
            source_subjects=[
                pipe.sources['main'],
                pipe.sources['lookup']
            ],
            target_subjects=[
                pipe.targets['main'],
                pipe.targets['errors']
            ]
        )

    def test_it_does_not_use_blanks_as_keys(self):
        ex_main = pemi.data.Table(
            '''
            | key | data |
            | -   | -    |
            | one | a    |
            |     | b    |
            | two | c    |
            ''',
            schema=pemi.Schema(
                key=StringField()
            )
        )

        ex_lookup = pemi.data.Table(
            '''
            | key | value     |
            | -   | -         |
            | one | ONE       |
            | two | TWO       |
            |     | NOT THREE |
            |     | NOT FOUR  |
            ''',
            schema=pemi.Schema(
                key=StringField()
            )
        )

        expected = pemi.data.Table(
            '''
            | key | value     |
            | -   | -         |
            | one | ONE       |
            | two | TWO       |
            '''
        )

        pipe = pemi.pipes.pd.PdLookupJoinPipe(
            main_key = ['key'],
            lookup_key = ['key']
        )

        rules = self.rules(pipe)
        scenario = self.scenario(pipe, rules)


        scenario.when(
            rules.when_example_for_source(
                ex_main, source_subject=pipe.sources['main']
            ),
            rules.when_example_for_source(
                ex_lookup, source_subject=pipe.sources['lookup']
            ),
        ).then(
            rules.then_target_matches_example(
                expected, target_subject=pipe.targets['main']
            )
        )
        scenario.run()



class TestPdConcatPipe(unittest.TestCase):
    def test_it_concatenates_sources(self):
        pipe = pemi.pipes.pd.PdConcatPipe(sources=['s1', 's2'])
        pipe.sources['s1'].df = pd.DataFrame({
            'origin': ['s1','s1','s1'],
            'f1': [1,2,3]
        })

        pipe.sources['s2'].df = pd.DataFrame({
            'origin': ['s2', 's2'],
            'f2': [1,2]
        })

        pipe.flow()
        expected_df = pd.DataFrame({
            'origin': ['s1', 's1', 's1', 's2', 's2'],
            'f1': [1,2,3, np.nan, np.nan],
            'f2': [np.nan, np.nan, np.nan, 1, 2]
        }, index = [0,1,2,0,1])
        actual_df = pipe.targets['main'].df
        pemi.testing.assert_frame_equal(actual_df, expected_df)

    def test_given_no_data(self):
        'It returns an empty dataframe'
        pipe = pemi.pipes.pd.PdConcatPipe(sources=['s1', 's2'])
        pipe.flow()

        expected_df = pd.DataFrame()
        actual_df = pipe.targets['main'].df
        pemi.testing.assert_frame_equal(actual_df, expected_df)


class TestPdFieldValueForkPipe(unittest.TestCase):
    def setUp(self):
        self.pipe = pemi.pipes.pd.PdFieldValueForkPipe(
            field='target',
            forks=['create', 'update', 'empty']
        )

        df = pd.DataFrame({
            'target': ['create', 'update', 'update', 'else1', 'create', 'else2'],
            'values': [1,2,3,4,5,6]
        })

        self.pipe.sources['main'].df = df
        self.pipe.flow()


    def test_it_forks_data_to_create(self):
        expected_df = pd.DataFrame({
            'target': ['create', 'create'],
            'values': [1,5]
        }, index=[0,4])
        actual_df = self.pipe.targets['create'].df
        pemi.testing.assert_frame_equal(actual_df, expected_df)


    def test_it_forks_data_to_update(self):
        expected_df = pd.DataFrame({
            'target': ['update', 'update'],
            'values': [2,3]
        }, index=[1,2])
        actual_df = self.pipe.targets['update'].df
        pemi.testing.assert_frame_equal(actual_df, expected_df)

    def test_it_puts_unknown_values_in_remainder(self):
        expected_df = pd.DataFrame({
            'target': ['else1', 'else2'],
            'values': [4,6]
        }, index=[3,5])
        actual_df = self.pipe.targets['remainder'].df
        pemi.testing.assert_frame_equal(actual_df, expected_df)

    def test_it_creates_an_empty_dataframe_with_the_right_columns(self):
        expected_df = pd.DataFrame(columns=['target', 'values'])
        actual_df = self.pipe.targets['empty'].df
        pemi.testing.assert_frame_equal(actual_df, expected_df)
