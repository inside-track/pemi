import pandas as pd
import numpy as np
import pytest

import pemi
import pemi.data
import pemi.testing as pt
import pemi.pipes.pd
from pemi.fields import *
from pemi.pd_mapper import *

#TODO: Can I remove this?
#pylint: disable=redefined-outer-name

class PdLookupJoinPipeScenarioFactory():
    def __init__(self, scenario, pipe):
        self.scenario = scenario
        self.pipe = pipe

    @staticmethod
    def case_keys():
        ids = list(range(1000))
        for i in ids:
            yield {
                'main_source': {'key': 'k{}'.format(i)},
                'lookup': {'lkey': 'k{}'.format(i)},
                'main_target': {'key': 'k{}'.format(i)},
                'errors': {'key': 'k{}'.format(i)}
            }

    def scenario_setup(self):
        return {
            'runner': self.pipe.flow,
            'case_keys': self.case_keys(),
            'sources': {
                'main_source': pipe.sources['main'],
                'lookup': pipe.sources['lookup']
            },
            'targets': {
                'main_target': pipe.targets['main'],
                'errors': pipe.targets['errors']
            }
        }

    def ex_main(self):
        return pemi.data.Table(
            '''
            | key    | words |
            | -      | -     |
            | {k[1]} | words |
            | {k[1]} | words |
            | {k[3]} | more  |
            | {k[7]} | words |
            | {k[4]} | even  |
            | {k[4]} | more  |
            '''.format(k=self.scenario.case_keys.cache('main_source', 'key')),
            schema=pemi.Schema(
                key=StringField(),
                words=StringField()
            )
        )

    def ex_lookup(self):
        return pemi.data.Table(
            '''
            | lkey    | values | words  |
            | -       | -      | -      |
            | {k[1]}  | one    | I      |
            | {k[4]}  | four   | people |
            | {k[1]}  | ONE    | fnord  |
            | {k[3]}  | three  | dead   |
            | {k[4]}  | FOUR   | fnord  |
            | {k[2]}  | two    | see    |
            '''.format(k=self.scenario.case_keys.cache('lookup', 'lkey')),
            schema=pemi.Schema(
                lkey=StringField(),
                values=StringField(),
                words=StringField()
            )
        )

    def background(self):
        return [
            pt.when.example_for_source(self.scenario.sources['main_source'], self.ex_main()),
            pt.when.example_for_source(self.scenario.sources['lookup'], self.ex_lookup())
        ]



with pt.Scenario('PdLookupJoinPipe Basics') as scenario:
    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey']
    )
    factory = PdLookupJoinPipeScenarioFactory(scenario, pipe)
    scenario.setup(**factory.scenario_setup())

    with scenario.case('it performs the lookup') as case:
        expected = pemi.data.Table(
            '''
            | key    | words | lkey     | values  | words_lkp |
            | -      | -     | -        | -       | -         |
            | {k[1]} | words | {lk[1]}  | one     | I         |
            | {k[1]} | words | {lk[1]}  | one     | I         |
            | {k[3]} | more  | {lk[3]}  | three   | dead      |
            | {k[4]} | even  | {lk[4]}  | four    | people    |
            | {k[4]} | more  | {lk[4]}  | four    | people    |
            '''.format(
                k=scenario.case_keys.cache('main_target', 'key'),
                lk=scenario.case_keys.cache('lookup', 'lkey')
            )
        )

        case.when(
            *factory.background()
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )

    with scenario.case('it redirects errors') as case:
        expected = pemi.data.Table(
            '''
            | key    | words |
            | -      | -     |
            | {k[7]} | words |
            '''.format(k=scenario.case_keys.cache('errors', 'key'))
        )

        case.when(
            *factory.background()
        ).then(
            pt.then.target_matches_example(scenario.targets['errors'], expected)
        )


with pt.Scenario('PdLookupJoinPipe IgnoreHandler') as scenario:
    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        missing_handler=RowHandler('ignore')
    )

    factory = PdLookupJoinPipeScenarioFactory(scenario, pipe)
    scenario.setup(**factory.scenario_setup())

    with scenario.case('it does not redirect errors') as case:
        case.when(
            *factory.background()
        ).then(
            pt.then.target_is_empty(scenario.targets['errors'])
        )

    with scenario.case('it works when the lookup is empty') as case:
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
            | key    | words |
            | -      | -     |
            | {k[1]} | words |
            | {k[1]} | words |
            | {k[3]} | more  |
            | {k[7]} | words |
            | {k[4]} | even  |
            | {k[4]} | more  |
            '''.format(
                k=scenario.case_keys.cache('main_target', 'key')
            )
        )

        case.when(
            *factory.background(),
            pt.when.example_for_source(scenario.sources['lookup'], ex_lookup)
        ).then(
            pt.then.target_is_empty(scenario.targets['errors'])
        )



with pt.Scenario('PdLookupJoinPipe FillNa') as scenario:
    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        missing_handler=RowHandler('ignore'),
        fillna={'value': 'EMPTY'}
    )
    factory = PdLookupJoinPipeScenarioFactory(scenario, pipe)
    scenario.setup(**factory.scenario_setup())

    with scenario.case('it fills in missing values') as case:
        expected = pemi.data.Table(
            '''
            | key    | words | lkey    | values  | words_lkp |
            | -      | -     | -       | -       | -         |
            | {k[1]} | words | {lk[1]} | one     | I         |
            | {k[1]} | words | {lk[1]} | one     | I         |
            | {k[3]} | more  | {lk[3]} | three   | dead      |
            | {k[7]} | words | EMPTY   | EMPTY   | EMPTY     |
            | {k[4]} | even  | {lk[4]} | four    | people    |
            | {k[4]} | more  | {lk[4]} | four    | people    |
            '''.format(
                k=scenario.case_keys.cache('main_target', 'key'),
                lk=scenario.case_keys.cache('lookup', 'lkey')
            )
        )

        case.when(
            *factory.background()
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )



with pt.Scenario('PdLookupJoinPipe Lookup Prefix') as scenario:
    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        lookup_prefix='existing_'
    )

    factory = PdLookupJoinPipeScenarioFactory(scenario, pipe)
    scenario.setup(**factory.scenario_setup())

    with scenario.case('it prefixes lookup fields') as case:
        expected = pemi.data.Table(
            '''
            | key    | words | lkey    | existing_values | existing_words |
            | -      | -     | -       | -               | -              |
            | {k[1]} | words | {lk[1]} | one             | I              |
            | {k[1]} | words | {lk[1]} | one             | I              |
            | {k[3]} | more  | {lk[3]} | three           | dead           |
            | {k[4]} | even  | {lk[4]} | four            | people         |
            | {k[4]} | more  | {lk[4]} | four            | people         |
            '''.format(
                k=scenario.case_keys.cache('main_target', 'key'),
                lk=scenario.case_keys.cache('lookup', 'lkey')
            )
        )

        case.when(
            *factory.background()
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )


with pt.Scenario('PdLookupJoinPipe Prefix Missing') as scenario:
    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        lookup_prefix='existing_',
        missing_handler=RowHandler('ignore')
    )

    factory = PdLookupJoinPipeScenarioFactory(scenario, pipe)
    scenario.setup(**factory.scenario_setup())

    with scenario.case('it prefixes lookup fields when lookup is empty') as case:
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
            | key    | words | lkey | existing_values | existing_words |
            | -      | -     | -    | -               | -              |
            | {k[1]} | words |      |                 |                |
            | {k[1]} | words |      |                 |                |
            | {k[3]} | more  |      |                 |                |
            | {k[7]} | words |      |                 |                |
            | {k[4]} | even  |      |                 |                |
            | {k[4]} | more  |      |                 |                |
            '''.format(
                k=scenario.case_keys.cache('main_target', 'key')
            ),
            schema=pemi.Schema(
                values=StringField(),
                words=StringField()
            )
        )

        case.when(
            *factory.background(),
            pt.when.example_for_source(scenario.sources['lookup'], lookup)
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )



with pt.Scenario('PdLookupJoinPipe Indicator') as scenario:
    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        missing_handler=RowHandler('ignore'),
        indicator='lkp_found'
    )

    factory = PdLookupJoinPipeScenarioFactory(scenario, pipe)
    scenario.setup(**factory.scenario_setup())

    with scenario.case('it adds an indicator') as case:
        expected = pemi.data.Table(
            '''
            | key    | words | lkp_found |
            | -      | -     | -         |
            | {k[1]} | words | True      |
            | {k[1]} | words | True      |
            | {k[3]} | more  | True      |
            | {k[7]} | words | False     |
            | {k[4]} | even  | True      |
            | {k[4]} | more  | True      |
            '''.format(
                k=scenario.case_keys.cache('main_target', 'key')
            )
        )

        case.when(
            *factory.background()
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )



with pt.Scenario('PdLookupJoinPipe Blank Keys') as scenario:
    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['key']
    )

    def case_keys():
        ids = list(range(1000))
        for i in ids:
            yield {
                'main_source': {'alt_key': 'k{}'.format(i)},
                'main_target': {'alt_key': 'k{}'.format(i)},
                'errors': {'alt_key': 'k{}'.format(i)}
            }

    scenario.setup(
        runner=pipe.flow,
        case_keys=case_keys(),
        sources={
            'main_source': pipe.sources['main'],
            'lookup': pipe.sources['lookup']
        },
        targets={
            'main_target': pipe.targets['main'],
            'errors': pipe.targets['errors']
        }
    )

    with scenario.case('it does not use blanks as keys') as case:
        ex_main = pemi.data.Table(
            '''
            | key | alt_key |
            | -   | -       |
            | one | {k[1]}  |
            |     | {k[2]}  |
            | two | {k[3]}  |
            '''.format(k=scenario.case_keys.cache('main_source', 'alt_key')),
            schema=pemi.Schema(
                key=StringField(),
                alt_key=StringField()
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
            | key | value | alt_key |
            | -   | -     | -       |
            | one | ONE   | {k[1]}  |
            | two | TWO   | {k[3]}  |
            '''.format(k=scenario.case_keys.cache('main_target', 'alt_key')),
            schema=pemi.Schema(
                key=StringField(),
                value=StringField(),
                alt_key=StringField()
            )
        )


        errors = pemi.data.Table(
            '''
            | key | alt_key |
            | -   | -       |
            |     | {k[2]}  |
            '''.format(k=scenario.case_keys.cache('main_target', 'alt_key')),
            schema=pemi.Schema(
                key=StringField(),
                alt_key=StringField()
            )
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main_source'], ex_main),
            pt.when.example_for_source(scenario.sources['lookup'], ex_lookup)
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected),
            pt.then.target_matches_example(scenario.targets['errors'], errors),
        )





class TestPdConcatPipe: #pylint: disable=no-self-use
    def test_it_concatenates_sources(self):
        pipe = pemi.pipes.pd.PdConcatPipe(sources=['s1', 's2'])
        pipe.sources['s1'].df = pd.DataFrame({
            'origin': ['s1', 's1', 's1'],
            'f1': [1, 2, 3]
        })

        pipe.sources['s2'].df = pd.DataFrame({
            'origin': ['s2', 's2'],
            'f2': [1, 2]
        })

        pipe.flow()
        expected_df = pd.DataFrame({
            'origin': ['s1', 's1', 's1', 's2', 's2'],
            'f1': [1, 2, 3, np.nan, np.nan],
            'f2': [np.nan, np.nan, np.nan, 1, 2]
        }, index=[0, 1, 2, 0, 1])
        actual_df = pipe.targets['main'].df
        pt.assert_frame_equal(actual_df, expected_df)

    def test_given_no_data(self):
        'It returns an empty dataframe'
        pipe = pemi.pipes.pd.PdConcatPipe(sources=['s1', 's2'])
        pipe.flow()

        expected_df = pd.DataFrame()
        actual_df = pipe.targets['main'].df
        pt.assert_frame_equal(actual_df, expected_df)


class TestPdFieldValueForkPipe: #pylint: disable=no-self-use
    @pytest.fixture(scope='class')
    def pipe(self):
        pipe = pemi.pipes.pd.PdFieldValueForkPipe(
            field='target',
            forks=['create', 'update', 'empty']
        )

        df = pd.DataFrame({
            'target': ['create', 'update', 'update', 'else1', 'create', 'else2'],
            'values': [1, 2, 3, 4, 5, 6]
        })

        pipe.sources['main'].df = df
        pipe.flow()
        return pipe

    def test_it_forks_data_to_create(self, pipe):
        expected_df = pd.DataFrame({
            'target': ['create', 'create'],
            'values': [1, 5]
        }, index=[0, 4])
        actual_df = pipe.targets['create'].df
        pt.assert_frame_equal(actual_df, expected_df)

    def test_it_forks_data_to_update(self, pipe):
        expected_df = pd.DataFrame({
            'target': ['update', 'update'],
            'values': [2, 3]
        }, index=[1, 2])
        actual_df = pipe.targets['update'].df
        pt.assert_frame_equal(actual_df, expected_df)

    def test_it_puts_unknown_values_in_remainder(self, pipe):
        expected_df = pd.DataFrame({
            'target': ['else1', 'else2'],
            'values': [4, 6]
        }, index=[3, 5])
        actual_df = pipe.targets['remainder'].df
        pt.assert_frame_equal(actual_df, expected_df)

    def test_it_creates_an_empty_dataframe_with_the_right_columns(self, pipe):
        expected_df = pd.DataFrame(columns=['target', 'values'])
        actual_df = pipe.targets['empty'].df
        pt.assert_frame_equal(actual_df, expected_df)
