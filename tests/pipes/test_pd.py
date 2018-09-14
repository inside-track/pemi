import pandas as pd
import numpy as np
import pytest
import factory

import pemi
import pemi.data
import pemi.testing as pt
import pemi.pipes.pd
from pemi.fields import *

class KeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence('k{}'.format)


def generate_scenario(name, pipe):
    return pt.Scenario(
        name=name,
        pipe=pipe,
        factories={
            'keys': KeyFactory
        },
        sources={
            'main_source': lambda pipe: pipe.sources['main'],
            'lookup': lambda pipe: pipe.sources['lookup']
        },
        targets={
            'main_target': lambda pipe: pipe.targets['main'],
            'errors': lambda pipe: pipe.targets['errors']
        },
        target_case_collectors={
            'main_target': pt.CaseCollector(subject_field='key', factory='keys',
                                            factory_field='id'),
            'errors': pt.CaseCollector(subject_field='key', factory='keys',
                                       factory_field='id'),
        }
    )

def background(scenario):
    ex_main = pemi.data.Table(
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
            k=scenario.factories['keys']['id']
        ),
        schema=pemi.Schema(
            key=StringField(),
            words=StringField()
        )
    )

    ex_lookup = pemi.data.Table(
        '''
        | lkey    | values | words  |
        | -       | -      | -      |
        | {k[1]}  | one    | I      |
        | {k[4]}  | four   | people |
        | {k[1]}  | ONE    | fnord  |
        | {k[3]}  | three  | dead   |
        | {k[4]}  | FOUR   | fnord  |
        | {k[2]}  | two    | see    |
        '''.format(
            k=scenario.factories['keys']['id']
        ),
        schema=pemi.Schema(
            lkey=StringField(),
            values=StringField(),
            words=StringField()
        )
    )

    return [
        pt.when.example_for_source(scenario.sources['main_source'], ex_main),
        pt.when.example_for_source(scenario.sources['lookup'], ex_lookup)
    ]

with generate_scenario(
    name='PdLookupJoinPipe Basics',
    pipe=pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey']
    )
) as scenario:

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
                k=scenario.factories['keys']['id'],
                lk=scenario.factories['keys']['id']
            )
        )

        case.when(
            *background(scenario)
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )

    with scenario.case('it redirects errors') as case:
        expected = pemi.data.Table(
            '''
            | key    | words |
            | -      | -     |
            | {k[7]} | words |
            '''.format(
                k=scenario.factories['keys']['id']
            )
        )

        case.when(
            *background(scenario)
        ).then(
            pt.then.target_matches_example(scenario.targets['errors'], expected)
        )


with generate_scenario(
    name='PdLookupJoinPipe on_missing=ignore',
    pipe=pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        on_missing='ignore'
    )
) as scenario:

    with scenario.case('it does not redirect errors') as case:
        case.when(
            *background(scenario)
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
                k=scenario.factories['keys']['id']
            )
        )

        case.when(
            *background(scenario),
            pt.when.example_for_source(scenario.sources['lookup'], ex_lookup)
        ).then(
            pt.then.target_is_empty(scenario.targets['errors'])
        )

    with scenario.case('it works when the lookup is REALLY empty') as case:
        ex_main = pemi.data.Table(
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
                k=scenario.factories['keys']['id']
            ),
            schema=pemi.Schema(
                key=StringField(),
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
                k=scenario.factories['keys']['id']
            )
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main_source'], ex_main),
        ).then(
            pt.then.target_is_empty(scenario.targets['errors'])
        )

with generate_scenario(
    name='PdLookupJoinPipe FillNa',
    pipe=pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        on_missing='ignore',
        fillna={'value': 'EMPTY'}
    )
) as scenario:

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
                k=scenario.factories['keys']['id'],
                lk=scenario.factories['keys']['id']
            )
        )

        case.when(
            *background(scenario)
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )



with generate_scenario(
    name='PdLookupJoinPipe Lookup Prefix',
    pipe=pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        lookup_prefix='existing_'
    )
) as scenario:

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
                k=scenario.factories['keys']['id'],
                lk=scenario.factories['keys']['id']
            )
        )

        case.when(
            *background(scenario)
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )


with generate_scenario(
    name='PdLookupJoinPipe Prefix Missing',
    pipe=pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        lookup_prefix='existing_',
        on_missing='ignore'
    )
) as scenario:

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
                k=scenario.factories['keys']['id']
            ),
            schema=pemi.Schema(
                values=StringField(),
                words=StringField()
            )
        )

        case.when(
            *background(scenario),
            pt.when.example_for_source(scenario.sources['lookup'], lookup)
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )



with generate_scenario(
    name='PdLookupJoinPipe Indicator',
    pipe=pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['lkey'],
        on_missing='ignore',
        indicator='lkp_found'
    )
) as scenario:

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
                k=scenario.factories['keys']['id']
            )
        )

        case.when(
            *background(scenario)
        ).then(
            pt.then.target_matches_example(scenario.targets['main_target'], expected)
        )



with pt.Scenario(
    name='PdLookupJoinPipe Blank Keys',
    pipe=pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key'],
        lookup_key=['key']
    ),
    factories={
        'keys': KeyFactory
    },
    sources={
        'main_source': lambda pipe: pipe.sources['main'],
        'lookup': lambda pipe: pipe.sources['lookup']
    },
    targets={
        'main_target': lambda pipe: pipe.targets['main'],
        'errors': lambda pipe: pipe.targets['errors']
    },
    target_case_collectors={
        'main_target': pt.CaseCollector(subject_field='alt_key', factory='keys',
                                        factory_field='id'),
        'errors': pt.CaseCollector(subject_field='alt_key', factory='keys',
                                   factory_field='id'),
    }
) as scenario:

    with scenario.case('it does not use blanks as keys') as case:
        ex_main = pemi.data.Table(
            '''
            | key | alt_key |
            | -   | -       |
            | one | {k[1]}  |
            |     | {k[2]}  |
            | two | {k[3]}  |
            '''.format(
                k=scenario.factories['keys']['id']
            ),
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
            '''.format(
                k=scenario.factories['keys']['id']
            ),
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
            '''.format(
                k=scenario.factories['keys']['id']
            ),
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





def test_pd_lookup_join_pipe_multiple_key_empty_lookup(): #pylint: disable=invalid-name
    # This covers a rather bizarre edge case that used to happen with empty lookup dataframes

    pipe = pemi.pipes.pd.PdLookupJoinPipe(
        main_key=['key1', 'key2'],
        lookup_key=['lkey1', 'lkey2'],
        on_missing='ignore'
    )

    pipe.sources['main'].df = pd.DataFrame({
        'key1': ['k11', 'k12'],
        'key2': ['k21', 'k22']
    })

    pipe.sources['lookup'].df = pemi.PdDataSubject(schema=pemi.Schema(
        lkey1=StringField(),
        lkey2=StringField()
    )).df

    pipe.flow()

    expected_df = pd.DataFrame({
        'key1': ['k11', 'k12'],
        'key2': ['k21', 'k22'],
        'lkey1': np.nan,
        'lkey2': np.nan
    })

    pt.assert_frame_equal(pipe.targets['main'].df, expected_df, check_dtype=False)





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
            'f2': [np.nan, np.nan, np.nan, 1, 2],
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

class TestPdLambdaPipe: #pylint: disable=too-few-public-methods
    def test_it_runs_a_simple_function(self):
        pipe = pemi.pipes.pd.PdLambdaPipe(
            lambda df: df.rename(columns={'oldname': 'newname'})
        )

        pipe.sources['main'].df = pd.DataFrame({
            'something': [1, 2, 3],
            'oldname': [1, 2, 3],
        })

        pipe.flow()

        expected_df = pd.DataFrame({
            'something': [1, 2, 3],
            'newname': [1, 2, 3],
        })
        pt.assert_frame_equal(pipe.targets['main'].df, expected_df)
