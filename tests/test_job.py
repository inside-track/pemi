import re
from collections import OrderedDict

import pytest

import pandas as pd
from pandas.util.testing import assert_frame_equal
from pandas.util.testing import assert_series_equal

import pemi
import pemi.testing as pt
from pemi.data_subject import PdDataSubject
from pemi.fields import *

import sys
this = sys.modules[__name__]

this.schemas = {
    'beers': pemi.Schema(
        id             = IntegerField(),
        name           = StringField(),
        abv            = DecimalField(precision=3, scale=1),
        last_brewed_at = DateField()
    ),
    'beers_w_style': pemi.Schema(
        id    = IntegerField(),
        name  = StringField(),
        abv   = DecimalField(precision=3, scale=1),
        style = StringField()
    )
}

class RemoteSourcePipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.target(
            PdDataSubject,
            name='main',
            schema=this.schemas['beers']
        )

    def flow(self):
        pemi.log().debug('FLOWING {}'.format(self))
        self.targets['main'].df = pd.DataFrame({'id': [1,2,3], 'name': ['one', 'two', 'three']})
        raise NotImplementedError('The test needs to mock out external calls')


class RemoteTargetPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.source(
            PdDataSubject,
            name='main',
            schema=this.schemas['beers_w_style']
        )

    def flow(self):
        pemi.log().debug('FLOWING {}'.format(self))
        raise NotImplementedError('The test needs to mock out external calls')


class BlackBoxPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.source(
            PdDataSubject,
            name='beers_file',
            schema=this.schemas['beers']
        )

        self.target(
            PdDataSubject,
            name='beers_w_style_file',
            schema=this.schemas['beers_w_style']
        )

        self.target(
            PdDataSubject,
            name='dropped_duplicates',
            schema=this.schemas['beers']
        )


        self.re_map = OrderedDict()
        self.re_map['IPA'] = re.compile(r'(IPA|India Pale)')
        self.re_map['Pale'] = re.compile(r'Pale')
        self.re_map['Kolsch'] = re.compile(r'Kolsch')


    def deduce_style(self, name):
        for style, re_name in self.re_map.items():
            if re.search(re_name, name) != None:
                return style
        return 'Unknown Style'

    def flow(self):
        source_df = self.sources['beers_file'].df
        # print(source_df)


        grouped = source_df.groupby(['id'], as_index=False)
        deduped_df = grouped.first()
        dupes_df = grouped.apply(lambda group: group.iloc[1:])


        deduped_df['style'] = deduped_df['name'].apply(self.deduce_style)

        target_fields = list(self.targets['beers_w_style_file'].schema.keys())
        self.targets['beers_w_style_file'].df = deduped_df[target_fields]
        self.targets['dropped_duplicates'].df = dupes_df

        # print(self.targets['beers_w_style_file'].df)
        # print(self.targets['dropped_duplicates'].df)

class BlackBoxJob(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.pipe(
            name='beers_file',
            pipe=RemoteSourcePipe()
        )

        self.pipe(
            name='beers_w_style_file',
            pipe=RemoteTargetPipe()
        )

        self.pipe(
            name='black_box',
            pipe=BlackBoxPipe()
        )


        self.connect('beers_file', 'main').to('black_box', 'beers_file')
        self.connect('black_box', 'beers_w_style_file').to('beers_w_style_file', 'main')

    def flow(self):
        self.connections.flow()


class TestBlackBoxJob():
    pipe = BlackBoxJob()

    pipe.pipes['beers_file'] = pt.mock_pipe(pipe.pipes['beers_file'])
    pipe.pipes['beers_w_style_file'] = pt.mock_pipe(pipe.pipes['beers_w_style_file'])

    def case_keys():
        ids = list(range(1000))
        for i in ids:
            yield {
                'beers_file': {'id': i},
                'beers_w_style_file': {'id': i},
                'dropped_duplicates': {'id': i}
            }


    scenario = pt.Scenario(
        runner=pipe.flow,
        case_keys=case_keys(),
        sources={
            'beers_file': pipe.pipes['beers_file'].targets['main']
        },
        targets={
            'beers_w_style_file': pipe.pipes['beers_w_style_file'].sources['main'],
            'dropped_duplicates': pipe.pipes['black_box'].targets['dropped_duplicates']
        }
    )

    def background(scenario):
        return [
            pt.when.source_conforms_to_schema(scenario.sources['beers_file']),
            pt.when.source_has_keys(scenario.sources, scenario.case_keys, 'beers_file')
        ]

    with scenario.case('it copies the name field') as case:
        case.when(
            *background(scenario)
        ).then(
            pt.then.field_is_copied(scenario.sources['beers_file'], 'name',
                                    scenario.targets['beers_w_style_file'], 'name',
                                    by=['id'])
        )

    with scenario.case('fields that are directly copied to the target') as case:
        case.when(
            *background(scenario)
        ).then(
            pt.then.fields_are_copied(scenario.sources['beers_file'],
                                      scenario.targets['beers_w_style_file'],
                                      by=['id'],
                                      mapping=[
                                          ('id', 'id'),
                                          ('name', 'name'),
                                          ('abv', 'abv')
                                      ]
            )
        )

    with scenario.case('it uses a default style') as case:
        case.when(
            *background(scenario),
            pt.when.source_field_has_value(scenario.sources['beers_file'], 'name', 'Deduce This!')
        ).then(
            pt.then.target_field_has_value(scenario.targets['beers_w_style_file'], 'style', 'Unknown Style')
        )


    with scenario.case('it deduced style from name') as case:
        example_beers = pemi.data.Table(
            '''
            | id     | name                     |
            | -      | -                        |
            | {b[1]} | Fireside IPA             |
            | {b[2]} | Perfunctory Pale Ale     |
            | {b[3]} | Ginormous India Pale Ale |
            '''.format(b = scenario.case_keys.cache('beers_file', 'id')),
            schema=pipe.pipes['beers_file'].targets['main'].schema
        )

        expected_styles = pemi.data.Table(
            '''
            | id     | name                     | style |
            | -      | -                        | -     |
            | {b[1]} | Fireside IPA             | IPA   |
            | {b[2]} | Perfunctory Pale Ale     | Pale  |
            | {b[3]} | Ginormous India Pale Ale | IPA   |
            '''.format(b = scenario.case_keys.cache('beers_w_style_file', 'id')),
            schema=pipe.pipes['beers_w_style_file'].sources['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['beers_file'], example_beers)
        ).then(
            pt.then.target_matches_example(scenario.targets['beers_w_style_file'], expected_styles)
        )

    with scenario.case('it drops and redirects duplicates') as case:
        example_duplicates = pemi.data.Table(
            '''
            | id     | name                     |
            | -      | -                        |
            | {b[1]} | Fireside IPA             |
            | {b[2]} | Perfunctory Pale Ale     |
            | {b[2]} | Excellent ESB            |
            | {b[4]} | Ginormous India Pale Ale |
            '''.format(b = scenario.case_keys.cache('beers_file', 'id')),
            schema=pipe.pipes['beers_file'].targets['main'].schema
        )

        expected_styles = pemi.data.Table(
            '''
            | id     | name                     |
            | -      | -                        |
            | {b[1]} | Fireside IPA             |
            | {b[2]} | Perfunctory Pale Ale     |
            | {b[4]} | Ginormous India Pale Ale |
            '''.format(b = scenario.case_keys.cache('beers_w_style_file', 'id')),
            schema=pipe.pipes['beers_w_style_file'].sources['main'].schema
        )

        dropped_duplicates = pemi.data.Table(
            '''
            | id     | name                     |
            | -      | -                        |
            | {b[2]} | Excellent ESB            |
            '''.format(b = scenario.case_keys.cache('dropped_duplicates', 'id')),
            schema=pipe.pipes['black_box'].targets['dropped_duplicates'].schema
        )


        case.when(
            pt.when.example_for_source(scenario.sources['beers_file'], example_duplicates)
        ).then(
            pt.then.target_matches_example(scenario.targets['beers_w_style_file'], expected_styles),
            pt.then.target_matches_example(scenario.targets['dropped_duplicates'], dropped_duplicates)
        )

    @pytest.mark.scenario(scenario)
    def test_scenario(self, case):
        case.assert_case()


# We can also test just the core pipe that does the interesting stuff in isolation from
# all of the sub pipes.  This may be simpler to test in most cases.
class TestBlackBoxPipe():
    pipe = BlackBoxPipe()

    def case_keys():
        ids = list(range(1000))
        for i in ids:
            yield {
                'beers_file': {'id': i},
                'beers_w_style_file': {'id': i}
            }

    scenario = pt.Scenario(
        runner=pipe.flow,
        case_keys=case_keys(),
        sources={
            'beers_file': pipe.sources['beers_file']
        },
        targets={
            'beers_w_style_file': pipe.targets['beers_w_style_file']
        }
    )

    def background(scenario):
        return [
            pt.when.source_conforms_to_schema(scenario.sources['beers_file']),
            pt.when.source_has_keys(scenario.sources, scenario.case_keys, 'beers_file')
        ]

    with scenario.case('it copies the name field') as case:
        case.when(
            *background(scenario)
        ).then(
            pt.then.field_is_copied(scenario.sources['beers_file'], 'name',
                                    scenario.targets['beers_w_style_file'], 'name',
                                    by=['id'])
        )

    with scenario.case('fields that are directly copied to the target') as case:
        case.when(
            *background(scenario)
        ).then(
            pt.then.fields_are_copied(scenario.sources['beers_file'],
                                      scenario.targets['beers_w_style_file'],
                                      by=['id'],
                                      mapping=[
                                          ('id', 'id'),
                                          ('name', 'name'),
                                          ('abv', 'abv')
                                      ]
            )
        )

    with scenario.case('it uses a default style') as case:
        case.when(
            *background(scenario),
            pt.when.source_field_has_value(scenario.sources['beers_file'], 'name', 'Deduce This!')
        ).then(
            pt.then.target_field_has_value(scenario.targets['beers_w_style_file'], 'style', 'Unknown Style')
        )

    @pytest.mark.scenario(scenario)
    def test_scenario(self, case):
        case.assert_case()
