import unittest
import re
from collections import OrderedDict

import pandas as pd
from pandas.util.testing import assert_frame_equal
from pandas.util.testing import assert_series_equal

import pemi
import pemi.pipes.dask
from pemi.testing import TestTable

import logging
pemi.log('pemi').setLevel(logging.WARN)


import sys
this = sys.modules[__name__]

this.schemas = {
    'beers': {
        'id': {'type': 'integer', 'required': True},
        'name': {'type': 'string'},
        'abv': {'type': 'decimal', 'precision': 3, 'scale': 1},
        'last_brewed_at': {'type': 'date'}
    },
    'beers_w_style': {
        'id': {'type': 'integer', 'required': True},
        'name': {'type': 'string'},
        'abv': {'type': 'decimal', 'precision': 3, 'scale': 1},
        'style': {'type': 'string'}
    }
}

class RemoteSourcePipe(pemi.Pipe):
    def config(self):
        self.target(
            name='main',
            schema=this.schemas['beers']
        )

    def flow(self):
        pemi.log().debug('FLOWING {}'.format(self))
        self.targets['main'].data = pd.DataFrame({'id': [1,2,3], 'name': ['one', 'two', 'three']})
        raise NotImplementedError('The test needs to mock out external calls')


class RemoteTargetPipe(pemi.Pipe):
    def config(self):
        self.source(
            name='main',
            schema=this.schemas['beers_w_style']
        )

    def flow(self):
        pemi.log().debug('FLOWING {}'.format(self))
        self.data = pd.DataFrame()
        raise NotImplementedError('The test needs to mock out external calls')


class BlackBoxPipe(pemi.Pipe):
    def config(self):
        self.source(
            name='beers_file',
            schema=this.schemas['beers']
        )

        self.target(
            name='beers_w_style_file',
            schema=this.schemas['beers_w_style']
        )

        self.target(
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
        source_df = self.sources['beers_file'].data.copy()

        grouped = source_df.groupby(['id'], as_index=False)
        deduped_df = grouped.first()
        dupes_df = grouped.apply(lambda group: group.iloc[1:])


        deduped_df['style'] = deduped_df['name'].apply(self.deduce_style)

        target_fields = list(self.targets['beers_w_style_file'].schema.keys())
        self.targets['beers_w_style_file'].data = deduped_df[target_fields]
        self.targets['dropped_duplicates'].data = dupes_df

class BlackBoxJob(pemi.Pipe):
    def config(self):
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


        self.connect(
            self.pipes['beers_file'].targets['main']
        ).to(
            self.pipes['black_box'].sources['beers_file']
        )

        self.connect(
            self.pipes['black_box'].targets['beers_w_style_file']
        ).to(
            self.pipes['beers_w_style_file'].sources['main']
        )

        self.dask = pemi.pipes.dask.DaskFlow(self.connections)

    def flow(self):
        self.dask.flow()


class TestBlackBoxJobMappings(unittest.TestCase):
    def setUp(self):
        self.pipe = BlackBoxJob()

        self.mocker = pemi.testing.PipeMocker(self.pipe)
        self.mocker.mock_pipe_subject(self.pipe.pipes['beers_file'].targets['main'])
        self.mocker.mock_pipe_subject(self.pipe.pipes['beers_w_style_file'].sources['main'])

        self.rules = pemi.testing.BasicRules(
            source_subject=self.pipe.pipes['beers_file'].targets['main'],
            target_subject=self.pipe.pipes['beers_w_style_file'].sources['main'],
            mocker=self.mocker
        )

        self.scenario = pemi.testing.Scenario(
            runner=self.pipe.flow,
            givens=[self.rules.when_source_conforms_to_schema()]
        )


    def test_it_copies_the_name_field(self):
        'The name field is directly copied to the target'

        self.scenario.when(
        ).then(
            self.rules.then_field_is_copied('name', 'name', by='id')
        )
        return self.scenario.run()

    def test_direct_copies(self):
        'Fields that are directly copied to the target'

        self.scenario.when(
            self.rules.when_source_conforms_to_schema()
        ).then(
            *self.rules.then_fields_are_copied({
                'id': 'id',
                'name': 'name',
                'abv': 'abv'
            }, by='id')
        )
        return self.scenario.run()

    def test_it_uses_a_default_style(self):
        'The style is set to unknown if can not be deduced'

        self.scenario.when(
            self.rules.when_source_field_has_value('name', 'Deduce This!')
        ).then(
            self.rules.then_target_field_has_value('style', 'Unknown Style')
        )
        return self.scenario.run()



class TestBlackBoxJobExamples(unittest.TestCase):

    def setUp(self):
        self.pipe = BlackBoxJob()

        self.mocker = pemi.testing.PipeMocker(self.pipe)
        self.mocker.mock_pipe_subject(self.pipe.pipes['beers_file'].targets['main'])
        self.mocker.mock_pipe_subject(self.pipe.pipes['beers_w_style_file'].sources['main'])

        self.source_subject = self.pipe.pipes['beers_file'].targets['main']
        self.target_subject = self.pipe.pipes['beers_w_style_file'].sources['main']

        self.rules = pemi.testing.BasicRules(
            source_subject=self.pipe.pipes['beers_file'].targets['main'],
            target_subject=self.pipe.pipes['beers_w_style_file'].sources['main'],
            mocker=self.mocker
        )

        self.scenario = pemi.testing.Scenario(
            runner=self.pipe.flow,
            givens=[self.given_example_beers()]
        )


    def example_beers(self):
        return TestTable(
            '''
            | id | name                     |
            | -  | -                        |
            | 1  | Fireside IPA             |
            | 2  | Perfunctory Pale Ale     |
            | 3  | Ginormous India Pale Ale |
            ''',
            schema=self.source_subject.schema
        )


    def given_example_beers(self):
        'Some example beer names'
        return self.rules.when_example_for_source(self.example_beers())


    def test_it_deduces_style_from_name(self):
        'The style is deduced from the name'

        expected_styles = TestTable(
            '''
            | id | name                     | style |
            | -  | -                        | -     |
            | 1  | Fireside IPA             | IPA   |
            | 2  | Perfunctory Pale Ale     | Pale  |
            | 3  | Ginormous India Pale Ale | IPA   |
            ''',
            schema=self.target_subject.schema
        )

        self.scenario.when(
        ).then(
            self.rules.then_target_matches_example(expected_styles)
        )
        return self.scenario.run()



class TestBlackBoxJobDuplicates(unittest.TestCase):

    def setUp(self):
        self.pipe = BlackBoxJob()

        self.mocker = pemi.testing.PipeMocker(self.pipe)
        self.mocker.mock_pipe_subject(self.pipe.pipes['beers_file'].targets['main'])
        self.mocker.mock_pipe_subject(self.pipe.pipes['beers_w_style_file'].sources['main'])

        self.source_subject = self.pipe.pipes['beers_file'].targets['main']
        self.target_subject = self.pipe.pipes['beers_w_style_file'].sources['main']

        self.rules = pemi.testing.BasicRules(
            source_subject=self.pipe.pipes['beers_file'].targets['main'],
            target_subject=self.pipe.pipes['beers_w_style_file'].sources['main'],
            mocker=self.mocker
        )

        self.dupe_rules = pemi.testing.BasicRules(
            source_subject=self.pipe.pipes['beers_file'].targets['main'],
            target_subject=self.pipe.pipes['black_box'].targets['dropped_duplicates'],
            mocker=self.mocker
        )

        self.scenario = pemi.testing.Scenario(
            runner=self.pipe.flow,
            givens=[self.given_example_duplicates()]
        )

    def example_duplicates(self):
        return TestTable(
            '''
            | id | name                     |
            | -  | -                        |
            | 1  | Fireside IPA             |
            | 2  | Perfunctory Pale Ale     |
            | 2  | Excellent ESB            |
            | 4  | Ginormous India Pale Ale |
            ''',
            schema=self.source_subject.schema
        )


    def given_example_duplicates(self):
        'Some example beers with duplicates'
        return self.rules.when_example_for_source(self.example_duplicates())


    def test_it_drops_duplicates(self):
        duplicates_dropped = TestTable(
            '''
            | id | name                     |
            | -  | -                        |
            | 1  | Fireside IPA             |
            | 2  | Perfunctory Pale Ale     |
            | 4  | Ginormous India Pale Ale |
            ''',
            schema=self.target_subject.schema
        )

        self.scenario.when(
        ).then(
            self.rules.then_target_matches_example(duplicates_dropped)
        )
        return self.scenario.run()

    def test_it_redirects_duplicates(self):
        duplicates = TestTable(
            '''
            | id | name                     |
            | -  | -                        |
            | 2  | Excellent ESB            |
            ''',
            schema=self.source_subject.schema
        )

        self.scenario.when(
        ).then(
            self.dupe_rules.then_target_matches_example(duplicates)
        )
        return self.scenario.run()



# We can also test just the core pipe that does the interesting stuff in isolation from
# all of the sub pipes.  This may be simpler to test in most cases.
class TestBlackBoxPipe(unittest.TestCase):
    def setUp(self):
        self.pipe = BlackBoxPipe()

        self.mocker = pemi.testing.PipeMocker(self.pipe)

        self.rules = pemi.testing.BasicRules(
            source_subject=self.pipe.sources['beers_file'],
            target_subject=self.pipe.targets['beers_w_style_file'],
            mocker=self.mocker
        )

        self.scenario = pemi.testing.Scenario(
            runner=self.pipe.flow,
            givens=[self.rules.when_source_conforms_to_schema()]
        )

    def test_it_copies_the_name_field(self):
        'The name field is directly copied to the target'

        self.scenario.when(
        ).then(
            self.rules.then_field_is_copied('name', 'name', by='id')
        )
        return self.scenario.run()

    def test_direct_copies(self):
        'Fields that are directly copied to the target'

        self.scenario.when(
            self.rules.when_source_conforms_to_schema()
        ).then(
            *self.rules.then_fields_are_copied({
                'id': 'id',
                'name': 'name',
                'abv': 'abv'
            }, by='id')
        )
        return self.scenario.run()

    def test_it_uses_a_default_style(self):
        'The style is set to unknown if can not be deduced'

        self.scenario.when(
            self.rules.when_source_field_has_value('name', 'Deduce This!')
        ).then(
            self.rules.then_target_field_has_value('style', 'Unknown Style')
        )
        return self.scenario.run()


if __name__ == '__main__':
    pass
