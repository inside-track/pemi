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
pemi.log('pemi').setLevel(logging.DEBUG)


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
        df = self.sources['beers_file'].data.copy()
        df['style'] = df['name'].apply(self.deduce_style)

        target_fields = list(self.targets['beers_w_style_file'].schema.keys())
        self.targets['beers_w_style_file'].data = df[target_fields]


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


# Can I test all of these conditions?
# # Deduplicates based on id
# # Redirects duplicate records


# TODO: SPlit this up into two classes
#       One with given = conforms to schema
#       Another with specific example data


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
            self.rules.then_field_is_copied('name', 'name')
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
            })
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

        #TODO: How can I have the rules autodetect sources and targets?
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



class TestBlackBoxPipe(unittest.TestCase):
    def setUp(self):
        self.pipe = BlackBoxPipe()
        self.scenario = pemi.testing.Scenario(
            runner=self.pipe.flow
        )

        self.mocker = pemi.testing.PipeMocker(self.pipe)
        self.job = BlackBoxPipe()


    def when_conforms_to_schema(self):
        'The source file conforms to the schema'

        data = TestTable(
            schema=self.pipe.sources['beers_file'].schema
        ).df

        self.mocker.mock_subject_data(self.pipe.sources['beers_file'], data)

    def then_name_field_copied(self):
        'The name field is copied to the target'

        given = self.pipe.sources['beers_file'].data
        actual = self.pipe.targets['beers_w_style_file'].data
        pd.testing.assert_series_equal(given['name'], actual['name'])

    def test_it_copies_the_name_field(self):
        'The name field is directly copied to the target'

        self.scenario.when(
            self.when_conforms_to_schema
        ).then(
            self.then_name_field_copied
        )
        return self.scenario.run()

    def when_name_examples(self):
        'Some example beer names'

        data = TestTable(
            '''
            | id | name                     |
            | -  | -                        |
            | 1  | Fireside IPA             |
            | 2  | Perfunctory Pale Ale     |
            | 3  | Ginormous India Pale Ale |
            ''',
            schema=self.pipe.sources['beers_file'].schema
        ).df

        self.mocker.mock_subject_data(self.pipe.sources['beers_file'], data)

    def then_name_examples_to_style(self):
        'Example beer names matches to style name'

        expected = TestTable(
            '''
            | id | name                     | style |
            | -  | -                        | -     |
            | 1  | Fireside IPA             | IPA   |
            | 2  | Perfunctory Pale Ale     | Pale  |
            | 3  | Ginormous India Pale Ale | IPA   |
            ''',
            schema=self.pipe.targets['beers_w_style_file'].schema
        ).df

        actual = self.pipe.targets['beers_w_style_file'].data

        subject_fields = ['id', 'name', 'style']
        assert_frame_equal(actual[subject_fields], expected[subject_fields])

    def test_it_deduces_style_from_name(self):
        'The style is deduced from the name'

        self.scenario.when(
            self.when_name_examples
        ).then(
            self.then_name_examples_to_style
        )
        return self.scenario.run()

if __name__ == '__main__':
    t = TestBlackBoxJob()
    t.setUp()
    print(dir(t))
