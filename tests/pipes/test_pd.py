import unittest

import pandas as pd

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
