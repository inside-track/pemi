import random

import pytest
import pandas as pd

import pemi
import pemi.data
import pemi.testing as pt
from pemi.fields import *


with pt.Scenario('Testing Basics') as scenario:
    mysource = pemi.PdDataSubject(
        schema=pemi.Schema(
            id=IntegerField(),
            first_name=StringField(),
            last_name=StringField()
        )
    )

    mytarget = pemi.PdDataSubject(
        schema=pemi.Schema(
            tid=StringField(),
            first_name=StringField(),
            last_name=StringField(),
            full_name=StringField()
        )
    )

    # The only rule with this generator is that each set of keys must be unique
    # All variables that could be present in a key for a source must be present on every call
    class BasicScenario:
        @staticmethod
        def case_keys():
            ids = list(range(1000))
            random.shuffle(ids)
            for i in ids:
                yield {
                    'mysource': {'id': i},
                    'mytarget': {'tid': 'T{}'.format(i)}
                }

        @staticmethod
        def runner(source, target):
            def _runner():
                target.df = pd.DataFrame(columns=list(target.schema.keys()))

                if len(source.df) > 0:
                    target.df['tid'] = source.df['id'].apply('T{}'.format)
                    target.df['first_name'] = source.df['first_name']
                    target.df['last_name'] = source.df['last_name']
                    target.df['full_name'] = source.df.apply(
                        lambda row: '{} {}'.format(row['first_name'], row['last_name']), axis=1
                    )
            return _runner

        @staticmethod
        def background(scenario):
            return [
                pt.when.source_has_keys(scenario.sources['mysource'], scenario.case_keys),
                pt.when.source_field_has_value(scenario.sources['mysource'],
                                               'last_name', 'Background')
            ]


    scenario.setup(
        runner=BasicScenario.runner(mysource, mytarget),
        case_keys=BasicScenario.case_keys(),
        sources={
            'mysource': mysource
        },
        targets={
            'mytarget': mytarget
        }
    )


    with scenario.case('Using when.source_field_has_value') as case:
        case.when(
            pt.when.source_field_has_value(scenario.sources['mysource'], 'id',
                                           scenario.case_keys.cache('mysource', 'id')[1]),
            pt.when.source_field_has_value(scenario.sources['mysource'], 'first_name', 'Joe'),
            pt.when.source_field_has_value(scenario.sources['mysource'], 'last_name', 'Jones')
        ).then(
            pt.then.target_field_has_value(scenario.targets['mytarget'], 'full_name', 'Joe Jones')
        )

    with scenario.case('Using when.source_fields_have_values') as case:
        case.when(
            pt.when.source_fields_have_values(scenario.sources['mysource'], mapping={
                'id': scenario.case_keys.cache('mysource', 'id')[1],
                'first_name': 'Joe',
                'last_name': 'Jones'
            })
        ).then(
            pt.then.target_field_has_value(scenario.targets['mytarget'], 'full_name', 'Joe Jones')
        )

    with scenario.case('Using then.target_fields_have_values') as case:
        case.when(
            pt.when.source_fields_have_values(scenario.sources['mysource'], mapping={
                'id': scenario.case_keys.cache('mysource', 'id')[1],
                'first_name': 'Joe',
                'last_name': 'Jones'
            })
        ).then(
            pt.then.target_fields_have_values(scenario.targets['mytarget'], {
                'first_name': 'Joe',
                'last_name': 'Jones'
            })
        )

    with scenario.case('Using when.source_has_keys') as case:
        case.when(
            pt.when.source_has_keys(scenario.sources['mysource'], scenario.case_keys),
            pt.when.source_field_has_value(scenario.sources['mysource'], 'first_name', 'Bob'),
            pt.when.source_field_has_value(scenario.sources['mysource'], 'last_name', 'Boberts')
        ).then(
            pt.then.target_field_has_value(scenario.targets['mytarget'], 'full_name', 'Bob Boberts')
        )


    with scenario.case('Using shared background 1') as case:
        case.when(
            *BasicScenario.background(scenario),
            pt.when.source_field_has_value(scenario.sources['mysource'],
                                           'first_name', 'Bob'),

        ).then(
            pt.then.target_field_has_value(scenario.targets['mytarget'],
                                           'full_name', 'Bob Background')
        )

    with scenario.case('Using shared background 2') as case:
        case.when(
            *BasicScenario.background(scenario),
            pt.when.source_field_has_value(scenario.sources['mysource'],
                                           'first_name', 'Joe'),
        ).then(
            pt.then.target_field_has_value(scenario.targets['mytarget'],
                                           'full_name', 'Joe Background')
        )



    with scenario.case('Using examples') as case:
        source_table = pemi.data.Table(
            '''
            | id        | first_name | last_name |
            | -         | -          | -         |
            | {sid[1]}  | Glerbo     | McDuck    |
            | {sid[2]}  | Glerbo     | McDuck2   |
            '''.format(sid=scenario.case_keys.cache('mysource', 'id')),
            schema=mysource.schema
        )

        target_table = pemi.data.Table(
            '''
            | tid       | full_name      |
            | -         | -              |
            | {tid[1]}  | Glerbo McDuck  |
            | {tid[2]}  | Glerbo McDuck2 |
            '''.format(tid=scenario.case_keys.cache('mytarget', 'tid')),
            schema=mysource.schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['mysource'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['mytarget'], target_table)
        )

    with scenario.case('Using unsorted examples') as case:
        source_table = pemi.data.Table(
            '''
            | id        | first_name | last_name |
            | -         | -          | -         |
            | {sid[1]}  | Glerbo     | McDuck    |
            | {sid[2]}  | Glerbo     | McDuck2   |
            '''.format(sid=scenario.case_keys.cache('mysource', 'id')),
            schema=mysource.schema
        )

        target_table = pemi.data.Table(
            '''
            | tid       | full_name      |
            | -         | -              |
            | {tid[2]}  | Glerbo McDuck2 |
            | {tid[1]}  | Glerbo McDuck  |
            '''.format(tid=scenario.case_keys.cache('mytarget', 'tid')),
            schema=mysource.schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['mysource'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['mytarget'], target_table, by=['tid'])
        )

    with scenario.case('Using then.field_is_copied') as case:
        case.when(
            pt.when.source_conforms_to_schema(scenario.sources['mysource']),
            pt.when.source_has_keys(scenario.sources['mysource'], scenario.case_keys),
        ).then(
            pt.then.field_is_copied(scenario.sources['mysource'], 'last_name',
                                    scenario.targets['mytarget'], 'last_name'),
            pt.then.field_is_copied(scenario.sources['mysource'], 'first_name',
                                    scenario.targets['mytarget'], 'first_name')
        )

    with scenario.case('Using then.fields_are_copied') as case:
        case.when(
            pt.when.source_conforms_to_schema(scenario.sources['mysource']),
            pt.when.source_has_keys(scenario.sources['mysource'], scenario.case_keys),
        ).then(
            pt.then.fields_are_copied(scenario.sources['mysource'], scenario.targets['mytarget'],
                                      [
                                          ('last_name', 'last_name'),
                                          ('first_name', 'first_name')
                                      ])
        )

    with scenario.case('Using then.target_does_not_have_fields') as case:
        case.when(
            pt.when.source_has_keys(scenario.sources['mysource'], scenario.case_keys),
        ).then(
            pt.then.target_does_not_have_fields(scenario.targets['mytarget'],
                                                'glerbo', 'mcstuffins')
        )

    with scenario.case('Using then.target_is_empty') as case:
        case.then(
            pt.then.target_is_empty(scenario.targets['mytarget'])
        )

    with scenario.case('Using then.target_has_n_records') as case:
        source_table = pemi.data.Table(
            '''
            | id        |
            | -         |
            | {sid[1]}  |
            | {sid[2]}  |
            '''.format(sid=scenario.case_keys.cache('mysource', 'id')),
            schema=mysource.schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['mysource'], source_table),
        ).then(
            pt.then.target_has_n_records(scenario.targets['mytarget'], 2)
        )




with pt.Scenario('Testing with multiple keys') as scenario:
    mysource = pemi.PdDataSubject(
        schema=pemi.Schema(
            student_id=StringField(),
            term_id=StringField()
        )
    )

    mytarget = pemi.PdDataSubject(
        schema=pemi.Schema(
            student_id=StringField(),
            term_id=StringField()
        )
    )

    def case_keys():
        student_ids = pemi.data.UniqueIdGenerator('stu{}'.format)
        term_ids = pemi.data.UniqueIdGenerator('T{}'.format)

        while True:
            student_id = next(student_ids)
            term_id = next(term_ids)
            yield {
                'mysource': {
                    'student_id': student_id,
                    'term_id': term_id
                },
                'mytarget': {
                    'student_id': student_id,
                    'term_id': term_id
                }
            }


    def runner(source, target):
        def _runner():
            target.df = source.df.copy()
        return _runner

    scenario.setup(
        runner=runner(mysource, mytarget),
        case_keys=case_keys(),
        sources={
            'mysource': mysource
        },
        targets={
            'mytarget': mytarget
        }
    )

    with scenario.case('it works when each row has unique keys') as case:
        source_table = pemi.data.Table(
            '''
            | student_id | term_id |
            | -          | -       |
            | {s[1]}     | {t[1]}  |
            | {s[2]}     | {t[2]}  |
            | {s[3]}     | {t[3]}  |
            '''.format(
                s=scenario.case_keys.cache('mysource', 'student_id'),
                t=scenario.case_keys.cache('mysource', 'term_id')
            ),
            schema=mysource.schema
        )

        target_table = pemi.data.Table(
            '''
            | student_id | term_id |
            | -          | -       |
            | {s[1]}     | {t[1]}  |
            | {s[2]}     | {t[2]}  |
            | {s[3]}     | {t[3]}  |
            '''.format(
                s=scenario.case_keys.cache('mytarget', 'student_id'),
                t=scenario.case_keys.cache('mytarget', 'term_id')
            ),
            schema=mytarget.schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['mysource'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['mytarget'], target_table)
        )


    with scenario.case('it works when keys are repeated on different rows') as case:
        source_table = pemi.data.Table(
            '''
            | student_id | term_id |
            | -          | -       |
            | {s[1]}     | {t[1]}  |
            | {s[1]}     | {t[2]}  |
            | {s[3]}     | {t[1]}  |
            '''.format(
                s=scenario.case_keys.cache('mysource', 'student_id'),
                t=scenario.case_keys.cache('mysource', 'term_id')
            ),
            schema=mysource.schema
        )

        target_table = pemi.data.Table(
            '''
            | student_id | term_id |
            | -          | -       |
            | {s[1]}     | {t[1]}  |
            | {s[1]}     | {t[2]}  |
            | {s[3]}     | {t[1]}  |
            '''.format(
                s=scenario.case_keys.cache('mytarget', 'student_id'),
                t=scenario.case_keys.cache('mytarget', 'term_id')
            ),
            schema=mysource.schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['mysource'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['mytarget'], target_table)
        )







class ChildPipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.source(
            pemi.PdDataSubject,
            name='child_source',
            schema=pemi.Schema(
                cs1=StringField(),
                cs2=StringField()
            )
        )

        self.target(
            pemi.PdDataSubject,
            name='child_target',
            schema=pemi.Schema(
                ct1=StringField(),
                ct2=StringField()
            )
        )

    def flow(self):
        pass

class ParentPipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.source(
            pemi.PdDataSubject,
            name='parent_source',
            schema=pemi.Schema(
                ps1=StringField(),
                ps2=StringField()
            )
        )

        self.target(
            pemi.PdDataSubject,
            name='parent_target',
            schema=pemi.Schema(
                pt1=StringField(),
                pt2=StringField()
            )
        )

        self.pipe(
            name='child',
            pipe=ChildPipe()
        )

    def flow(self):
        pass


class GrandPipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.pipe(
            name='parent',
            pipe=ParentPipe()
        )

    def flow(self):
        pass


class TestPipeMock:
    @pytest.fixture
    def mock_pipe(self):
        pipe = GrandPipe()
        pt.mock_pipe(pipe, 'parent')
        return pipe

    @pytest.fixture
    def real_pipe(self):
        pipe = GrandPipe()
        return pipe

    def test_parent_is_mocked(self, mock_pipe):
        assert isinstance(mock_pipe.pipes['parent'], pt.MockPipe)

    def test_parent_source_reassigned(self, real_pipe, mock_pipe):
        assert real_pipe.pipes['parent'].sources['parent_source'].schema \
            == mock_pipe.pipes['parent'].sources['parent_source'].schema

    def test_parent_target_reassigned(self, real_pipe, mock_pipe):
        assert real_pipe.pipes['parent'].targets['parent_target'].schema \
            == mock_pipe.pipes['parent'].targets['parent_target'].schema

    #TODO: not sure if traversing nested pipes is necessary or desired.
    @pytest.mark.skip
    def test_child_is_mocked(self, mock_pipe):
        assert isinstance(mock_pipe.pipes['parent'].pipes['child'], pt.MockPipe)
