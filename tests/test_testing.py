import datetime

import pytest
import factory
import pandas as pd

import pemi
import pemi.data
import pemi.testing as pt
from pemi.fields import *


# Use FactoryBoy for generating keys use to collected records that belong to a case
class StudentKeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence('stu{}'.format)
    tid = factory.LazyAttribute(lambda obj: 'T{}'.format(obj.id))

class TermKeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence('Term{}'.format)



class BasicPipe(pemi.Pipe):
    def __init__(self):
        super().__init__()

        self.source(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                id=StringField(),
                first_name=StringField(),
                last_name=StringField(faker=lambda: 'Faked'),
                birthdate=DateField(format='%m/%d/%Y')
            )
        )

        self.target(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                tid=StringField(),
                first_name=StringField(),
                last_name=StringField(),
                full_name=StringField(),
                birthdate=DateField()
            )
        )

    def flow(self):
        source_df = self.sources['main'].df
        target_df = pd.DataFrame(columns=list(self.targets['main'].schema.keys()))

        if len(source_df) > 0:
            target_df['tid'] = source_df['id'].apply('T{}'.format)
            target_df['first_name'] = source_df['first_name']
            target_df['last_name'] = source_df['last_name']
            target_df['full_name'] = source_df.apply(
                lambda row: '{} {}'.format(row['first_name'], row['last_name']), axis=1
            )
            target_df['birthdate'] = source_df['birthdate']

        self.targets['main'].df = target_df



with pt.Scenario(
    name='Testing Basics',
    pipe=BasicPipe(),
    factories={
        'student': StudentKeyFactory
    },
    sources={
        # Use a lambda to reference the pipe that is configured within the scenario (above)
        'main': lambda pipe: pipe.sources['main']
    },
    targets={
        'main': lambda pipe: pipe.targets['main']
    },
    target_case_collectors={
        'main': pt.CaseCollector(subject_field='tid', factory='student', factory_field='tid')
    }
) as scenario:

    with scenario.case('Using when.source_field_has_value') as case:
        case.when(
            pt.when.source_field_has_value(scenario.sources['main'], 'id',
                                           scenario.factories['student']['id'][1]),
            pt.when.source_field_has_value(scenario.sources['main'], 'first_name', 'Joe'),
            pt.when.source_field_has_value(scenario.sources['main'], 'last_name', 'Jones')
        ).then(
            pt.then.target_field_has_value(scenario.targets['main'], 'full_name', 'Joe Jones')
        )

    with scenario.case('when.source_field_has_value coerces values') as case:
        case.when(
            pt.when.source_field_has_value(scenario.sources['main'], 'id',
                                           scenario.factories['student']['id'][1]),
            pt.when.source_field_has_value(scenario.sources['main'], 'birthdate', '01/09/2014')
        ).then(
            pt.then.target_field_has_value(scenario.targets['main'], 'birthdate',
                                           datetime.datetime(2014, 1, 9).date())
        )

    with scenario.case('Using when.source_fields_have_values') as case:
        case.when(
            pt.when.source_fields_have_values(scenario.sources['main'], mapping={
                'id': scenario.factories['student']['id'][1],
                'first_name': 'Joe',
                'last_name': 'Jones'
            })
        ).then(
            pt.then.target_field_has_value(scenario.targets['main'], 'full_name', 'Joe Jones')
        )

    with scenario.case('when.source_fields_have_values coerces values') as case:
        case.when(
            pt.when.source_fields_have_values(scenario.sources['main'], mapping={
                'id': scenario.factories['student']['id'][1],
                'first_name': 'Joe',
                'last_name': 'Jones',
                'birthdate': '01/09/2014'
            })
        ).then(
            pt.then.target_field_has_value(scenario.targets['main'], 'birthdate',
                                           datetime.datetime(2014, 1, 9).date())
        )

    with scenario.case('Using then.target_fields_have_values') as case:
        case.when(
            pt.when.source_fields_have_values(scenario.sources['main'], mapping={
                'id': scenario.factories['student']['id'][1],
                'first_name': 'Joe',
                'last_name': 'Jones'
            })
        ).then(
            pt.then.target_fields_have_values(scenario.targets['main'], {
                'first_name': 'Joe',
                'last_name': 'Jones'
            })
        )


    with scenario.case('Using when.source_conforms_to_schema') as case:
        case.when(
            pt.when.source_conforms_to_schema(
                scenario.sources['main'],
                {'id': scenario.factories['student']['id']}
            ),
        ).then(
            pt.then.target_field_has_value(scenario.targets['main'], 'last_name', 'Faked')
        )


    # Background has to be a lambda or method so it is evaluated within the context of a case
    background = lambda: [
        pt.when.source_conforms_to_schema(
            scenario.sources['main'],
            {'id': scenario.factories['student']['id']}
        ),
        pt.when.source_field_has_value(scenario.sources['main'], 'last_name', 'Background')
    ]


    with scenario.case('Using shared background 1') as case:
        case.when(
            *background(),
            pt.when.source_field_has_value(scenario.sources['main'], 'first_name', 'Bob')
        ).then(
            pt.then.target_field_has_value(scenario.targets['main'], 'full_name', 'Bob Background')
        )

    with scenario.case('Using shared background 2') as case:
        case.when(
            *background(),
            pt.when.source_field_has_value(scenario.sources['main'], 'first_name', 'Joe'),
        ).then(
            pt.then.target_field_has_value(scenario.targets['main'], 'full_name', 'Joe Background')
        )



    with scenario.case('Using examples') as case:
        source_table = pemi.data.Table(
            '''
            | id        | first_name | last_name |
            | -         | -          | -         |
            | {sid[1]}  | Glerbo     | McDuck    |
            | {sid[2]}  | Glerbo     | McDuck2   |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        target_table = pemi.data.Table(
            '''
            | tid       | full_name      |
            | -         | -              |
            | {tid[1]}  | Glerbo McDuck  |
            | {tid[2]}  | Glerbo McDuck2 |
            '''.format(
                tid=scenario.factories['student']['tid']
            ),
            schema=scenario.targets['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['main'], target_table)
        )

    with scenario.case('Using unsorted examples') as case:
        source_table = pemi.data.Table(
            '''
            | id        | first_name | last_name |
            | -         | -          | -         |
            | {sid[1]}  | Glerbo     | McDuck    |
            | {sid[2]}  | Glerbo     | McDuck2   |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        target_table = pemi.data.Table(
            '''
            | tid       | full_name      |
            | -         | -              |
            | {tid[2]}  | Glerbo McDuck2 |
            | {tid[1]}  | Glerbo McDuck  |
            '''.format(
                tid=scenario.factories['student']['tid']
            ),
            schema=scenario.targets['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['main'], target_table, by=['tid'])
        )

    with scenario.case('Using then.field_is_copied') as case:
        case.when(
            *background()
        ).then(
            pt.then.field_is_copied(scenario.sources['main'], 'last_name',
                                    scenario.targets['main'], 'last_name'),
            pt.then.field_is_copied(scenario.sources['main'], 'first_name',
                                    scenario.targets['main'], 'first_name')
        )

    with scenario.case('Using then.fields_are_copied') as case:
        case.when(
            *background()
        ).then(
            pt.then.fields_are_copied(scenario.sources['main'], scenario.targets['main'],
                                      [
                                          ('last_name', 'last_name'),
                                          ('first_name', 'first_name')
                                      ])
        )

    with scenario.case('Using then.target_does_not_have_fields') as case:
        case.when(
            *background()
        ).then(
            pt.then.target_does_not_have_fields(scenario.targets['main'],
                                                ['glerbo', 'mcstuffins'])
        )

    with scenario.case('Using then.target_has_fields') as case:
        case.when(
            *background()
        ).then(
            pt.then.target_has_fields(
                scenario.targets['main'],
                ['last_name', 'first_name', 'full_name']
            )
        )

    with scenario.case('Using then.target_has_fields with only option') as case:
        case.when(
            *background()
        ).then(
            pt.then.target_has_fields(
                scenario.targets['main'],
                ['tid', 'last_name', 'first_name', 'full_name', 'birthdate'],
                only=True
            )
        )

    with scenario.case('Using then.target_is_empty') as case:
        case.then(
            pt.then.target_is_empty(scenario.targets['main'])
        )

    with scenario.case('Using then.target_has_n_records') as case:
        source_table = pemi.data.Table(
            '''
            | id        |
            | -         |
            | {sid[1]}  |
            | {sid[2]}  |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.target_has_n_records(scenario.targets['main'], 2)
        )



class MultipleKeyPipe(pemi.Pipe):
    def __init__(self):
        super().__init__()

        self.source(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                student_id=StringField(),
                term_id=StringField()
            )
        )

        self.target(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                student_id=StringField(),
                term_id=StringField()
            )
        )

    def flow(self):
        self.targets['main'].df = self.sources['main'].df.copy()


with pt.Scenario(
    name='Testing with Multiple Keys',
    pipe=MultipleKeyPipe(),
    factories={
        'student': StudentKeyFactory,
        'term': TermKeyFactory
    },
    sources={
        'main': lambda pipe: pipe.sources['main']
    },
    targets={
        'main': lambda pipe: pipe.targets['main']
    },
    target_case_collectors={
        # Can only configure one case collector per target.  I can't think of a use case where
        # we would need more than this.
        'main': pt.CaseCollector(subject_field='student_id', factory='student', factory_field='id')
    }
) as scenario:

    with scenario.case('it works when each row has unique keys') as case:
        source_table = pemi.data.Table(
            '''
            | student_id | term_id |
            | -          | -       |
            | {s[1]}     | {t[1]}  |
            | {s[2]}     | {t[2]}  |
            | {s[3]}     | {t[3]}  |
            '''.format(
                s=scenario.factories['student']['id'],
                t=scenario.factories['term']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        target_table = pemi.data.Table(
            '''
            | student_id | term_id |
            | -          | -       |
            | {s[1]}     | {t[1]}  |
            | {s[2]}     | {t[2]}  |
            | {s[3]}     | {t[3]}  |
            '''.format(
                s=scenario.factories['student']['id'],
                t=scenario.factories['term']['id']
            ),
            schema=scenario.targets['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['main'], target_table)
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
                s=scenario.factories['student']['id'],
                t=scenario.factories['term']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        target_table = pemi.data.Table(
            '''
            | student_id | term_id |
            | -          | -       |
            | {s[1]}     | {t[1]}  |
            | {s[1]}     | {t[2]}  |
            | {s[3]}     | {t[1]}  |
            '''.format(
                s=scenario.factories['student']['id'],
                t=scenario.factories['term']['id']
            ),
            schema=scenario.targets['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.target_matches_example(scenario.targets['main'], target_table)
        )





class SortedPipe(pemi.Pipe):
    def __init__(self):
        super().__init__()

        self.source(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                id=StringField(),
                order=StringField(),
                name=StringField(),
            )
        )

        self.target(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                tid=StringField(),
                order=StringField(),
                target_order=StringField(),
                name=StringField(),
            )
        )

    def flow(self):
        source_df = self.sources['main'].df
        target_df = pd.DataFrame(columns=list(self.targets['main'].schema.keys()))

        if len(source_df) > 0:
            target_df['tid'] = source_df['id'].apply('T{}'.format)
            target_df['name'] = source_df['name']
            target_df['order'] = source_df['order']
            target_df['target_order'] = source_df['order']
            target_df.sort_values(['order'], inplace=True)

        self.targets['main'].df = target_df

with pt.Scenario(
    name='Testing with sorted data',
    pipe=SortedPipe(),
    factories={
        'student': StudentKeyFactory
    },
    sources={
        'main': lambda pipe: pipe.sources['main']
    },
    targets={
        'main': lambda pipe: pipe.targets['main']
    },
    target_case_collectors={
        'main': pt.CaseCollector(subject_field='tid', factory='student', factory_field='tid')
    }
) as scenario:

    background = lambda: [
        pt.when.source_conforms_to_schema(
            scenario.sources['main'],
            {'id': scenario.factories['student']['id']}
        )
    ]

    with scenario.case('Using then.field_is_copied with by') as case:
        source_table = pemi.data.Table(
            '''
            | id        | order |
            | -         | -     |
            | {sid[1]}  | o2    |
            | {sid[2]}  | o1    |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.field_is_copied(scenario.sources['main'], 'name',
                                    scenario.targets['main'], 'name',
                                    by=['order'])
        )


    with scenario.case('Using then.fields_are_copied with by') as case:
        source_table = pemi.data.Table(
            '''
            | id        | order |
            | -         | -     |
            | {sid[1]}  | o2    |
            | {sid[2]}  | o1    |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.fields_are_copied(scenario.sources['main'],
                                      scenario.targets['main'],
                                      mapping=[('name', 'name'), ('order', 'order')],
                                      by=['order'])
        )


    with scenario.case('Using then.fields_are_copied with source_by and target_by') as case:
        source_table = pemi.data.Table(
            '''
            | id        | order |
            | -         | -     |
            | {sid[1]}  | o2    |
            | {sid[2]}  | o1    |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], source_table),
        ).then(
            pt.then.fields_are_copied(
                scenario.sources['main'],
                scenario.targets['main'],
                mapping=[('name', 'name'), ('order', 'order')],
                source_by=['order'],
                target_by=['target_order']
            )
        )


class FilteredPipe(pemi.Pipe):
    def __init__(self):
        super().__init__()

        self.source(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                id=StringField(),
                first_name=StringField(),
                last_name=StringField()
            )
        )

        self.target(
            pemi.PdDataSubject,
            name='main',
            schema=pemi.Schema(
                tid=StringField(),
                first_name=StringField(),
                last_name=StringField()
            )
        )

    def flow(self):
        self.targets['main'].df = self.sources['main'].df.query('last_name != "Test"').copy()


with pt.Scenario(
    name='Testing with data filters',
    pipe=FilteredPipe(),
    factories={
        'student': StudentKeyFactory
    },
    sources={
        'main': lambda pipe: pipe.sources['main']
    },
    targets={
        'main': lambda pipe: pipe.targets['main']
    },
    target_case_collectors={
        'main': pt.CaseCollector(subject_field='id', factory='student', factory_field='id')
    }
) as scenario:

    with scenario.case('Test records are filtered out') as case:
        ex_main_source = pemi.data.Table(
            '''
            | id       | last_name |
            | -        | -         |
            | {sid[1]} | Sanchez   |
            | {sid[2]} | Test      |
            | {sid[3]} | Smith     |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        ex_main_target = pemi.data.Table(
            '''
            | id       | last_name |
            | -        | -         |
            | {sid[1]} | Sanchez   |
            | {sid[3]} | Smith     |
            '''.format(
                sid=scenario.factories['student']['id']
            )
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], ex_main_source),
        ).then(
            pt.then.target_matches_example(scenario.targets['main'], ex_main_target),
        )

    with scenario.case('target_field_has_value should fail with no data') as case:
        ex_main_source = pemi.data.Table(
            '''
            | id       | last_name |
            | -        | -         |
            | {sid[1]} | Test      |
            | {sid[2]} | Test      |
            | {sid[3]} | Test      |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], ex_main_source),
        ).then(
            pt.then.target_field_has_value(
                scenario.targets['main'], 'last_name', 'Test'
            )
        ).expect_exception(pt.NoTargetDataError)


    with scenario.case('target_fields_have_values should fail with no data') as case:
        ex_main_source = pemi.data.Table(
            '''
            | id       | last_name | first_name |
            | -        | -         | -          |
            | {sid[1]} | Test      | Alpha      |
            | {sid[2]} | Test      | Alpha      |
            | {sid[3]} | Test      | Alpha      |
            '''.format(
                sid=scenario.factories['student']['id']
            ),
            schema=scenario.sources['main'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['main'], ex_main_source),
        ).then(
            pt.then.target_fields_have_values(
                scenario.targets['main'],
                {
                    'last_name': 'Test',
                    'first_name': 'Alpha'
                }
            )
        ).expect_exception(pt.NoTargetDataError)



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
        assert isinstance(mock_pipe.pipes['parent'], pemi.pipe.MockPipe)

    def test_parent_source_reassigned(self, real_pipe, mock_pipe):
        assert real_pipe.pipes['parent'].sources['parent_source'].schema \
            == mock_pipe.pipes['parent'].sources['parent_source'].schema

    def test_parent_target_reassigned(self, real_pipe, mock_pipe):
        assert real_pipe.pipes['parent'].targets['parent_target'].schema \
            == mock_pipe.pipes['parent'].targets['parent_target'].schema
