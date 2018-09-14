import factory

import pemi
import pemi.testing as pt
from pemi.fields import *

class HelloNamePipe(pemi.Pipe):
    # Override the constructor to configure the pipe
    def __init__(self, **kwargs):
        # Make sure to call the parent constructor
        super().__init__(**kwargs)

        # Add a data source to our pipe - a pandas dataframe called 'input'
        self.source(
            pemi.PdDataSubject,
            name='input',
            schema=pemi.Schema(
                name=StringField()
            )
        )

        # Add a data target to our pipe - a pandas dataframe called 'output'
        self.target(
            pemi.PdDataSubject,
            name='output'
        )

    # All pipes must define a 'flow' method that is called to execute the pipe
    def flow(self):
        self.targets['output'].df = self.sources['input'].df.copy()
        self.targets['output'].df['salutation'] = self.sources['input'].df['name'].apply(
            'Hello {}'.format
        )

class KeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence(lambda n: 'scooby-{}'.format(n))


with pt.Scenario(
    name='Testing HelloNamePipe',
    pipe=HelloNamePipe(),
    factories={
        'scooby': KeyFactory
    },
    sources={
        'input': lambda pipe: pipe.sources['input']
    },
    targets={
        'output': lambda pipe: pipe.targets['output']
    },
    target_case_collectors={
        'output': pt.CaseCollector(subject_field='id', factory='scooby', factory_field='id')
    }
) as scenario:

    with scenario.case('Populating salutation') as case:
        case.when(
            pt.when.source_conforms_to_schema(
                scenario.sources['input'],
                {'id': scenario.factories['scooby']['id']}
            ),
            pt.when.source_field_has_value(scenario.sources['input'], 'name', 'Dawn')
        ).then(
            pt.then.target_field_has_value(scenario.targets['output'], 'salutation', 'Hello Dawn')
        )

    with scenario.case('Name is copied') as case:
        case.when(
            pt.when.source_conforms_to_schema(
                scenario.sources['input'],
                {'id': scenario.factories['scooby']['id']}
            ),
        ).then(
            pt.then.field_is_copied(scenario.sources['input'], 'name',
                                    scenario.targets['output'], 'name')
        )

    with scenario.case('Dealing with many records') as case:
        ex_input = pemi.data.Table(
            '''
            | id       | name  |
            | -        | -     |
            | {sid[1]} | Spike |
            | {sid[2]} | Angel |
            '''.format(
                sid=scenario.factories['scooby']['id']
            )
        )

        ex_output = pemi.data.Table(
            '''
            | id       | salutation  |
            | -        | -           |
            | {sid[1]} | Hello Spike |
            | {sid[2]} | Hello Angel |
            '''.format(
                sid=scenario.factories['scooby']['id']
            )
        )

        case.when(
            pt.when.example_for_source(scenario.sources['input'], ex_input)
        ).then(
            pt.then.target_matches_example(scenario.targets['output'], ex_output)
        )
