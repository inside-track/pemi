Proposal for Refactoring Scenario Configuration
===============================================

Problems
--------

Setting up scenarios is awkward.  In practice, we've ended up with a situation where
we define a scenario in a test, and then have a separate scenario configuration object
(which is usually defined first).  So we end up with things like this:

.. code-block:: python

    # Pemi does not naturally lead to declaring a scenario configurator class, but in practice
    # we found that this was necessary to build scenarios when we just wanted to
    # vary the parameters used in a pipe (so we didn't have to duplicate sources/targets/case_keys.
    # PROPOSAL:  Would like to avoid having to invent this kind of thing in a project.
    class StudentPipeScenarioConfig:
        def __init__(self, pipe, scenario):
            self.pipe = pipe
            self.scenario = scenario
            self.setup()


        def setup(self):
            self.mock_pipe()

            self.scenario.setup(
                runner=self.pipe.flow,
                case_keys=self.case_keys(),
                sources={
                    'main_source': self.pipe.sources['main'],
                    'student_ids': self.pipe.pipes['lkp_student_uuids'].pipes['student_ids']\
                      .targets['main'],
                    'student_emails': self.pipe.pipes['student_emails'].targets['main'],
                },
                targets={
                    'create_students': self.pipe.pipes['create_students'].sources['main'],
                    'update_students': self.pipe.pipes['update_students'].sources['main'],
                }
            )

        # Why do we need to mock these manually?  I can't think of a reason why
        # we wouldn't want to mock all pipes that are sources and targets of a scenario.
        # PROPOSAL: Auto-mock pipes that are used as scenario source/targets
        #  and put the schemas of those mocked data subjects in the scenario subjects (e.g.,
        #  scenario.sources['student_ids'].schema)
        def mock_pipe(self):
            saved_schemas = {}
            for target in ['create_students', 'update_students']:
                saved_schemas[target] = self.pipe.pipes[target].schema

            pt.mock_pipe(self.pipe.pipes['lkp_student_uuids'], 'student_ids')
            pt.mock_pipe(self.pipe, 'student_emails')
            pt.mock_pipe(self.pipe, 'create_students')
            pt.mock_pipe(self.pipe, 'update_students')

            for target, schema in saved_schemas.items():
                self.pipe.pipes[target].schema = schema


        # Case keys are just confusing.  I barely know how to describe them.  There's nothing
        # obvious about what they do or how they're related to scenario sources/targets.
        # PROPOSAL: Get rid of case keys and come up with a concept that is more natural, but
        #  that preserves the functionality of case keys.
        @staticmethod
        def case_keys():
            student_uuids = pemi.data.UniqueIdGenerator('stuUUID{}'.format)
            student_ids = pemi.data.UniqueIdGenerator('stu{}'.format)
            emails = pemi.data.UniqueIdGenerator('someone{}@example.com'.format)

            while True:
                student_id = next(student_ids)
                student_uuid = next(student_uuids)
                email = next(emails)

                yield {
                    'main_source': {'student_id': student_id},
                    'student_ids': {'student_id': student_id, 'student_uuid': student_uuid},
                    'student_emails': {'student_uuid': student_uuid, 'email': email},

                    'create_students': {'external_id': student_id},
                    'update_students': {'student_uuid': student_uuid, 'external_id': student_id},
                }

        # Again, case keys are confusing, wtf does case_keys.cache mean?
        # PROPOSAL: Make this intelligible.
        def background(self):
            ex_student_ids = pemi.data.Table(
                '''
                | external_id | uuid         | merged_into_student_id |
                | -           | -            | -                      |
                | {stu[1]}    | {stuUUID[1]} |                        |
                | {stu[2]}    | {stuUUID[2]} |                        |
                '''.format(
                    stu=self.scenario.case_keys.cache('student_ids', 'student_id'),
                    stuUUID=self.scenario.case_keys.cache('student_ids', 'student_uuid')
                ),
                schema=self.pipe.pipes['lkp_student_uuids'].pipes['student_ids'].targets['main'].schema
            )

            ex_student_emails = pemi.data.Table(
                '''
                | student_uuid | email      |
                | -            | -          |
                | {stuUUID[1]} | {email[1]} |
                | {stuUUID[2]} | {email[2]} |
                '''.format(
                    stuUUID=self.scenario.case_keys.cache('student_emails', 'student_uuid'),
                    email=self.scenario.case_keys.cache('student_emails', 'email'),
                ),
                schema=self.pipe.pipes['student_emails'].targets['main'].schema
            )


            return [
                pt.when.example_for_source(self.scenario.sources['student_ids'],
                                           ex_student_ids),
                pt.when.example_for_source(self.scenario.sources['student_emails'],
                                           ex_student_emails),
            ]

    # We define a scenario, but then we have to define a scenario configuration,
    # which modifies the scenario we just created.  What's going on here is totally opaque.
    # PROPOSAL: Configure the scenario at the same time as defining it.
    with pt.Scenario('StudentPipe') as scenario:
        pipe = StudentPipe()
        scenario_config = ItkBatchStudentPipeScenarioConfig(pipe, scenario)


        with scenario.case('it does something awesome') as case:
            case.when(
                scenario_config.background()
            ).then(
                 # something awesome
            )



Proposed Refactoring
--------------------

.. code-block:: python


    # Replace "case keys" with Factories (ala factory boy - http://factoryboy.readthedocs.io/en/latest/index.html)
    # that generate the unique ids we need to put records into specific cases
    class StudentFactory(factory.Factory):
        class Meta:
            model = dict

        external_id = factory.Sequence(lambda n: 'stu{}'.format(n))
        uuid = factory.LazyFunction(uuid.uuid4)


    with pt.Scenario(
        name='The name of this scenario',
        pipe=pipe,
        runner=pipe.flow,
        factories={
            'students': StudentFactory
        },
        # Make a clear separation between scenario sources and targets so we can use 'main'
        # for both a source and target (using 'main_source'/'main_target' is an artifact
        # of the non-intuitive requirements of the case_key generator.
        sources={
            'main': pipe.sources['main'],
            'student_ids': pipe.pipes['student_ids'].targets['main'],
            'student_emails': pipe.pipes['student_emails'].targets['main'],
        },
        targets={
            'create_students': self.pipe.pipes['create_students'].sources['main'],
            'update_students': self.pipe.pipes['update_students'].sources['main'],
        },
        # Build a kind of case collector object so that it makes clear that we're assigning
        #  records to cases by identifying that some field on a target that is generated from a factory
        target_case_collectors={
            'create_students': CaseCollector(subject_field='student_id', factory=factories['students'], factory_field='external_id'),
            'update_students': CaseCollector(subject_field='student_id', factory=factories['students'], factory_field='external_id'),
        }
    ) as scenario:

        # As soon as we get to '... as scenario', we're done configuring the scenario.
        # Now we get get on to defining specific examples, backgrounds, and cases.

        # Get rid of case_keys.cache and clearly identify that an id comes from a factory.
        ex_student_ids = pemi.data.Table(
            '''
            | external_id | uuid         | merged_into_student_id |
            | -           | -            | -                      |
            | {stu[1]}    | {stuUUID[1]} |                        |
            | {stu[2]}    | {stuUUID[2]} |                        |
            '''.format(
                stu=scenario.factories['students'].field('student_id')
                stuUUID=scenario.factories['students'].field('student_uuid')
            ),
            # Reference the scenario source schema instead of the pipe
            schema=scenario.sources['student_ids'].schema
        )


        # Viable alternative to the above
        ex_student_ids = pemi.data.Table(
            '''
            | external_id           | uuid           | merged_into_student_id |
            | -                     | -              | -                      |
            | {stu[1][external_id]} | {stu[1][uuid]} |                        |
            | {stu[2][external_id]} | {stu[2][uuid]} |                        |
            '''.format(
                stu=scenario.factories['students']
            ),
            schema=scenario.sources['student_ids'].schema#self.pipe.pipes['lkp_student_uuids'].pipes['student_ids'].targets['main'].schema
        )


        # Backgrounds don't need to be methods
        background = [
            pt.when.example_for_source(scenario.sources['student_ids'], ex_student_ids),
        ]


        # Everything else as usual.
        with scenario.case('it does something awesome') as case:
            case.when(
                *background
            ).then(
                 # something awesome
            )
