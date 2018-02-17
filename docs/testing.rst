Testing
=======

Testing is an essential component of software development that is
often neglected in the data and ETL world.  Pemi was designed to fill
that gap, and facilitate writing expressive data transformation tests.
Pemi tests run on top of the popular Python testing framework
`Pytest <https://docs.pytest.org/en/latest>`_.

The concepts involved in testing Pemi pipes include

* A **Scenario** describes the transformation that is being tested
  (e.g., a Pemi pipe), and the data sources and targets that are the
  subject of the test.  Scenarios are composed of one more **Cases**.
* A **Case** is a set of **Conditions** and **Expectations** that describe
  how the pipe is supposed to function.
* A **Condition** describes how the the data for a particular case is
  to be initialized -- e.g., "when the field 'name' has the value 'Xander'".
* An **Expectation** describes the expected result, after the pipe has
  been executed -- e.g., "then the field 'salutation' has the value
  'Hello Xander'".

To see these concepts play out in an example, let's write a simple
test for our ``HelloNamePipe``.  In this README, we'll talk through it
in stages, but the full example can be found in `tests/test_readme.py
<https://github.com/inside-track/pemi/blob/master/tests/test_readme.py>`_.

To aid with grouping cases into distinct scenarios, scenarios are defined using
a context-manager pattern.  So if we want to build a scenario called "Testing HelloNamePipe",
we set that up like::

    import pemi.testing as pt

    with pt.Scenario('Testing HelloNamePipe') as scenario:
        pipe = HelloNamePipe()

        scenario.setup(
            runner=pipe.flow,
            case_keys=case_keys(),
            sources={
                'input': pipe.sources['input']
            },
            targets={
                'output': pipe.targets['output']
            }
        )

Within this block, we create an instance of ``HelloNamePipe`` as the
variable ``pipe`` and configure the scenario using the ``setup`` method.
Let's quickly review the parameters of ``scenario.setup(...)``:

* ``runner`` - This is a function that is called to execute the process
  we'll be testing.  It is only called once per scenario after all
  cases have been defined.
* ``case_keys`` - Case keys are needed to identify which records belong
  to which cases.  More on this below.
* ``sources``/``targets`` - These are the sources/targets that will be
  the subject of testing.  Defined as a dictionary, the keys are a
  short-hand for referencing the specific data subjects indicated in
  the values.


With testing, we're specifying how a data transformation is supposed
to behave under certain conditions.  Typically, we're focused on how
subtle variations in the values of fields in the sources affect the
values of fields in the targets.  Each of these subtle variations
defines a **Case** that was mentioned above.  Now, it would be
possible to have the tester execute the runner for every case that
needed to be tested.  However, this could result in exceedingly slow
tests, particularly when the overhead of loading data and executing a
process is high (like it is for in-database transformations, and even
more so for Apache Spark).  Therefore, Pemi testing was built to only
execute the runner once for each scenario, regardless of how many
cases are defined. This can only work if the records of the targets
can be associated with a particular case in which the conditions and
expectations are defined.

This brings us to **Case Keys**.  The case_keys argument in the
scenario setup needs to be a generator that yields a dictionary.  The
keys of this dictionary are the targets of the scenario.  The values
are also a dictionary, but they are a dictionary where the keys are
the names of fields and the values are a value for the field that
**must be unique across all cases**.  In this case, we define the
``case_keys`` generator as::

        def case_keys():
            scooby_ids = pemi.data.UniqueIdGenerator('scooby-{}'.format)
            while True:
                scooby_id = next(scooby_ids)
                yield {
                    'input': {'id': scooby_id},
                    'output': {'id': scooby_id}
                }

``pemi.data.UniqueIdGenerator`` is a class used to help build generators
that can conform to any format needed.  Although in this case, a
simple incrementing integer would also suffice.

Column-Oriented Tests
---------------------

With the hard part out of the way, we can now define our first test
case.  Cases are also defined using a context-manager pattern.  To test
that the salutations are behaving correctly we could write::

        with scenario.case('Populating salutation') as case:
            case.when(
                pt.when.source_conforms_to_schema(scenario.sources['input']),
                pt.when.source_has_keys(scenario.sources['input'], scenario.case_keys),
                pt.when.source_field_has_value(scenario.sources['input'], 'name', 'Dawn')
            ).then(
                pt.then.target_field_has_value(scenario.targets['output'], 'salutation', 'Hello Dawn')
            )

The conditions set up the data to be tested:
* ``pt.when.source_conforms_to_schema`` loads dummy data into the source
called 'input', and uses the schema to determine the valid values
that can be used.
* ``pt.when.source_has_keys`` uses the ``scenario.case_keys`` generator to
populate the ``id`` field of ``input`` with unique values that will also
be used to collect the target and relate that to this case.
* ``pt.when.source_field_has_value`` sets up the ``name`` field of the
source data to have the value ``Dawn``.

The expectations are then:
* ``pt.then.target_field_has_value`` the target field ``salutations`` on
the output has the value ``Hello Dawn``.  If we were to modify this
value to be ``Goodbye Dawn, don't let any vampires bite you neck``, then
the test would fail.

This style of testing is referred to as "Column-Oriented" because we're only focused
on the values of particular columns.  We do not care about how the individual records
are ordered or related to one another.

Row-Oriented Tests
------------------

Column-oriented tests are not always sufficient to describe data
transformations.  Sometimes we care about how rows are related.  For
example, we might need to describe how to drop duplicate records, or
how to join two data sources together.  To that end, we can write
"Row-Oriented" tests.  While the example we are working with here
doesn't have any row operations, we can still write a test case that
highlights how it can work. ::

        with scenario.case('Dealing with many records') as case:
            ex_input = pemi.data.Table(
                '''
                | id       | name  |
                | -        | -     |
                | {sid[1]} | Spike |
                | {sid[2]} | Angel |
                '''.format(
                    sid=scenario.case_keys.cache('input', 'id')
                )
            )

            ex_output = pemi.data.Table(
                '''
                | id       | salutation  |
                | -        | -           |
                | {sid[1]} | Hello Spike |
                | {sid[2]} | Hello Angel |
                '''.format(
                    sid=scenario.case_keys.cache('output', 'id')
                )
            )

            case.when(
                pt.when.source_conforms_to_schema(scenario.sources['input']),
                pt.when.example_for_source(scenario.sources['input'], ex_input)
            ).then(
                pt.then.target_matches_example(scenario.targets['output'], ex_output)
            )

In this case, we set up two data tables to show how the output records
are related to the input records.  Using examples built with
``pemi.data.Table``, we can focus the test case on just those fields
that we care about.  If we had a source that had 80 fields in it, we
would only need to define those that we care about for this particular
test.  Pemi will use the schema defined for that source to fill in the
other fields with dummy data.

In the above example we use ``scenario.case_keys.cache`` to reference
the unique identifiers that group a set of records into a case.  In
``ex_input``, ``{sid[1]}`` will evaluate to some value specified by the
case key generator (e.g., ``scooby-9`` or ``scooby-12``, etc.).  However,
when ``{sid[1]}`` is referenced in the ``ex_output``, it will use the same
value that was generated for the ``ex_input``.

A complete version of this test can be found in `tests/test_readme.py
<https://github.com/inside-track/pemi/blob/master/tests/test_readme.py>`_.

Running Tests
-------------

Pemi tests require that the pytest package be installed in your
project.  Furthermore, you'll need to tell pytest that you want to use
pemi tests by added the following to your ``conftest.py``::

    import pemi
    pytest_plugins = ['pemi.pytest']
