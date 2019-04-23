'''

Testing is described with examples in :doc:`tests`.

.. autoclass:: Scenario
  :members:

.. autoclass:: Case
  :members:

.. autoclass:: when
  :members:

.. autoclass:: then
  :members:

'''

from collections import namedtuple
from collections import OrderedDict

import os
import sys
import inspect

import pytest
import pandas as pd

import pemi
import pemi.data

from pemi.tabular import PemiTabular

# Imported here for backwards compatiblity
from pemi.pipe import mock_pipe #pylint: disable=unused-import


#pylint: disable=too-many-lines


pd.set_option('display.expand_frame_repr', False)

class KeyFactoryFieldError(Exception): pass
class CaseStructureError(Exception): pass
class NoTargetCaseCollectorError(Exception): pass
class UnableToFindCaseError(Exception): pass
class NoTargetDataError(AssertionError): pass

def assert_frame_equal(actual, expected, **kwargs):
    try:
        pd.util.testing.assert_frame_equal(actual, expected, **kwargs)
    except AssertionError as err:
        msg = str(err)
        msg += '\nActual:\n{}'.format(actual)
        msg += '\nExpected:\n{}'.format(expected)
        raise AssertionError(msg)

def assert_series_equal(actual, expected, **kwargs):
    actual.reset_index(drop=True, inplace=True)
    expected.reset_index(drop=True, inplace=True)
    try:
        pd.util.testing.assert_series_equal(actual, expected, **kwargs)
    except AssertionError as err:
        msg = str(err)
        msg += '\nActual:\n{}'.format(actual)
        msg += '\nExpected:\n{}'.format(expected)
        raise AssertionError(msg)

class when: #pylint: disable=invalid-name
    #pylint: enable=invalid-name
    '''
    Contains methods used to set up conditions for a testing case.
    '''

    @staticmethod
    def source_field_has_value(source, field, value):
        '''
        Sets the value of a specific field to a specific value.

        Args:
            source (scenario.sources[]): The scenario source data subject.

            field (str): Name of field.

            value (str, iter): Value to set for the field.

        Examples:
            Set the value of the field ``name`` to the string value ``Buffy`` in
            the scenario source ``main``::

                case.when(
                    when.source_field_has_value(scenario.sources['main'], 'name', 'Buffy')
                )
        '''

        def _when(case):
            nrecords = len(source[case].data)
            if hasattr(value, '__next__'):
                values = pd.Series([next(value) for i in range(nrecords)])
            else:
                values = pd.Series([value]*nrecords)

            if field in source.schema:
                values = values.apply(
                    lambda v: v if hasattr(v, '__pemi_test_no_coerce__') \
                        else source.schema[field].coerce(v)
                )

            source[case].data[field] = values

        return _when

    @staticmethod
    def source_fields_have_values(source, mapping):
        '''
        Sets the value of a multiples fields to a specific values.

        Args:
            source (scenario.sources[]): The scenario source data subject.

            mapping (dict): Dictionary where the keys are the names of fields and the values
                are the values those fields are to be set to.

        Examples:
            Set the value of the field ``name`` to the string value ``Buffy`` and
            the value of the field ``vampires_slain`` to ``133`` in
            the scenario source ``main``::

                case.when(
                    when.source_fields_have_values(
                        scenario.sources['main'],
                        {
                            'name': 'Buffy',
                            'vampires_slain': 133
                        }
                    )
                )
        '''

        def _when(case):
            for field, value in mapping.items():
                nrecords = len(source[case].data)
                if hasattr(value, '__next__'):
                    values = pd.Series([next(value) for i in range(nrecords)])
                else:
                    values = pd.Series([value]*nrecords)

                if field in source.schema:
                    values = values.apply(
                        lambda v, f=field: v if hasattr(v, '__pemi_test_no_coerce__') \
                            else source.schema[f].coerce(v)
                    )

                source[case].data[field] = values


        return _when

    @staticmethod
    def example_for_source(source, table):
        """
        Set specific rows and columns to specific values.

        Args:
            source (scenario.sources[]): The scenario source data subject.

            table (pemi.data.Table): Pemi data table to use for specifying data.

        Example:
            Given a Pemi data table, specify rows and columns for the source `main`::

                case.when(
                    when.example_for_source(
                        scenario.sources['main'],
                        pemi.data.Table(
                            '''
                            | id       | name  |
                            | -        | -     |
                            | {sid[1]} | Spike |
                            | {sid[2]} | Angel |
                            '''.format(
                                sid=scenario.factories['vampires']['id']
                            )
                        )
                    )
                )

        """

        def _when(case):
            source[case].data = table.df

        return _when

    @staticmethod
    def source_conforms_to_schema(source, key_factories=None):
        '''
        Creates 3 records and fills out data for a data source subject
        that conforms to the data types specified by the data
        subject's schema.

        Args:
            source (scenario.sources[]): The scenario source data subject.

            key_factories (dict): A dictionary where the keys are the names of fields
            and the values are the field value generator originating from a scenario
            key factory (just see the example ;)).

        Example:

            For the source subject 'main', this will generate faked data that conforms
            to the schema defined for main.  It will also populate the ``id`` field
            with values generated from the ``id`` field in the ``vampires`` factory::

                case.when(
                    when.source_conforms_to_schema(
                        scenario.sources['main'],
                        {'id': scenario.factories['vampires']['id']}
                    )
                )

        '''


        key_factories = key_factories or {}
        ntestrows = 3

        key_values = {
            field: [factory[os.urandom(32)] for _ in range(ntestrows)]
            for field, factory in key_factories.items()
        }

        def _when(case):
            data = pemi.data.Table(
                nrows=ntestrows,
                schema=source.subject.schema
            ).df

            for field, values in key_values.items():
                data[field] = pd.Series(values, index=data.index)

            source[case].data = data
        return _when


class then: #pylint: disable=invalid-name
    #pylint: enable=invalid-name

    '''
    Contains methods used to test that actual outcome is equal to expected outcome.
    '''

    @staticmethod
    def target_field_has_value(target, field, value):
        '''
        Asserts that a specific field has a specific value.

        Args:
            target (scenario.targets[]): The scenario target data subject.

            field (str): Name of field.

            value (str): Value of the field that is expected.

        Examples:
            Asserts that the value of the field ``name`` is set to the string value ``Buffy`` in
            the scenario target ``main``::

                case.then(
                    then.target_field_has_value(scenario.targets['main'], 'name', 'Buffy')
                )
        '''

        def _then(case):
            if len(target[case].data[field]) < 1:
                raise NoTargetDataError('Target has no data for case')

            target_data = pd.Series(list(target[case].data[field]))
            expected_data = pd.Series([value] * len(target_data), index=target_data.index)

            assert_series_equal(target_data, expected_data,
                                check_names=False,
                                check_dtype=False,
                                check_datetimelike_compat=True)

        return _then

    @staticmethod
    def target_fields_have_values(target, mapping):
        '''
        Asserts that multiple fields have specific values.

        Args:
            target (scenario.targets[]): The scenario target data subject.

            mapping (dict): Dictionary where the keys are the names of fields and the values
                are the expected values those fields.

        Examples:
            Asserts that the value of the field ``name`` is the string value ``Buffy`` and
            the value of the field ``vampires_slain`` is ``133`` in
            the scenario target ``main``::

                case.then(
                    then.target_fields_have_values(
                        scenario.targets['main'],
                        {
                            'name': 'Buffy',
                            'vampires_slain': 133
                        }
                    )
                )
        '''

        def _then(case):
            if len(target[case].data) < 1:
                raise NoTargetDataError('Target has no data for case')

            actual = target[case].data[list(mapping.keys())]
            expected = pd.DataFrame(index=actual.index)
            for k, v in mapping.items():
                expected[k] = pd.Series([v] * len(actual), index=actual.index)

            assert_frame_equal(actual, expected, check_names=False, check_dtype=False)
        return _then

    @staticmethod
    def target_matches_example(target, expected_table, by=None):
        """
        Asserts that a given target matches an example data table

        Args:
            target (scenario.targets[]): The scenario target data subject.

            expected_table (pemi.data.Table): Expected result data.  If the table
                has fewer columns than the pipe generates, those extra columns are
                not considered in the comparison.

            by (list): A list of field names to sort the result data by before
                performing the comparison.

        Examples:
            Asserts that the scenario target ``main`` conforms to the expected data::

                case.then(
                    then.target_matches_example(
                        scenario.targets['main'],
                        pemi.data.Table(
                            '''
                            | id       | name  |
                            | -        | -     |
                            | {sid[1]} | Spike |
                            | {sid[2]} | Angel |
                            '''.format(
                                sid=scenario.factories['vampires']['id']
                            )
                        ),
                        by=['id'] #esp important if the ids are generated randomly
                    )
                )
        """


        subject_fields = expected_table.defined_fields

        def _then(case):
            expected = expected_table.df[subject_fields]
            actual = target[case].data[subject_fields]

            if by:
                expected = expected.sort_values(by).reset_index(drop=True)
                actual = actual.sort_values(by).reset_index(drop=True)
            else:
                expected = expected.reset_index(drop=True)
                actual = actual.reset_index(drop=True)

            assert_frame_equal(actual, expected, check_names=False, check_dtype=False)

        return _then

    @staticmethod
    def field_is_copied(source, source_field, target, target_field, by=None, #pylint: disable=too-many-arguments
                        source_by=None, target_by=None):
        '''
        Asserts that a field value is copied from the source to the target.

        Args:
            source (scenario.sources[]): The scenario source data subject.

            source_field (str): The name of the source field.

            target (scenario.targets[]): The scenario target data subject.

            target_field (str): The name of the target field.

            by (list): A list of field names to sort the data by before
                performing the comparison.

            source_by (list): A list of field names to sort the source data by before
                performing the comparison (uses ``by`` if not given).

            target_by (list): A list of field names to sort the target data by before
                performing the comparison (uses ``by`` if not given).

        Examples:
            Asserts that the value of the source field ``name`` is copied to the
            target field ``slayer_name``::

                case.then(
                    then.field_is_copied(
                        scenario.sources['main'], 'name',
                        scenario.targets['main'], 'slayer_name',
                        by=['id']
                    )
                )
        '''

        source_by = source_by or by
        target_by = target_by or by or source_by

        def _then(case):
            if source_by:
                expected = source[case].data.sort_values(source_by)\
                                            .reset_index(drop=True)[[source_field]]
                actual = target[case].data.sort_values(target_by)\
                                          .reset_index(drop=True)[[target_field]]
            else:
                expected = source[case].data[[source_field]]
                actual = target[case].data[[target_field]]

            try:
                assert_series_equal(actual[target_field], expected[source_field], check_names=False)
            except AssertionError as err:
                raise AssertionError(
                    'Source field {} not copied to target field {}: {}'.format(
                        source_field, target_field, err
                    )
                )

        return _then

    @staticmethod
    def fields_are_copied(source, target, mapping, by=None, source_by=None, target_by=None): #pylint: disable=too-many-arguments
        '''
        Asserts that various field values are copied from the source to the target.

        Args:
            source (scenario.sources[]): The scenario source data subject.

            target (scenario.targets[]): The scenario target data subject.

            mapping (list): A list of tuples.  Each tuple contains the source field name
                and target field name, in that order.

            by (list): A list of field names to sort the data by before
                performing the comparison.

            source_by (list): A list of field names to sort the source data by before
                performing the comparison (uses ``by`` if not given).

            target_by (list): A list of field names to sort the target data by before
                performing the comparison (uses ``by`` if not given).

        Examples:
            Asserts that the value of the source field ``name`` is copied to the
            target field ``slayer_name`` and ``num`` is copied to ``vampires_slain``::

                case.then(
                    then.fields_are_copied(
                        scenario.sources['main'],
                        scenario.targets['main'],
                        [
                            ('name', 'slayer_name'),
                            ('num', 'vampires_slain')
                        ],
                        by=['id']
                    )
                )
        '''


        source_fields = list({m[0] for m in mapping})
        target_fields = list({m[1] for m in mapping})

        source_by = source_by or by
        target_by = target_by or by or source_by

        def _then(case):
            if source_by:
                expected = source[case].data.sort_values(source_by)\
                                            .reset_index(drop=True)[source_fields]
                actual = target[case].data.sort_values(target_by)\
                                          .reset_index(drop=True)[target_fields]
            else:
                expected = source[case].data[source_fields]
                actual = target[case].data[target_fields]

            for source_field, target_field in mapping:
                try:
                    assert_series_equal(actual[target_field], expected[source_field],
                                        check_names=False, check_dtype=False)
                except AssertionError as err:
                    raise AssertionError(
                        'Source field {} not copied to target field {}: {}'.format(
                            source_field, target_field, err
                        )
                    )

        return _then


    @staticmethod
    def target_does_not_have_fields(target, fields):
        '''
        Asserts that the target does not have certain fields.

        Args:
            target (scenario.targets[]): The scenario target data subject.

            fields (list): List of field names that should not be on the target.

        Examples:
            Asserts that the scenario target ``main`` does not have the fields
            ``sparkle_factor`` or ``is_werewolf``::

                case.then(
                    then.target_does_not_have_fields(
                        scenario.targets['main'],
                        ['sparkle_factor', 'is_werewolf']
                    )
                )
        '''


        def _then(case):
            unexpected_fields = set(fields) & set(target[case].data.columns)
            if len(unexpected_fields) > 0:
                raise AssertionError(
                    "The fields '{}' were not expected to be found in the target".format(
                        unexpected_fields
                    )
                )

        return _then

    @staticmethod
    def target_has_fields(target, fields, only=False):
        '''
        Asserts that the target has certain fields.

        Args:
            target (scenario.targets[]): The scenario target data subject.

            fields (list): List of field names that should not be on the target.

            only (bool): Specifies whether the target should only have the fields listed.  Raises
                an exception if there are additional fields.

        Examples:
            Asserts that the scenario target ``main`` only has the fields
            ``name`` and ``vampires_slain``::

                case.then(
                    then.target_has_fields(
                        scenario.targets['main'],
                        ['name', 'vampires_slain'],
                        only=True
                    )
                )
        '''

        def _then(case):
            missing_fields = set(fields) - set(target[case].data.columns)
            extra_fields = set(target[case].data.columns) - set(fields)

            if len(missing_fields) > 0:
                raise AssertionError(
                    "The fields '{}' were expected to be found in the target".format(
                        missing_fields
                    )
                )

            if len(extra_fields) > 0 and only:
                raise AssertionError(
                    "The fields '{}' were not expected to be found on the target".format(
                        extra_fields
                    )
                )

        return _then

    @staticmethod
    def target_is_empty(target):
        '''
        Asserts that the target has no records.

        Args:
            target (scenario.targets[]): The scenario target data subject.

        Examples:
            Asserts that the scenario target ``errors`` does not have any records::

                case.then(then.target_is_empty(scenario.targets['errors'])
        '''

        def _then(case):
            nrecords = len(target[case].data)
            if nrecords != 0:
                raise AssertionError(
                    'Expecting target to be empty, found {} records'.format(
                        nrecords
                    )
                )

        return _then

    @staticmethod
    def target_has_n_records(target, expected_n):
        '''
        Asserts that the target has a specific number of records.

        Args:
            target (scenario.targets[]): The scenario target data subject.

            expected_n (int): The number of records expected.

        Examples:
            Asserts that the scenario target ``main`` has 3 records::

                case.then(then.target_has_n_records(scenario.targets['main'], 3)
        '''

        def _then(case):
            nrecords = len(target[case].data)
            if nrecords != expected_n:
                raise AssertionError(
                    'Excpecting target to have {} records, found {} records'.format(
                        expected_n, nrecords
                    )
                )

        return _then


class SubscriptableLambda: #pylint: disable=too-few-public-methods
    '''
    Used to help with putting specific values in example data tables.

    Args:
        func (func): Some python function you want to access as subscriptable.

    Examples:
        In the simplest form::

            sl = SubscriptableLambda(lambda v: v + 10)
            sl[3] #=> 13


        This class is useful in tests when creating complex methods that need to be used
        int table data::

            payload = pt.SubscriptableLambda(lambda ref: json.dumps({
                'external_id': scenario.factories['students']['external_id'][ref]
            }))

            response = pt.SubscriptableLambda(lambda ref: json.dumps([{
                'itk-api': [
                    {'resource_uuid': scenario.factories['students']['uuid'][ref]}
                ]
            }]))

            ex_create_response = pemi.data.Table(
                """
                | payload             | response             |
                | -                   | -                    |
                | {payload[created1]} | {response[created1]} |
                | {payload[created2]} | {response[created2]} |
                | {payload[created3]} | {response[created3]} |
                | {payload[created4]} | {response[created4]} |
                """.format(
                    payload=payload,
                    response=response
                ),
                schema=pemi.Schema(
                    payload=JsonField(),
                    response=JsonField()
                )
            )
    '''

    def __init__(self, func):
        self.func = func

    def __getitem__(self, key=None):
        return self.func(key)


CaseCollector = namedtuple('CaseCollector', ['subject_field', 'factory', 'factory_field'])

class DuplicateScenarioError(Exception): pass
class DuplicateCaseError(Exception): pass

class Scenario: #pylint: disable=too-many-instance-attributes, too-many-arguments
    '''
    A **Scenario** describes the transformation that is being tested
    (a Pemi pipe), and the data sources and targets that are the
    subject of the test.  Scenarios are composed of one more **Cases**.

    Args:
        name (str): The name of a scenario.  Multiple scenarios may be present in a file,
            but the names of each scenario must be unique.

        pipe (pemi.Pipe): The Pemi pipe that is the main subject of the test.  Test
            data will be provided to the sources of the pipe (defined below), and the pipe
            will be executed.  Note that the pipe is only executed once per scenario.

        flow (str): The name of the method used to execute the pipe (default: `flow`).

        factories(dict): A dictionary where the keys are the names of factories and
            the values are FactoryBoy factories that will be used to generate unique keys.

        sources (dict): A dictionary where the keys are the names of sources that will
            be the subjects of testing.  The values are methods that accept the pipe
            referenced in the **pipe** argument above and return the data subject that
            will be used as a source.

        targets (dict): A dictionary where the keys are the names of targets that will
            be the subjects of testing.  The values are methods that accept the pipe
            referenced in the **pipe** argument above and return the data subject that
            will be used as a target.

        target_case_collectors (dict): A dictionary where the keys are the names of the
            targets that will be the subjects of testing.  The values are ``CaseCollector``
            objects that tie a field in the scenario's target to the field in a given factory.
            Every named target needs to have a case collector.

        selector (str): A string representing a regular expression.  Any case names that
            **do not** match this regex will be excluded from testing.

        usefixtures (str): Name of a Pytest fixture to use for the scenario.  Often used
            for database setup/teardown options.

    '''

    def __init__(self, name, pipe, factories, sources, targets, target_case_collectors,
                 flow='flow', selector=None, usefixtures=None):
        self.name = name
        self.pipe = pipe
        self.flow = flow
        self.factories = self._setup_factories(factories)
        self.sources = self._setup_subjects(sources)
        self.targets = self._setup_subjects(targets)
        self.target_case_collectors = target_case_collectors
        self.selector = selector
        self.usefixtures = usefixtures or []

        self.cases = OrderedDict()
        self.has_run = False

    def _register_test(self, module_name):
        @pytest.mark.usefixtures(*self.usefixtures)
        @pytest.mark.scenario(self, self.selector)
        def test_scenario(case):
            case.assert_case()
        test_attr = 'testScenario:{}'.format(self.name)
        if hasattr(sys.modules[module_name], test_attr):
            raise DuplicateScenarioError(
                'Scenario names must be unique to a module.  '
                'Duplicate detected: {}'.format(test_attr)
            )
        setattr(sys.modules[module_name], test_attr, test_scenario)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        current_frame = inspect.currentframe()
        calling_module = inspect.getouterframes(current_frame)[1].frame.f_locals['__name__']
        self._register_test(calling_module)

    @staticmethod
    def _setup_factories(factories):
        return {name: KeyFactory(factory) for name, factory in factories.items()}

    def _setup_subjects(self, subjects):
        return {name: TestSubject(subject(self.pipe), name) for name, subject in subjects.items()}

    def case(self, name):
        if name in self.cases:
            raise DuplicateCaseError(
                'Case names must be unique to a scenario.  '
                'Duplicate case detected in scenario "{}": "{}"'.format(
                    self.cases[name].scenario.name, name
                )
            )

        case = Case(name, self)

        for factory in self.factories.values():
            factory.next_case(case)

        self.cases[name] = case
        return case

    def run(self):
        if self.has_run:
            return

        self.setup_cases()
        getattr(self.pipe, self.flow)()
        self.collect_results()

        self.has_run = True

    def setup_cases(self):
        for case in self.cases.values():
            case.setup()

        self.load_test_data()

    def load_test_data(self):
        for _, source in self.sources.items():
            if len(source.data.values()) > 0:
                all_case_data = pd.concat(
                    [cd.data for cd in source.data.values()],
                    ignore_index=True,
                    sort=False
                )
                source.subject.from_pd(all_case_data)

    def collect_results(self):
        for target_name, target in self.targets.items():
            all_target_data = target.subject.to_pd()
            for case in self.cases.values():
                target[case].data = pd.DataFrame(columns=all_target_data.columns)

            try:
                collector = self.target_case_collectors[target_name]
            except KeyError:
                raise NoTargetCaseCollectorError(
                    'No case collector defined for target {}'.format(target_name)
                )

            if len(all_target_data) > 0:
                all_target_data['__pemi_case__'] = all_target_data[collector.subject_field].apply(
                    self.factories[collector.factory].case_lookup(collector)
                )
                for case, df in all_target_data.groupby(['__pemi_case__'], sort=False):
                    del df['__pemi_case__']
                    target[case].data = df

class Case:
    '''
    A **Case** is a set of **Conditions** and **Expectations** that describe
    how the pipe is supposed to function.

    Args:

        name (str): The name of the case.  The names of cases within a scenario must be unique.

        scenario (pemi.testing.Scenario): The scenario object that this case is associated with.
    '''

    def __init__(self, name, scenario):
        self.name = name
        self.scenario = scenario
        self.whens = []
        self.thens = []
        self.expected_exception = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def when(self, *funcs):
        '''
        Accepts a list of functions that are used to set up the data for a specific case.
        Each of the functions should accept one argument, which is the case object.
        See pemi.testing.when for examples.
        '''

        self.whens.extend(funcs)
        return self

    def then(self, *funcs):
        '''
        Accepts a list of functions that are used to test the result data for a specific case.
        Each of the functions should accept one argument, which is the case object.
        See pemi.testing.then for examples.
        '''

        self.thens.extend(funcs)
        return self

    def expect_exception(self, exception):
        '''
        Used to indicate that the test case is expected to fail with exception ``exception``.
        If the test case raises this exception, then it will pass.  If it does not raise the
        exception, then it will fail.
        '''

        self.expected_exception = exception

    def setup(self):
        for i_when in self.whens:
            i_when(self)

    def _assert_then(self, i_then):
        if self.expected_exception:
            with pytest.raises(self.expected_exception):
                i_then(self)
        else:
            i_then(self)

    def assert_case(self):
        self.scenario.run()

        try:
            if len(self.thens) < 1:
                raise CaseStructureError
            for i_then in self.thens:
                self._assert_then(i_then)

        except AssertionError:
            errors_tbl = PemiTabular()
            msg = '\nAssertion Error for {}'.format(self)
            for name, source in self.scenario.sources.items():
                source_df = source.subject.to_pd()
                msg += '\nSource {}:\n{}'.format(name, source_df)
                errors_tbl.add(df=source_df, df_name="\nSource {}".format(name))
            for name, target in self.scenario.targets.items():
                target_df = target.subject.to_pd()
                msg += '\nTarget {}:\n{}'.format(name, target_df)
                errors_tbl.add(df=target_df, df_name="\nTarget {}".format(name))
            errors_tbl.render(file='pemi-errors.html')
            raise AssertionError(msg)
        except CaseStructureError:
            msg = '\nCase Structure Error for {}'.format(self)
            msg += '\tNo .then clause found in test case'
            raise CaseStructureError(msg)

    def __str__(self):
        return "<Case '{}' ({})>".format(self.name, id(self))






class KeyFactoryField: #pylint: disable=too-few-public-methods
    '''
    For internal use only.

    Used to access a particular field from a given key factory.

    Example::

        kff = KeyFactoryField(keyfactory, 'id')
        kff[2] #=> Returns the 'id' field from the keyfactory instance referenced by then integer 2
    '''

    def __init__(self, keyfactory, field):
        self.keyfactory = keyfactory
        self.field = field

    def __getitem__(self, ref=None):
        '''
        Args:
          ref (Object) - Any hashable object used to reference a particular key factory instance.
        '''
        instance = self.keyfactory.instance(ref)
        if self.field in instance:
            return instance[self.field]

        raise KeyFactoryFieldError(
            'Key field "{}" not defined for factory {}'.format(
                self.field, self.keyfactory.factory
            )
        )

class KeyFactory:
    '''
    For internal use only.

    Wrapper around a FactoryBoy factory that caches any factory instances created.

    Example::

        class BeersKeyFactory(factory.Factory):
            class Meta:
                model = dict
            beer_id = factory.Sequence(lambda n: n)

        keyfactory = KeyFactory(BeersKeyFactory)

        keyfactory.instance('a') #=> generates a new instance of BeersKeyFactory cached with key 'a'
        keyfactory['beer_id']['a'] #=> Returns field 'beer_id' from cache referenced by 'a'

        keyfactory.instance('z') #=> generates a new instance of BeersKeyFactory cached with key 'z'
        keyfactory['beer_id']['z'] #=> Returns field 'beer_id' from cache referenced by 'z'
    '''

    def __init__(self, factory):
        self.factory = factory
        self.case = None
        self.cached = {}
        self.next_case(None)

    def next_case(self, case):
        self.case = case
        self.cached[self.case] = {}

    def __getitem__(self, field):
        return KeyFactoryField(self, field)

    def instance(self, ref=None):
        ref = os.urandom(32) if ref is None else ref

        if ref not in self.cached[self.case]:
            self.cached[self.case][ref] = self.factory()
        return self.cached[self.case][ref]

    def case_lookup(self, collector):
        lkp = {}
        for case_id, cached_keys in self.cached.items():
            for keys in cached_keys.values():
                if collector.factory_field not in keys:
                    raise KeyFactoryFieldError(
                        '"{}" is not a known factory fields for {}'.format(
                            collector.factory_field, self.factory
                        )
                    )
                lkp[keys[collector.factory_field]] = case_id

        def _lkp(v):
            try:
                return lkp[v]
            except KeyError as _err:
                raise UnableToFindCaseError(
                    'Unable to associate field "{}" and value "{}" with a case'.format(
                        collector.subject_field, v
                    )
                )

        return _lkp



class CaseData: #pylint: disable=attribute-defined-outside-init,too-few-public-methods
    '''
    For internal use only.

    Creates a dataframe for a specific test case.
    All of the ``when`` conditions in a case will add or modify this dataframe
    before it gets concatenated with other cases and run through the pipe that
    is the subject of the scenario.
    '''

    def __init__(self, case, test_subject):
        self.case = case
        self.test_subject = test_subject

    @property
    def data(self):
        if not hasattr(self, '_data'):
            cols = self.test_subject.subject.schema.keys()
            self._data = pd.DataFrame([[None] * len(cols)], columns=cols)

        return self._data

    @data.setter
    def data(self, value):
        self._data = value

class TestSubject: #pylint: disable=too-few-public-methods
    '''
    For internal use only.

    Wrapper around an actual pipe data subject.  Used to set or fetch case-specific data records.
    '''

    def __init__(self, subject, name):
        self.subject = subject
        self.name = name
        self.data = {}
        self.schema = self.subject.schema

    def __getitem__(self, case):
        if case not in self.data:
            self.data[case] = CaseData(case, self)
        return self.data[case]
