from collections import namedtuple
from collections import OrderedDict

import os
import sys

import pytest
import pandas as pd

import pemi
import pemi.data

# Imported here for backwards compatiblity
from pemi.pipe import mock_pipe #pylint: disable=unused-import

pd.set_option('display.expand_frame_repr', False)

class KeyFactoryFieldError(Exception): pass
class CaseStructureError(Exception): pass
class NoTargetCaseCollectorError(Exception): pass
class UnableToFindCaseError(Exception): pass

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
        def _when(case):
            nrecords = len(source[case].data)
            if hasattr(value, '__next__'):
                source[case].data[field] = pd.Series([next(value) for i in range(nrecords)])
            else:
                source[case].data[field] = pd.Series([value]*nrecords)

        return _when

    @staticmethod
    def source_fields_have_values(source, mapping):
        def _when(case):
            for field, value in mapping.items():
                nrecords = len(source[case].data)
                if hasattr(value, '__next__'):
                    source[case].data[field] = pd.Series([next(value) for i in range(nrecords)])
                else:
                    source[case].data[field] = pd.Series([value]*nrecords)

        return _when

    @staticmethod
    def example_for_source(source, table):
        def _when(case):
            source[case].data = table.df

        return _when

    @staticmethod
    def source_conforms_to_schema(source, key_factories=None):
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
        def _then(case):
            target_data = pd.Series(list(target[case].data[field]))
            expected_data = pd.Series([value] * len(target_data), index=target_data.index)

            assert_series_equal(target_data, expected_data,
                                check_names=False,
                                check_dtype=False,
                                check_datetimelike_compat=True)

        return _then

    @staticmethod
    def target_fields_have_values(target, mapping):
        def _then(case):
            actual = target[case].data[list(mapping.keys())]
            expected = pd.DataFrame(index=actual.index)
            for k, v in mapping.items():
                expected[k] = pd.Series([v] * len(actual), index=actual.index)

            assert_frame_equal(actual, expected, check_names=False, check_dtype=False)
        return _then

    @staticmethod
    def target_matches_example(target, expected_table, by=None):
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
                assert_series_equal(expected[source_field], actual[target_field], check_names=False)
            except AssertionError as err:
                raise AssertionError(
                    'Source field {} not copied to target field {}: {}'.format(
                        source_field, target_field, err
                    )
                )

        return _then

    @staticmethod
    def fields_are_copied(source, target, mapping, by=None, source_by=None, target_by=None): #pylint: disable=too-many-arguments
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
                    assert_series_equal(expected[source_field], actual[target_field],
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
    Used to help with putting specific values in example data tables.  For example::

        payload = pt.SubscriptableLambda(lambda cache: json.dumps({
            'external_id': scenario.factories['students']['external_id'][cache]
        }))

        response = pt.SubscriptableLambda(lambda cache: json.dumps([{
            'itk-api': [
                {'resource_uuid': scenario.factories['students']['uuid'][cache]}
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

class Scenario: #pylint: disable=too-many-instance-attributes, too-many-arguments
    '''
    A scenario blah blah.

    Args::
       blah blah
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
        setattr(sys.modules[module_name], 'testScenario:{}'.format(self.name), test_scenario)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        import inspect
        current_frame = inspect.currentframe()
        calling_module = inspect.getouterframes(current_frame)[1].frame.f_locals['__name__']
        self._register_test(calling_module)

    @staticmethod
    def _setup_factories(factories):
        return {name: KeyFactory(factory) for name, factory in factories.items()}

    def _setup_subjects(self, subjects):
        return {name: TestSubject(subject(self.pipe), name) for name, subject in subjects.items()}

    def case(self, name):
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
    A case blah blah....
    '''

    def __init__(self, name, scenario):
        self.name = name
        self.scenario = scenario
        self.whens = []
        self.thens = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def when(self, *funcs):
        self.whens.extend(funcs)
        return self

    def then(self, *funcs):
        self.thens.extend(funcs)
        return self

    def setup(self):
        for i_when in self.whens:
            i_when(self)

    def assert_case(self):
        self.scenario.run()

        try:
            if len(self.thens) < 1:
                raise CaseStructureError
            for i_then in self.thens:
                i_then(self)
        except AssertionError:
            msg = '\nAssertion Error for {}'.format(self)
            for name, source in self.scenario.sources.items():
                msg += '\nSource {}:\n{}'.format(name, source.subject.to_pd())
            for name, target in self.scenario.targets.items():
                msg += '\nTarget {}:\n{}'.format(name, target.subject.to_pd())
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
