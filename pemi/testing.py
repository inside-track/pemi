from collections import OrderedDict
from itertools import tee

import os
import sys

import pytest
import pandas as pd

import pemi
import pemi.data

pd.set_option('display.expand_frame_repr', False)

#TODO: Organize and doc

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

    @staticmethod
    def source_has_keys(source, case_keys):
        def gen_values(case):
            while True:
                values = {}
                cache_id = os.urandom(32)
                for field in case_keys.subject_keys[source.name]:
                    values[field] = case_keys.cache(source.name, field, case=case)[cache_id]
                yield values

        def _when(case):
            values = gen_values(case)
            keys_df = pd.DataFrame([next(values) for i in range(len(source[case].data))])
            for field in case_keys.subject_keys[source.name]:
                source[case].data[field] = keys_df[field]

        return _when

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
    def source_conforms_to_schema(source):
        def _when(case):
            data = pemi.data.Table(
                nrows=3,
                schema=source.subject.schema
            ).df

            source[case].data = data
        return _when


class then: #pylint: disable=invalid-name
    #pylint: enable=invalid-name

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
    def field_is_copied(source, source_field, target, target_field, by=None):
        def _then(case):
            if by:
                expected = source[case].data.sort_values(by).reset_index(drop=True)[[source_field]]
                actual = target[case].data.sort_values(by).reset_index(drop=True)[[target_field]]
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
    def fields_are_copied(source, target, mapping, by=None):
        source_fields = list(set([m[0] for m in mapping]))
        target_fields = list(set([m[1] for m in mapping]))

        def _then(case):
            if by:
                expected = source[case].data.sort_values(by).reset_index(drop=True)[source_fields]
                actual = target[case].data.sort_values(by).reset_index(drop=True)[target_fields]
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
    def target_does_not_have_fields(target, *fields):
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
    def target_has_fields(target, *fields):
        def _then(case):
            missing_fields = set(fields) - set(target[case].data.columns)
            if len(missing_fields) > 0:
                raise AssertionError(
                    "The fields '{}' were expected to be found in the target".format(
                        missing_fields
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
    def __init__(self, func):
        self.func = func

    def __getitem__(self, cache=None):
        return self.func(cache)


class CaseKeyTracker:
    def __init__(self, case_key_gen):
        self.case = None

        self.cached = {}
        self.subject_case_keys = {}

        self.subject_keys = {name: list(keys.keys()) for name, keys in next(case_key_gen).items()}
        self._build_synced_generators(case_key_gen)

    def next_case(self, case):
        self.cached = {}
        self.case = case

    def _track_key(self, subject_case_keys, case=None):
        case = case if case else self.case
        for subject_name, key in subject_case_keys.items():
            if subject_name not in self.subject_case_keys:
                self.subject_case_keys[subject_name] = []
            self.subject_case_keys[subject_name].append({'case': case, 'key': key})

    def _build_synced_generators(self, case_key_gen):
        subjects = list(self.subject_keys.keys())

        self.synced_generators = {}
        for idx, tgen in enumerate(tee(case_key_gen, len(subjects) + 1)):
            if idx < len(subjects):
                self.synced_generators[subjects[idx]] = tgen
            else:
                self.synced_generators['__internal__'] = tgen

    def get_key(self, subject, field, case=None, cache=None):
        cache = os.urandom(32) if cache is None else cache
        if cache not in self.cached:
            self.cached[cache] = next(self.synced_generators['__internal__'])
            self._track_key(self.cached[cache], case=case)

        try:
            return self.cached[cache][subject][field]
        except KeyError:
            msg = "No case key found for field '{}' and subject '{}'".format(field, subject)
            raise KeyError(msg)

    def cache(self, subject, field, case=None):
        return SubscriptableLambda(lambda name: self.get_key(subject, field, case=case, cache=name))



class CaseData: #pylint: disable=attribute-defined-outside-init,too-few-public-methods
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
    def __init__(self, subject, name):
        self.subject = subject
        self.name = name
        self.data = {}

    def __getitem__(self, case):
        case_id = id(case)
        if case_id not in self.data:
            self.data[case_id] = CaseData(case, self)
        return self.data[case_id]

class Case:
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
            for i_then in self.thens:
                i_then(self)
        except AssertionError:
            msg = '\nAssertion Error for {}'.format(self)
            for name, source in self.scenario.sources.items():
                msg += '\nSource {}:\n{}'.format(name, source.subject.to_pd())
            for name, target in self.scenario.targets.items():
                msg += '\nTarget {}:\n{}'.format(name, target.subject.to_pd())
            raise AssertionError(msg)


    def __str__(self):
        return "<Case '{}' ({})>".format(self.name, id(self))

class Scenario: #pylint: disable=too-many-instance-attributes
    def __init__(self, name, *selector, usefixtures=[]):
        self.name = name
        self.selector = selector
        self.usefixtures = usefixtures

        self.runner = None
        self.case_keys = None

        self.cases = OrderedDict()
        self.has_run = False

        self.sources = None
        self.targets = None


    def _register_test(self, module_name):
        @pytest.mark.usefixtures(*self.usefixtures)
        @pytest.mark.scenario(self, *self.selector)
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

    def setup(self, runner, case_keys=None, sources=None, targets=None):
        sources = sources or {}
        targets = targets or {}

        self.runner = runner
        self.case_keys = CaseKeyTracker(case_keys)

        self.sources = {name: TestSubject(subject, name) for name, subject in sources.items()}
        self.targets = {name: TestSubject(subject, name) for name, subject in targets.items()}


    def case(self, name):
        case = Case(name, self)
        self.case_keys.next_case(case)
        self.cases[name] = case
        return case

    def setup_cases(self):
        for case in self.cases.values():
            case.setup()

        self.load_test_data()

    def load_test_data(self):
        for _, source in self.sources.items():
            if len(source.data.values()) > 0:
                all_case_data = pd.concat(
                    [cd.data for cd in source.data.values()],
                    ignore_index=True
                )
                source.subject.from_pd(all_case_data)


    def collect_results(self):
        def assign_case(target_name):
            try:
                subject_case_keys = self.case_keys.subject_case_keys[target_name]
            except KeyError:
                raise KeyError('''No case keys assigned for target '{}'.
                     This usually means you forgot to build source data with case keys.
                     '''.format(target_name))

            key_fields = list(subject_case_keys[0]['key'].keys())

            def _assign_case(row):
                row_key = set(dict(row[key_fields]).items())
                for key_case in subject_case_keys:
                    key = set(key_case['key'].items())
                    if len(row_key & key) > 0:
                        return key_case['case']
                raise KeyError("No case found for {} for target '{}'".format(row_key, target_name))
            return _assign_case

        for target_name, target in self.targets.items():
            all_target_data = target.subject.to_pd()
            for case in self.cases.values():
                target[case].data = pd.DataFrame(columns=all_target_data.columns)

            if len(all_target_data) > 0:
                all_target_data['__pemi_case__'] = all_target_data.apply(
                    assign_case(target_name), axis=1
                )
                for case, df in all_target_data.groupby(['__pemi_case__'], sort=False):
                    del df['__pemi_case__']
                    target[case].data = df

    def assert_cases(self):
        for case in self.cases.values():
            case.assert_case()

    def run(self):
        if self.has_run:
            return

        self.setup_cases()
        self.runner()
        self.collect_results()

        self.has_run = True


class MockPipe(pemi.Pipe):
    def flow(self):
        pemi.log.debug('FLOWING mocked pipe: %s', self)

def mock_pipe(parent_pipe, pipe_name):
    pipe = parent_pipe.pipes[pipe_name]
    mocked = MockPipe(name=pipe.name)
    for source in pipe.sources:
        mocked.sources[source] = pipe.sources[source]
        mocked.sources[source].pipe = mocked

    for target in pipe.targets:
        mocked.targets[target] = pipe.targets[target]
        mocked.targets[target].pipe = mocked

    #TODO: optionally copy some attributes of mocked pipe

    parent_pipe.pipes[pipe_name] = mocked
