from collections import OrderedDict
from collections import namedtuple
from itertools import tee

import os
import re
import io
import unittest

import pandas as pd
import pandas.util.testing

import pemi
import pemi.data

#TODO: Organize and doc

def assert_frame_equal(actual, expected, **kwargs):
    try:
        pd.util.testing.assert_frame_equal(actual, expected, **kwargs)
    except AssertionError as err:
        print('Actual:\n{}'.format(actual))
        print('Expected:\n{}'.format(expected))
        raise err

def assert_series_equal(actual, expected, **kwargs):
    actual.reset_index(drop=True, inplace=True)
    expected.reset_index(drop=True, inplace=True)
    try:
        pd.util.testing.assert_series_equal(actual, expected, **kwargs)
    except AssertionError as err:
        print('Actual:\n{}'.format(actual))
        print('Expected:\n{}'.format(expected))
        raise err

def to_tuple(d):
    return namedtuple('KeyMapTuple', d.keys())(**d)


class when():
    def source_has_keys(sources, case_keys, source_name):
        source = sources[source_name]

        def gen_values(case):
            while True:
                values = {}
                cache_id = os.urandom(32)
                for field in case_keys.subject_keys[source_name]:
                    values[field] = case_keys.cache(source_name, field, case=case)[cache_id]
                yield values

        def _when(case):
            values = gen_values(case)
            keys_df = pd.DataFrame([next(values) for i in range(len(source[case].data))])
            for field in case_keys.subject_keys[source_name]:
                source[case].data[field] = keys_df[field]

        return _when

    def source_field_has_value(source, field, value):
        def _when(case):
            if hasattr(value, '__next__'):
                n = len(source[case].data)
                source[case].data[field] = pd.Series([next(value) for i in range(n)])
            else:
                source[case].data[field] = value

        return _when

    def example_for_source(source, table):
        def _when(case):
            source[case].data = table.df

        return _when

    def source_conforms_to_schema(source):
        def _when(case):
            data = pemi.data.Table(
                nrows=3,
                schema=source.subject.schema
            ).df

            source[case].data = data
        return _when



class then():
    def target_field_has_value(target, field, value):
        def _then(case):
            target_data = target[case].data[field]
            expected_data = pd.Series([value] * len(target_data), index = target_data.index)
            assert_series_equal(target_data, expected_data, check_names=False, check_dtype=False)

        return _then

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
                raise AssertionError('Source field {} not copied to target field {}: {}'.format(
                    source_field, target_field, err)
                )

        return _then

    def fields_are_copied(source, target, mapping, by=None):
        def _then(case):
            source_fields = list(set([m[0] for m in mapping]))
            target_fields = list(set([m[1] for m in mapping]))

            if by:
                expected = source[case].data.sort_values(by).reset_index(drop=True)[source_fields]
                actual = target[case].data.sort_values(by).reset_index(drop=True)[target_fields]
            else:
                expected = source[case].data[source_fields]
                actual = target[case].data[target_fields]

            for source_field, target_field in mapping:
                try:
                    assert_series_equal(expected[source_field], actual[target_field], check_names=False)
                except AssertionError as err:
                    raise AssertionError('Source field {} not copied to target field {}: {}'.format(
                        source_field, target_field, err)
                    )

        return _then


    def target_does_not_have_fields(target, *fields):
        def _then(case):
            unexpected_fields = set(fields) & set(target[case].data.columns)
            if len(unexpected_fields) > 0:
                raise AssertionError("The fields '{}' were not expected to be found in the target".format(unexpected_fields))

        return _then

    def target_is_empty(target):
        def _then(case):
            nrecords = len(target[case].data)
            if nrecords != 0:
                 raise AssertionError('Expecting target to be empty, found {} records'.format(nrecords))

        return _then

    def target_has_n_records(target, expected_n):
        def _then(case):
            nrecords = len(target[case].data)
            if nrecords != expected_n:
                raise AssertionError('Excpecting target to have {} records, found {} records'.format(expected_n, nrecords))

        return _then




class CachedCaseKeyAccessor():
    def __init__(self, func):
        self.func = func

    def __getitem__(self, cache=None):
        return self.func(cache)


class CaseKeyTracker():
    def __init__(self, case_key_gen):
        self.case = None

        self.cached = {}
        self.subject_key_to_case = {}

        self.subject_keys = {name: list(keys.keys()) for name, keys in next(case_key_gen).items()}
        self._build_synced_generators(case_key_gen)

    def _next_case(self, case):
        self.cached = {}
        self.case = case

    def _track_key(self, subject_case_keys, case=None):
        case = case if case else self.case
        for subject_name, key in subject_case_keys.items():
            if subject_name not in self.subject_key_to_case:
                self.subject_key_to_case[subject_name] = {}
            self.subject_key_to_case[subject_name][to_tuple(key)] = case

    def _build_synced_generators(self, case_key_gen):
        subjects = list(self.subject_keys.keys())

        self.synced_generators = {}
        for idx, tgen in enumerate(tee(case_key_gen, len(subjects) + 1)):
            if idx < len(subjects):
                self.synced_generators[subjects[idx]] = tgen
            else:
                self.synced_generators['__internal__'] = tgen

    def get_key(self, subject, field, case=None, cache=None):
        cache = os.urandom(32) if not cache else cache
        if cache not in self.cached:
            self.cached[cache] = next(self.synced_generators['__internal__'])
            self._track_key(self.cached[cache], case=case)
        return self.cached[cache][subject][field]

    def cache(self, subject, field, case=None):
        return CachedCaseKeyAccessor(lambda name: self.get_key(subject, field, case=case, cache=name))



class CaseData():
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

class TestSubject():
    def __init__(self, subject):
        self.subject = subject
        self.data = {}

    def __getitem__(self, case):
        case_id = id(case)
        if not(case_id in self.data):
            self.data[case_id] = CaseData(case, self)
        return self.data[case_id]

class Case():
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
        for w in self.whens:
            w(self)

    def assert_case(self):
        self.scenario.run()

        for t in self.thens:
            t(self)

    def __str__(self):
        return "<Case '{}' ({})>".format(self.name, id(self))

class Scenario():
    def __init__(self, runner, case_keys=None, sources={}, targets={}):
        self.runner = runner
        self.case_keys = CaseKeyTracker(case_keys)

        self.cases = OrderedDict()
        self.has_run = False

        self.sources = {name: TestSubject(subject) for name, subject in sources.items()}
        self.targets = {name: TestSubject(subject) for name, subject in targets.items()}


    def case(self, name):
        case = Case(name, self)
        self.case_keys._next_case(case)
        self.cases[name] = case
        return case

    def setup_cases(self):
        for case in self.cases.values():
            case.setup()

        self.load_test_data()

    def load_test_data(self):
        for source_name, source in self.sources.items():
            if len(source.data.values()) > 0:
                all_case_data = pd.concat([cd.data for cd in source.data.values()], ignore_index=True)
                source.subject.from_pd(all_case_data)


    def collect_results(self):
        def assign_case(target_name):
            def _assign_case(row):
                key_to_case = self.case_keys.subject_key_to_case[target_name]
                # Assumes every subject CaseKey uses the same fields
                #  probably a nicer way to do this in the class
                key_fields = list(list(key_to_case.keys())[0]._fields)

                key = to_tuple(dict(row[key_fields]))
                return self.case_keys.subject_key_to_case[target_name][key]
            return _assign_case

        for target_name, target in self.targets.items():
            all_target_data = target.subject.to_pd()
            for case in self.cases.values():
                target[case].data = pd.DataFrame(columns=all_target_data.columns)

            if len(all_target_data) > 0:
                all_target_data['__pemi_case__'] = all_target_data.apply(assign_case(target_name), axis=1)
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
        pemi.log.debug('FLOWING mocked pipe: {}'.format(self))

def mock_pipe(parent_pipe, pipe_name):
    pipe = parent_pipe.pipes[pipe_name]
    mocked = MockPipe(name=pipe.name)
    for source in pipe.sources:
        mocked.sources[source] = pipe.sources[source]
        mocked.sources[source].pipe = mocked

    for target in pipe.targets:
        mocked.targets[target] = pipe.targets[target]
        mocked.targets[target].pipe = mocked

    parent_pipe.pipes[pipe_name] = mocked
