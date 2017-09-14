import re
import io
import unittest

import pandas as pd

import pemi
import pemi.data


class Scenario():
    def __init__(self, runner=lambda: None, source_subjects=[], target_subjects=[], givens=[]):
        self.runner = runner
        self.source_subjects = source_subjects
        self.target_subjects = target_subjects
        self.givens = givens
        self.whens = []
        self.thens = []

    def when(self, *whens):
        self.whens = whens
        return self

    def load_test_data_to_source_subjects(self):
        for source in self.source_subjects:
            source.from_pd(source.__test_data__)

    def get_test_data_from_target_subjects(self):
        for target in self.target_subjects:
            target.__test_data__ = target.to_pd()

    def then(self, *thens):
        self.thens = thens
        return self

    def run(self):
        for given in self.givens:
            given()
        for when in self.whens:
            when()
        self.load_test_data_to_source_subjects()
        self.runner()
        self.get_test_data_from_target_subjects()
        for then in self.thens:
            then()

        return self

class MockPipe(pemi.Pipe):
    def flow(self):
        pemi.log().debug('FLOWING mocked pipe: {}'.format(self))
        pass



def mock_pipe(pipe):
    mocked = MockPipe(name=pipe.name)
    for source in pipe.sources:
        mocked.sources[source] = pipe.sources[source]
        mocked.sources[source].pipe = mocked

    for target in pipe.targets:
        mocked.targets[target] = pipe.targets[target]
        mocked.targets[target].pipe = mocked

    return mocked



class MultipleSubjectsError(Exception): pass

class Rules():
    def __init__(self, source_subjects, target_subjects):
        self.source_subjects = source_subjects
        self.target_subjects = target_subjects

    def _find_source(self, subject):
        return self._find_subject(self.source_subjects, subject)

    def _find_target(self, subject):
        return self._find_subject(self.target_subjects, subject)

    def _find_subject(self, subject_list, subject):
        if subject == None and len(subject_list) == 1:
            return subject_list[0]
        elif subject != None:
            return subject
        else:
            raise MultipleSubjectsError('Multiple subjects defined in rules, none selected: {}'.format(subject_list))


    def when_source_conforms_to_schema(self, source_subject=None):
        'The source data subject conforms to the schema'
        source_subject = self._find_source(source_subject)

        def _when_source_conforms_to_schema():
            data = pemi.data.Table(
                schema=source_subject.schema
            ).df

            source_subject.__test_data__ = data

        _when_source_conforms_to_schema.__doc__ = '''
            The source data subject {} conforms to the schema: {}
        '''.format(
            source_subject,
            source_subject.schema
        )

        return _when_source_conforms_to_schema

    def when_sources_conform_to_schemas(self):
        return [self.when_source_conforms_to_schema(source_subject) for source_subject in self.source_subjects]


    def then_field_is_copied(self, source_field, target_field, source_subject=None, target_subject=None, by=None):
        source_subject = self._find_source(source_subject)
        target_subject = self._find_target(target_subject)

        if source_field == None:
            raise TypeError('Expecting a field name for source_field_name, got None')
        if target_field == None:
            raise TypeError('Expecting a field name for target_field_name, got None')

        def _then_field_is_copied():
            expected = source_subject.__test_data__
            actual = target_subject.__test_data__

            if by:
                expected = expected.sort_values(by).reset_index(drop=True)
                actual = actual.sort_values(by).reset_index(drop=True)

            pd.testing.assert_series_equal(expected[source_field], actual[target_field])


        _then_field_is_copied.__doc__ = '''
            The field {} from the source {} is copied to field {} on the target {}
        '''.format(
            source_subject,
            source_field,
            target_subject,
            target_field
        )
        return _then_field_is_copied

    def then_fields_are_copied(self, mapping, source_subject=None, target_subject=None, by=None):
        thens = []
        for source_field, target_field in mapping.items():
            thens.append(
                self.then_field_is_copied(
                    source_subject=source_subject,
                    target_subject=target_subject,
                    source_field=source_field,
                    target_field=target_field,
                    by=by
                )
            )
        return thens

    def when_source_field_has_value(self, field_name, field_value, source_subject=None):
        source_subject = self._find_source(source_subject)

        def _when_source_field_has_value():
            source_data = source_subject.__test_data__
            source_data[field_name] = pd.Series([field_value] * len(source_data))

        _when_source_field_has_value.__doc__ = '''
            The source field '{}' has the value "{}"
        '''.format(source_subject, field_value)
        return _when_source_field_has_value

    def then_target_field_has_value(self, field_name, field_value, target_subject=None):
        target_subject = self._find_target(target_subject)

        def _then_target_field_has_value():
            target_data = target_subject.__test_data__
            pd.testing.assert_series_equal(target_data[field_name], pd.Series([field_value] * len(target_data)), check_names=False)

        _then_target_field_has_value.__doc__ = '''
            The target field '{}' has the value "{}"
        '''.format(target_subject, field_value)
        return _then_target_field_has_value

    def when_example_for_source(self, table, source_subject=None):
        source_subject = self._find_source(source_subject)

        def _when_example_for_source():
            source_subject.__test_data__ = table.df

        _when_example_for_source.__doc__ = '''
            The following example for '{}':
            {}
        '''.format(source_subject, table.df[table.defined_fields])
        return _when_example_for_source

    def then_target_matches_example(self, expected_table, target_subject=None, by=None):
        target_subject = self._find_target(target_subject)
        subject_fields = expected_table.defined_fields

        def _then_target_matches_example():
            expected = expected_table.df[subject_fields]
            actual = target_subject.__test_data__[subject_fields]

            if by:
                expected = expected.sort_values(by).reset_index(drop=True)
                actual = actual.sort_values(by).reset_index(drop=True)
            else:
                expected = expected.reset_index(drop=True)
                actual = actual.reset_index(drop=True)

            try:
                pd.testing.assert_frame_equal(actual, expected, check_names=False)
            except AssertionError as err:
                pemi.log().error('Actual:\n{}'.format(actual))
                pemi.log().error('Expected:\n{}'.format(expected))
                raise err

        _then_target_matches_example.__doc__ = '''
            The target '{}' matches the example:
        '''.format(target_subject, expected_table.df[subject_fields])
        return _then_target_matches_example
