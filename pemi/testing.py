import re
import io
import unittest

import pandas as pd
import faker
from faker import Factory

import pemi

fake = Factory.create()

class InvalidHeaderSeparatorError(Exception): pass

default_fakers = {
    'integer': fake.pyint,
    'string': fake.word,
    'date': fake.date_object,
    'datetime': fake.date_time,
    'float': fake.pyfloat,
    'decimal': fake.pydecimal,
    'boolean': fake.pybool
}


class TestTable:
    def __init__(self, markdown=None, nrows=10, schema=pemi.Schema(), fake_with={}):
        self.markdown = markdown
        self.schema = pemi.Schema(schema)
        self.nrows = nrows
        self.fake_with = fake_with

        if self.markdown:
            self.defined_fields = list(self._build_from_markdown().columns)
        else:
            self.defined_fields = list(self.schema.keys())

    def _clean_markdown(self):
        cleaned = self.markdown

        # Remove trailing comments
        cleaned = re.compile(r'(#.*$)', flags = re.MULTILINE).sub('', cleaned)

        # Remove beginning and terminal pipe on each row
        cleaned = re.compile(r'(^\s*\|\s*|\s*\|\s*$)', flags = re.MULTILINE).sub('', cleaned)

        # Remove whitespace surrouding pipes
        cleaned = re.compile(r'\s*\|\s*').sub('|', cleaned)

        # Split by newlines
        cleaned = cleaned.split('\n')

        # Remove header separator
        header_separator = cleaned.pop(1)
        if re.search(re.compile(r'^[\s\-\|]*$'), header_separator) == None:
            raise InvalidHeaderSeparatorError('Bad header separator: {}'.format(header_separator))

        # Unsplit
        cleaned = '\n'.join(cleaned)
        return cleaned

    def _build_from_markdown(self):
        cleaned = self._clean_markdown()
        str_df = pd.read_csv(io.StringIO(cleaned), sep='|', converters=self.schema.str_converters())

        df = pd.DataFrame()
        for header in list(str_df):
            if header in self.schema.keys():
                df[header] = str_df[header].apply(self.schema[header].in_converter)
            else:
                df[header] = str_df[header]
        return df

    def _fake_series(self, column, nsample=5):
        meta = self.schema[column]
        faker_func = self.fake_with.get(column, {}).get('valid') or default_fakers[meta['type']]

        if self.fake_with.get(column, {}).get('unique'):
            fake_data_dupes = [faker_func() for i in range(nsample * 3)]
            fake_data = list(set(fake_data_dupes))[0:nsample]
            random.shuffle(fake_data)
        else:
            fake_data = [faker_func() for i in range(nsample)]

        return pd.Series(fake_data)


    @property
    def df(self):
        if self.markdown:
            df = self._build_from_markdown()
        else:
            df = pd.DataFrame([], index=range(self.nrows))

        for column in self.schema.keys():
            if column in df:
                next
            else:
                df[column] = self._fake_series(column, len(df))
        return df



class MockPipe(pemi.Pipe):
    def flow(self):
        pemi.log().debug('FLOWING mocked pipe: {}'.format(self))
        pass


class PipeMocker():
    def __init__(self, pipe):
        self.pipe = pipe

    def mock_subject_data(self, subject, data=None):
        subject.data = data

    def mock_pipe_subject(self, subject):
        pipe_of_subject = subject.pipe

        mocked = MockPipe(name=pipe_of_subject.name)
        if subject.name in pipe_of_subject.targets:
            mocked.target(
                name=subject.name,
                schema=subject.schema
            )

        if subject.name in pipe_of_subject.sources:
            mocked.source(
                name=subject.name,
                schema=subject.schema
            )

        self.pipe.pipes[pipe_of_subject.name] = mocked


class Scenario():
    def __init__(self, runner, givens=[]):
        self.runner = runner
        self.givens = givens
        self.whens = []
        self.thens = []

    def when(self, *whens):
        self.whens = whens
        return self

    def then(self, *thens):
        self.thens = thens
        return self

    def run(self):
        for given in self.givens:
            given()
        for when in self.whens:
            when()
        self.runner()
        for then in self.thens:
            then()

        return self


class BasicRules():
    def __init__(self, source_subject, target_subject, mocker):
        self.source_subject = source_subject
        self.target_subject = target_subject
        self.mocker = mocker


    def when_source_conforms_to_schema(self):
        'The source data subject conforms to the schema'

        def _when_source_conforms_to_schema():
            data = TestTable(
                schema=self.source_subject.schema
            ).df

            self.mocker.mock_subject_data(self.source_subject, data)

        _when_source_conforms_to_schema.__doc__ = '''
            The source data subject {} conforms to the schema: {}
        '''.format(
            self.source_subject,
            self.source_subject.schema
        )

        return _when_source_conforms_to_schema


    def then_field_is_copied(self, source_name, target_name):
        def _then_field_is_copied():
            given = self.source_subject.data
            actual = self.target_subject.data
            pd.testing.assert_series_equal(given[source_name], actual[target_name])


        _then_field_is_copied.__doc__ = '''
            The field {} from the source {} is copied to field {} on the target {}
        '''.format(
            self.source_subject,
            source_name,
            self.target_subject,
            target_name
        )
        return _then_field_is_copied

    def then_fields_are_copied(self, fields):
        return [self.then_field_is_copied(source, target) for source, target in fields.items()]

    def when_source_field_has_value(self, field_name, field_value):
        # This is definitely pandas specific, so perhaps we wrap up these conditions
        # in data type specific modules.

        def _when_source_field_has_value():
            self.source_subject.data[field_name] = pd.Series([field_value] * len(self.source_subject.data))

        _when_source_field_has_value.__doc__ = '''
            The source field '{}' has the value "{}"
        '''.format(self.source_subject, field_value)
        return _when_source_field_has_value

    def then_target_field_has_value(self, field_name, field_value):
        def _then_target_field_has_value():
            pd.testing.assert_series_equal(self.target_subject.data[field_name], pd.Series([field_value] * len(self.target_subject.data)), check_names=False)

        _then_target_field_has_value.__doc__ = '''
            The target field '{}' has the value "{}"
        '''.format(self.target_subject, field_value)
        return _then_target_field_has_value

    def when_example_for_source(self, table):
        def _when_example_for_source():
            self.mocker.mock_subject_data(self.source_subject, table.df)

        _when_example_for_source.__doc__ = '''
            The following example for '{}':
            {}
        '''.format(self.source_subject, table.df[table.defined_fields])
        return _when_example_for_source

    def then_target_matches_example(self, expected_table):
        subject_fields = expected_table.defined_fields
        def _then_target_matches_example():
            expected = expected_table.df[subject_fields]
            actual = self.target_subject.data[subject_fields]
            pd.testing.assert_frame_equal(actual, expected)

        _then_target_matches_example.__doc__ = '''
            The target '{}' matches the example:
        '''.format(self.target_subject, expected_table.df[subject_fields])
        return _then_target_matches_example
