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
