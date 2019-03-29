import re
import io
import random

import pandas as pd
from faker import Factory

import pemi
from pemi.fields import *


FAKER = Factory.create()

class UniqueIdGenerator: #pylint: disable=too-few-public-methods
    '''
    Class used to build id generators.
    fmt - A function that accepts a single integer argument and returns a value to be used as an id.

    Example:
      students = UniqueIdGenerator(lambda i: 'S{i}'.format(i))
      [next(students) for x in range(10)]
      #=> ['S6', 'S5', 'S3', 'S1', 'S4', 'S7', 'S9', 'S2', 'S8', 'S16']

    This generator also supports the call method, which operates the same as ``next``.

    Example:
      [UniqueIdGenerator()() for x in range(10)]
      #=> ['S6', 'S5', 'S3', 'S1', 'S4', 'S7', 'S9', 'S2', 'S8', 'S16']
    '''
    def __init__(self, fmt=int):
        self.fmt = fmt
        self.size = 1
        self.gen_sample()

    def gen_sample(self):
        self.sample = list(range(10**(self.size-1), 10**self.size))
        random.shuffle(self.sample)
        self.size += 1

    def __next__(self):
        i = self.sample.pop()
        if len(self.sample) == 0:
            self.gen_sample()
        return self.fmt(i)

    def __call__(self):
        return next(self)

class InvalidHeaderSeparatorError(Exception): pass


class Table: #pylint: disable=too-few-public-methods

    #pylint: disable=no-member
    @staticmethod
    def _fake_decimal(field):
        def digits(precision, scale):
            left_digits = FAKER.random_int(min=0, max=precision - scale)
            right_digits = FAKER.random_int(min=0, max=scale)

            if left_digits == right_digits == 0:
                left_digits = precision - scale
                right_digits = scale

            return {'left_digits': left_digits, 'right_digits': right_digits}

        return lambda: FAKER.pydecimal(**digits(field.precision, field.scale))

    DEFAULT_FAKERS = {
        IntegerField:  lambda field: FAKER.pyint,
        StringField:   lambda field: FAKER.word,
        DateField:     lambda field: FAKER.date_object,
        DateTimeField: lambda field: FAKER.date_time,
        FloatField:    lambda field: FAKER.pyfloat,
        DecimalField:  _fake_decimal.__func__,
        BooleanField:  lambda field: FAKER.pybool,
        JsonField:     lambda field: lambda: FAKER.pydict(5, True, 'str', 'int', 'date')
    }
    #pylint: enable=no-member

    def __init__(self, markdown=None, nrows=10,
                 schema=pemi.Schema(), coerce_with=None):
        self.markdown = markdown
        self.schema = schema
        self.nrows = nrows
        self.coerce_with = coerce_with or {}

        if self.markdown:
            self.defined_fields = list(self._build_from_markdown().columns)
        else:
            self.defined_fields = list(self.schema.keys())

    def _clean_markdown(self):
        cleaned = self.markdown

        # Remove trailing comments
        cleaned = re.compile(r'(#[^\|]*$)', flags=re.MULTILINE).sub('', cleaned)

        # Remove whitespace surrouding pipes
        cleaned = re.compile(r'[ \t]*\|[ \t]*').sub('|', cleaned)

        # Remove beginning and terminal pipe on each row
        cleaned = re.compile(r'(^\s*\|\s*|\s*\|\s*$)', flags=re.MULTILINE).sub('', cleaned)

        # Split by newlines
        cleaned = cleaned.split('\n')

        # Remove header separator
        header_separator = cleaned.pop(1)
        if re.search(re.compile(r'^[\s\-\|]*$'), header_separator) is None:
            raise InvalidHeaderSeparatorError('Bad header separator: {}'.format(header_separator))

        # Unsplit
        cleaned = '\n'.join(cleaned)
        return cleaned

    def _build_from_markdown(self):
        cleaned = self._clean_markdown()
        str_df = pd.read_csv(
            io.StringIO(cleaned),
            sep='|',
            converters={k:str for k in self.schema.keys()}
        )

        df = pd.DataFrame()
        for header in list(str_df):
            if header in self.coerce_with:
                df[header] = str_df[header].apply(self.coerce_with[header])
            elif header in self.schema.keys():
                df[header] = str_df[header].apply(self.schema[header].coerce)
            else:
                df[header] = str_df[header]
        return df

    def _fake_series(self, column, nsample=5):
        field = self.schema[column]

        default_faker = self.DEFAULT_FAKERS.get(type(field))(field)
        faker_func = field.metadata.get('faker', default_faker)
        fake_data = [faker_func() for i in range(nsample)]
        return pd.Series(fake_data)

    @property
    def df(self):
        if self.markdown:
            df = self._build_from_markdown()
        else:
            df = pd.DataFrame([], index=range(self.nrows))

        for column in self.schema.keys():
            if column not in df:
                df[column] = self._fake_series(column, len(df))
        return df
