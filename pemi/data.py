import re
import io

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


class Table:
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

        # Remove whitespace surrouding pipes
        cleaned = re.compile(r'[ \t]*\|[ \t]*').sub('|', cleaned)

        # Remove beginning and terminal pipe on each row
        cleaned = re.compile(r'(^\s*\|\s*|\s*\|\s*$)', flags = re.MULTILINE).sub('', cleaned)

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
        faker_func = self.fake_with.get(column, {}).get('valid') or default_fakers[meta['ftype']]

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
