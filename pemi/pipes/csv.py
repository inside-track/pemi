import os
import re

import pandas as pd

import pemi
from pemi.pipes.patterns import TargetPipe

def default_column_normalizer(name):
    name = str(name)
    name = re.sub(r'\s+', '_', name).lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    return name


class StrConverter(dict):
    def __contains__(self, key):
        return True

    def __getitem__(self, key):
        return str

class LocalCsvFileSourcePipe(pemi.Pipe):
    def __init__(self, *, paths, schema=None, csv_opts=None,
                 filename_field=None, filename_full_path=False, normalize_columns=True, **params):
        super().__init__(**params)

        self.paths = paths
        self.schema = schema
        self.filename_field = filename_field
        self.filename_full_path = filename_full_path
        self.csv_opts = self._build_csv_opts(csv_opts or {})

        if callable(normalize_columns):
            self.column_normalizer = normalize_columns
        elif normalize_columns:
            self.column_normalizer = default_column_normalizer
        else:
            self.column_normalizer = lambda col: col

        self.target(
            pemi.PdDataSubject,
            name='main',
            schema=self.schema
        )

        self.target(
            pemi.PdDataSubject,
            name='errors',
            schema=self.schema
        )

    def extract(self):
        return self.paths

    def parse(self, data):
        pemi.log.debug('Parsing files at %s', data)

        filepaths = data
        mapped_dfs = []
        error_dfs = []
        for filepath in filepaths:
            parsed_dfs = self._parse_one(filepath)
            mapped_dfs.append(parsed_dfs.mapped)
            error_dfs.append(parsed_dfs.errors)

        if len(filepaths) > 0:
            self.targets['main'].df = pd.concat(mapped_dfs, sort=False)
            self.targets['errors'].df = pd.concat(error_dfs, sort=False)
        elif self.schema:
            self.targets['main'].df = pd.DataFrame(columns=list(self.schema.keys()))
            self.targets['errors'].df = pd.DataFrame(columns=list(self.schema.keys()))
        else:
            self.targets['main'].df = pd.DataFrame()
            self.targets['errors'].df = pd.DataFrame()

        pemi.log.debug('Parsed %i records', len(self.targets['main'].df))
        return self.targets['main'].df

    def _build_csv_opts(self, user_csv_opts):
        if self.schema:
            file_fieldnames = [k for k in self.schema.keys() if k != self.filename_field]
            usecols = lambda col: self.column_normalizer(col) in file_fieldnames
        else:
            usecols = None

        mandatory_opts = {
            'converters': StrConverter(),
            'usecols': usecols
        }

        default_opts = {
            'engine':          'c',
            'error_bad_lines': True
        }

        return {**default_opts, **user_csv_opts, **mandatory_opts}

    def _parse_one(self, filepath):
        pemi.log.debug('Parsing file at %s', filepath)

        raw_df = pd.read_csv(filepath, **self.csv_opts)
        pemi.log.debug('Found %i raw records', len(raw_df))

        raw_df.columns = [self.column_normalizer(col) for col in raw_df.columns]

        if self.filename_field:
            if self.filename_full_path:
                raw_df[self.filename_field] = filepath
            else:
                raw_df[self.filename_field] = os.path.basename(filepath)

        if self.schema:
            return raw_df.mapping(
                [(name, name, field.coerce) for name, field in self.schema.items()],
                on_error='redirect'
            )
        return raw_df.mapping([], inplace=True)

    def flow(self):
        self.parse(self.extract())


class LocalCsvFileTargetPipe(TargetPipe):
    def __init__(self, *, path, csv_opts=None, **params):
        super().__init__(**params)

        self.path = path
        self.csv_opts = self._build_csv_opts(csv_opts or {})

    def encode(self):
        return self.sources['main'].df

    def load(self, encoded_data):
        df = encoded_data
        df.to_csv(self.path, **self.csv_opts)
        return self.path

    @staticmethod
    def _build_csv_opts(user_csv_opts):
        mandatory_opts = {}

        default_opts = {
            'index': False
        }

        return {**default_opts, **user_csv_opts, **mandatory_opts}
