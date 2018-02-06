import os
import re

import pandas as pd

import pemi
import pemi.pd_mapper
from pemi.pipes.patterns import SourcePipe
from pemi.pipes.patterns import TargetPipe

def default_column_normalizer(name):
    name = re.sub(r'\s+', '_', name).lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    return name


class LocalCsvFileSourcePipe(SourcePipe):
    def __init__(self, *, paths, csv_opts={},
                 filename_field=None, filename_full_path=False, normalize_columns=True, **params):
        super().__init__(**params)

        self.paths = paths
        self.filename_field = filename_field
        self.filename_full_path = filename_full_path
        self.csv_opts = self._build_csv_opts(csv_opts)

        #TODO: Tests for this
        if callable(normalize_columns):
            self.column_normalizer = normalize_columns
        elif normalize_columns:
            self.column_normalizer = default_column_normalizer
        else:
            self.column_normalizer = lambda col: col

        self.targets['main'].schema = self.schema
        self.field_maps = pemi.pd_mapper.schema_maps(self.schema)

    def extract(self):
        return self.paths

    def parse(self, data):
        filepaths = data
        mapped_dfs = []
        error_dfs = []
        for filepath in filepaths:
            parsed_dfs = self._parse_one(filepath)
            mapped_dfs.append(parsed_dfs.mapped_df)
            error_dfs.append(parsed_dfs.errors_df)

        if len(filepaths) > 0:
            self.targets['main'].df = pd.concat(mapped_dfs)
            self.targets['errors'].df = pd.concat(error_dfs)
        else:
            self.targets['main'].df = pd.DataFrame(columns=list(self.schema.keys()))
            self.targets['errors'].df = pd.DataFrame(columns=list(self.schema.keys()))

        return self.targets['main'].df

    def _build_csv_opts(self, user_csv_opts):
        file_fieldnames = [k for k in self.schema.keys() if k != self.filename_field]

        mandatory_opts = {
            # Assumes we'll never get a csv with > 10000 columns
            'converters': {idx:str for idx in range(10000)},
            'usecols':    lambda col: self.column_normalizer(col) in file_fieldnames
        }

        default_opts = {
            'engine':          'c',
            'error_bad_lines': True
        }

        return {**default_opts, **user_csv_opts, **mandatory_opts}

    def _parse_one(self, filepath):
        raw_df = pd.read_csv(filepath, **self.csv_opts)
        raw_df.columns = [self.column_normalizer(col) for col in raw_df.columns]

        if self.filename_field:
            if self.filename_full_path:
                raw_df[self.filename_field] = filepath
            else:
                raw_df[self.filename_field] = os.path.basename(filepath)
        mapper = pemi.pd_mapper.PdMapper(raw_df, maps=self.field_maps).apply()
        return mapper


class LocalCsvFileTargetPipe(TargetPipe):
    def __init__(self, *, path, csv_opts={}, **params):
        super().__init__(**params)

        self.path = path
        self.csv_opts = self._build_csv_opts(csv_opts)

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
