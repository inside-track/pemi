import pemi
from pemi import SourcePipe
from pemi import TargetPipe

from pemi.field_map import FieldMap
from pemi.field_map import Handler
from pemi.mappers.pandas import PandasMapper

import pandas as pd

class LocalCsvFileSourcePipe(SourcePipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.schema = pemi.Schema(params['schema'])
        self.paths = params['paths']
        self.csv_opts = self._build_csv_opts(params.get('csv_opts', {}))

        self.targets['main'].schema = self.schema
        self.field_maps = self._build_field_maps()

    def extract(self):
        return self.paths

    def parse(self, filepaths):
        mapped_dfs = []
        error_dfs = []
        for filepath in filepaths:
            parsed_dfs = self._parse_one(filepath)
            mapped_dfs.append(parsed_dfs.mapped_df)
            error_dfs.append(parsed_dfs.errors_df)

        self.targets['main'].data = pd.concat(mapped_dfs)
        self.targets['error'].data = pd.concat(error_dfs)

        return self.targets['main'].data

    def _build_csv_opts(self, user_csv_opts):
        mandatory_opts = {
            'converters': self.schema.str_converters(),
            'usecols':    self.schema.keys()
        }

        default_opts = {
            'engine':          'c',
            'error_bad_lines': True
        }

        return {**default_opts, **user_csv_opts, **mandatory_opts}

    def _build_field_maps(self):
        field_maps = []
        for name, field in self.schema.items():
            fm = FieldMap(source=name, target=name, transform=field.in_converter, handler=Handler('catch'))
            field_maps.append(fm)

        return field_maps

    def _parse_one(self, filepath):
        raw_df = pd.read_csv(filepath, **self.csv_opts)
        mapper = PandasMapper(raw_df, *self.field_maps)
        mapper.execute(raise_errors=False)
        return mapper


class LocalCsvFileTargetPipe(TargetPipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.schema = pemi.Schema(params['schema'])
        self.path = params['path']
        self.csv_opts = self._build_csv_opts(params.get('csv_opts', {}))

    def encode(self):
        return self.sources['main'].data

    def load(self, df):
        df.to_csv(self.path, **self.csv_opts)
        return self.path

    def _build_csv_opts(self, user_csv_opts):
        mandatory_opts = {}

        default_opts = {
            'index': False
        }

        return {**default_opts, **user_csv_opts, **mandatory_opts}
