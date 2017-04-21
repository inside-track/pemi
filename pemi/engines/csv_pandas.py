import pandas as pd

from pemi.decorators import cached_property
from pemi.engines.engine import Engine


# This might be easier if I started writing some feature tests

# Remember, Engines are used for both reading and writing
class CsvPandasEngine(Engine):
    def __init__(self, files_resource, read_opts={}, write_opts={}, **kargs):
        self.files_resource = files_resource
        self.user_read_opts = read_opts
        self.user_write_opts = write_opts

        self.error_df = pd.DataFrame()

    def read_opts(self):
        mandatory_opts = {
            'converters': self.schema.str_converters(),
            'usecols':    self.schema.keys()
        }

        default_opts = {
            'engine':          'c',
            'error_bad_lines': True
        }

        return {**default_opts, **self.user_read_opts, **mandatory_opts}

    def to_pandas(self):
        return self.df.copy()

    @cached_property
    def df(self):
        raw_df = pd.read_csv(self.files_resource, **self.read_opts())

        validated_df = pd.DataFrame()
        for name, field in self.schema.items():
            # OK, so now how do I deal with conversion errors
            validated_df[name] = raw_df[name].apply(field.in_converter)

        return typed_df


    def __getitem__(self, key):
        return self.df[key]
