import pandas as pd

import pemi
from pemi import Handler
from pemi import FieldMap
from pemi.mappers.pandas import PandasMapper
from pemi.engines import PandasEngine

# This will be a parent class that we can use for regexing files we get from filesystems
class FilesInterface(pemi.interfaces.Interface):
    def __init__(self, schema, filepaths):
        super().__init__(schema, PandasEngine)
        self.filepaths = filepaths

class CsvFilesInterface(FilesInterface):
    def __init__(self, schema, filepaths, csv_opts={}, **kargs):
        super().__init__(schema, filepaths)

        self.user_csv_opts = csv_opts

    def to_pandas(self):
        #TODO: Make this work with an array of filepaths
        raw_df = pd.read_csv(self.filepaths, **self.pandas_reader_csv_opts())

        field_maps = []
        for name, field in self.schema.items():
            fm = FieldMap(source=name, target=name, transform=field.in_converter, handler=Handler('catch'))
            field_maps.append(fm)

        mapper = PandasMapper(raw_df, *field_maps)
        mapper.execute(raise_errors=False)

        return PandasEngine(self.schema, mapper.mapped_df, mapper.errors_df)

    def from_pandas(self, pandas_engine):
        df = pandas_engine.df

        # TODO: Figure out how to support single and multiple targets
        #   should probably be able to support splitting out to multiple
        #   targets based on some generator pattern.
        df.to_csv(self.filepaths, **self.pandas_writer_csv_opts())



    def pandas_reader_csv_opts(self):
        mandatory_opts = {
            'converters': self.schema.str_converters(),
            'usecols':    self.schema.keys()
        }

        default_opts = {
            'engine':          'c',
            'error_bad_lines': True
        }

        return {**default_opts, **self.user_csv_opts, **mandatory_opts}

    def pandas_writer_csv_opts(self):
        mandatory_opts = {
        }

        default_opts = {
            'index': False
        }

        return {**default_opts, **self.user_csv_opts, **mandatory_opts}


    # This is needed here so that when I have soemthing like
    #     source = SftpSource(schema, CsvFilesEngine, csv_opts={})
    # Then I can do
    #     source['my_field']
    # And use that for testing or whatever
    # However, CSV files wouldn't be able to that in their natural state.
    # So, we might have to convert this to pandas in order to test it.
    def __getitem__(self, key):
        memoized_this = self.to_pandas()
        return memoize_this[key]
