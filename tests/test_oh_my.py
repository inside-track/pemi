import unittest
from pemi.decorators import cached_property

#DataSubject, Interfaces, Engines, Schemas, DataFrames, oh my!

# A data subject is defined by a schema and an engine
class DataSubject():
    def __init__(self, schema, engine_class):
        self.schema = schema
        self.engine_class = engine_class

    # The engine is constructed in a specific child class and describes how
    # to get elements of data.
    @cached_property
    def engine(self):
        raise NotImplementedError

    # DataSubjects get data from the engine
    def __getitem__(self, key):
        # Ok, this is supposed to be an instance of the engine, not the class.....
        return self.engine[key]


class DataSource(DataSubject):
    # DataSources extract data from some thing
    def extract(self):
        raise NotImplementedError

    @cached_property
    def engine(self):
        return self.build_engine()

    # DataSources parse the extracted data to build an engine
    def build_engine(self):
        raise NotImplementedError

    # DataSources convert the data they have extracted to other engines
    def to_pandas(self):
        self.engine.to_pandas()

    def to_spark(self):
        self.engine.to_spark()

    def to_alchemy(self):
        self.engine.to_alchemy()

class DataTarget(DataSubject):
    # DataTargets read data from some type of engine
    def from_pandas(self, df):
        self.engine = PandasEngine(df).to_engine(self.engine_class)

    def from_spark(self, df):
        self.engine = SparkEngine(df).to_engine(self.engine_class)

    def from_alchemy(self, df):
        self.engine = AlchemyEngine(df).to_engine(self.engine_class)

    # DataTargets encode the engine into a form that the target system can understand
    def encode(self):
        raise NotImplementedError

    # DataTargets load data into the target system
    def load(self):
        raise NotImplementedError

# # # A specific data source inherits from the DataSource class
# # #  and defines the engine
# # class CsvPandasSource(DataSubject):
# #     def __init__(self, schema, files, csv_opts={}):
# #         super().__init__(schema, PandasEngine)
# #         self.files = files
# #         self.csv_opts = csv_opts

# #     def extract(self):
# #         # The stuff that extracts the csv files
# #         raise NotImplementedError

# #     def build_engine(self):
# #         # the stuff that reads the csv, builds the pandas dataframe and then...
# #         raise NotImplementedError

# #     @cached_property
# #     def engine(self):
# #         df = self.build_engine()
# #         PandasEngine(df)

# # # A specific data target inherits from the DataTarget class
# # class CsvPandasTarget(DataTarget):
# #     def __init__(self, schema, files, csv_opts={}):
# #         super().__init__(schema, PandasEngine)
# #         self.files = files
# #         self.csv_opts = csv_opts

# #     # DataTargets encode the engine into a form that the target system can understand
# #     def encode(self):
# #         # Stuff that converts the engine (now a PandasEngine) into a csv file
# #         raise NotImplementedError

# #     # DataTargets load data into the target system
# #     def load(self):
# #         # Stuff that puts the csv into the target system
# #         # I think that being able to put the csv files into S3, SFTP, or local should
# #         # be handled by self.files or some equivalent
# #         raise NotImplementedError






# Engines describe how the data is stored internally and how to convert to other engines
class Engine():
    # Engines need to know the schema in order to do conversions to other engines
    def __init__(self, schema):
        self.schema = schema

    # Engines describe how to get data out of the subject
    def __getitem__(self, key):
        raise NotImplementedError

    # Engines describe conversions
    def to_pandas(self):
        raise NotImplementedError

    def to_spark(self):
        raise NotImplementedError

    def to_alchemy(self):
        raise NotImplementedError

    def to_engine(self, engine_class):
        if engine_class == PandasEngine:
            self.to_pandas()
        # etc.


# class PandasEngine(Engine):
#     def __init__(self, schema, df):
#         super().__init__(schema)
#         self.df = df

#     def to_pandas(self):
#         self.df.copy()

#     def to_spark(self):
#         # something that woudl convert this pandas dataframe into a spark dataframe
#         raise NotImplementedError

#     def __getitem__(self, key):
#         return self.df[key]


# ###
# # Ooh, what if CSV files were engines?
# class CsvFilesEngine(Engine):
#     def __init__(self, schema, path, csv_opts={}):
#         self.path = path
#         self.csv_opts = csv_opts

#     def to_pandas(self):
#         # the stuff that reads the csv, builds the pandas dataframe, does data conversions
#         # ..
#         return (converted_df, errors_df)

#     # This is needed here so that when I have soemthing like
#     #     source = SftpSource(schema, CsvFilesEngine, csv_opts={})
#     # Then I can do
#     #     source['my_field']
#     # And use that for testing or whatever
#     # However, CSV files wouldn't be able to that in their natural state.
#     # So, we might have to convert this to pandas in order to test it.
#     def __getitem__(self, key):
#         memoized_this = self.to_pandas()
#         return memoize_this[key]

# # We could then use an SftpSource to get the files
# # SftpSource(schema, CsvFilesEngine, csv_opts={})
# # SftpSource(schema, ParquetFilesEngine)
# #
# # Similarly, for other sources:
# # S3Source(schema, CsvFilesEngine, csv_opts={})
# # S3Source(schema, ParquetFilesEngine)

# class SftpSource(DataSource):
#     def __init__(self, schema, engine_class, **engine_args)
#         super().__init__(schema, engine.__class__)
#         self.engine_args = engine_args

#     # DataSources extract data from some thing
#     def extract(self):
#         # The stuff that downloads files to a temporary location
#         raise NotImplementedError

#     # DataSources parse the extracted data to build an engine
#     def build_engine(self):
#         # The stuff that creates the CsvFilesEngine that later converts to a dataframe
#         # self.engine = engine_class(**self.engine_args)
#         raise NotImplementedError



class DummyDataSource(DataSource):
    def build_engine(self):
        self.engine = 'dummyEngine'
        return self.engine

class TestDataSubject(unittest.TestCase):
    def test_engine_raises_when_not_defined(self):
        ds = DataSubject(None, None)
        self.assertRaises(NotImplementedError, lambda: ds.engine)

    def test_engine_ok_when_set(self):
        ds = DataSubject(None, None)
        ds.engine = 'myEngine'
        self.assertEqual(ds.engine, 'myEngine')

    def test_engine_ok_when_built(self):
        ds = DummyDataSource(None, None)
        ds.build_engine()
        self.assertEqual(ds.engine, 'dummyEngine')

    def test_source_builds_engine(self):
        ds = DummyDataSource(None, None)
        self.assertEqual(ds.engine, 'dummyEngine')
