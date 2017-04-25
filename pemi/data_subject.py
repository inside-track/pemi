import pandas as pd
from pemi.decorators import cached_property

class DataSubject():
    '''
    A data subject is a parent class defined by a schema and engine class.
    '''

    def __init__(self, schema, engine_class):
        self.schema = schema
        self.engine_class = engine_class

    @cached_property
    def engine(self):
        '''
        The engine is constructed in a specific child class and describes
        how to get elements of data.
        '''
        raise NotImplementedError

    def __getitem__(self, key):
        ''' DataSubjects wrap access to data stored in the engine '''
        return self.engine[key]

class DataSource(DataSubject):
    '''
    Data sources extract data from some thing and build the engine.
    '''

    @cached_property
    def engine(self):
        return self.build_engine()

    def build_engine(self):
        ''' DataSources use the extracted data to build an engine '''
        raise NotImplementedError

    # DataSources convert the data they have extracted to other engines
    def to_pandas(self):
        return self.engine.to_pandas()

    def to_spark(self):
        return self.engine.to_spark()

    def to_alchemy(self):
        return self.engine.to_alchemy()


class DataTarget(DataSubject):
    @cached_property
    def engine(self):
        '''
        The engine is constructed in a specific child class and describes
        how to get elements of data.
        '''
        raise AttributeError('Engine has not been defined')

    # DataTargets read data from some type of engine
    def from_pandas(self, df):
        self.engine = PandasEngine(df).to_engine(self.engine_class)

    def from_spark(self, df):
        self.engine = SparkEngine(df).to_engine(self.engine_class)

    def from_alchemy(self, df):
        self.engine = AlchemyEngine(df).to_engine(self.engine_class)

    def encode(self):
        ''' DataTargets encode the engine into a form that the target system can understand '''
        raise NotImplementedError

    def load(self):
        ''' DataTargets load data into the target system '''
        raise NotImplementedError
