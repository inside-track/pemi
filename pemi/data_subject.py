import pandas as pd
import pemi
from pemi.decorators import cached_property

class DataSubject():
    '''
    A data subject is a parent class defined by a schema and interface class.
    '''

    def __init__(self, schema, interface_class):
        self.schema = schema
        self.interface_class = interface_class

    @cached_property
    def interface(self):
        '''
        The interface is constructed in a specific child class and describes
        how to interact with elements of data.
        '''
        return self.build_interface()

    def build_interface(self):
        ''' DataSources use the extracted data to build an interface '''
        raise NotImplementedError

# should every interface define a default engine?
    def __getitem__(self, key):
        ''' DataSubjects wrap access to data stored in an engine constructed by the interface '''
        return self.engine[key]

class DataSource(DataSubject):
    '''
    Data sources extract data from some interface and build the engine.
    '''

    # DataSources convert the data they have extracted to other engines
    def to_pandas(self):
        return self.interface.to_pandas()

    def to_spark(self):
        return self.interface.to_spark()

    def to_alchemy(self):
        return self.interface.to_alchemy()


class DataTarget(DataSubject):
    '''
    Data targets import data from an engine and load it via an interface
    '''

    def from_engine(self, engine):
        if engine.__class__ == pemi.engines.PandasEngine:
            self.interface.from_pandas(engine)
#        elif engine.__class__ == SparkEngine:
#            self.interface.from_spark(engine)
        else:
            raise NotImplementedError('Engine {} not recognized'.format(engine.__class__))

    def encode(self):
        ''' DataTargets encode the engine into a form that the target system can understand '''
        raise NotImplementedError

    def load(self):
        ''' DataTargets load data into the target system '''
        raise NotImplementedError
