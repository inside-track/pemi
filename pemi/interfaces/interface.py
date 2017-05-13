import pemi
import pemi.engines

class Interface():
    def __init__(self, schema, default_engine_class):
        self.schema = schema

        # Every interface should define a default engine class
        self.default_engine_class = default_engine_class

    # Interfaces know how to convert to engines (maybe not all of them)
    def to_engine(self, engine_class=None):
        engine_class = engine_class if engine_class != None else self.default_engine_class

        if engine_class == pemi.engines.PandasEngine:
            self.to_pandas()
        else:
            raise NotImplementedError

    def to_pandas(self):
        raise NotImplementedError

    # Interfaces know how to convert from engines (maybe not all of them)
    def from_engine(self, engine):
        raise NotImplementedError

    # Interfaces need to be able to get data out of a subject
    #  They may use a standard engine (e.g., Pandas) internally in order to do this
    def __getitem__(self, key):
        raise NotImplementedError
