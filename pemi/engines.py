class Engine():
    ''' Engines store data in some format, and know how to convert to other engines '''

    def __init__(self, schema):
        # Engines need to know the schema in order to do conversions to other engines
        # do they?  might be simpler if they weren't needed
        # could possibly solve schema issues through introspection of underlying data structures

        # Maybe I should scrap it for now until I know I need it.
        self.schema = schema

    # Engines describe how to get data out of the subject
    def __getitem__(self, key):
        raise NotImplementedError

    def to_pandas(self):
        raise NotImplementedError

    def to_spark(self):
        raise NotImplementedError

    def to_alchemy(self):
        raise NotImplementedError

    def to_engine(self, engine_class):
        # Could maybe make this generic detection of a to_* attribute
        raise NotImplementedError
        if engine_class == PandasEngine:
            self.to_pandas()
        # etc.

class PandasEngine(Engine):
    ''' Initialize a PandasEngine with a schema and a Pandas dataframe '''

    def __init__(self, schema, df, df_errors=None):
        super().__init__(schema)
        self.df = df
        self.df_errors = df_errors

    def to_pandas(self):
        return self

    def to_spark(self):
        # something that would convert this pandas dataframe into a spark dataframe
        raise NotImplementedError
