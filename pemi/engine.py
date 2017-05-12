class Engine():
    ''' Engines store data in some format, and know how to convert to other engines '''
    def __init__(self, schema):
        # Engines need to know the schema in order to do conversions to other engines
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
