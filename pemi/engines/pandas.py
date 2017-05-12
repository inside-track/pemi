import pemi

class PandasEngine(pemi.engine.Engine):
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

    def __getitem__(self, key):
        return self.df[key]
