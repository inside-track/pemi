import json

import pandas as pd

import pemi
from pemi.fields import *

__all__ = [
    'PdDataSubject',
    'SaDataSubject',
    'SparkDataSubject'
]

class MissingFieldsError(Exception): pass

# TODOC: Note that to_pd and from_pd are only strictly needed for testing
class DataSubject:
    '''
    A data subject is mostly just a schema and a generic data object
    Actually, it's mostly just a schema that knows which pipe it belongs to (if any)
    and can be converted from and to a pandas dataframe (really only needed for testing to work)
    '''

    def __init__(self, schema=pemi.Schema(), data=None, name=None, pipe=None):
        self.schema = schema
        #TODO: I think I can get rid of self.data
        self.data = data
        self.name = name
        self.pipe = pipe

    def __str__(self):
        subject_str = '<{}({}) {}>'.format(self.__class__.__name__, self.name, id(self))
        if self.pipe:
            return '{}.{}'.format(self.pipe, subject_str)
        return subject_str

    def to_pd(self):
        raise NotImplementedError

    def from_pd(self, df, **kwargs):
        raise NotImplementedError

    def connect_from(self, other): #pylint: disable=unused-argument
        self.validate_schema()
        raise NotImplementedError

    def validate_schema(self): #pylint: disable=no-self-use
        return True


class PdDataSubject(DataSubject):
    def __init__(self, df=None, **kwargs):
        super().__init__(**kwargs)

        if df is None or df.shape == (0, 0):
            df = self._empty_df()
        self.df = df

    def to_pd(self):
        return self.df

    def from_pd(self, df, **kwargs):
        self.df = df

    def connect_from(self, other):
        if other.df is None or other.df.shape == (0, 0):
            self.df = self._empty_df()
        else:
            self.df = other.df
        self.validate_schema()

    def validate_schema(self):
        'Verify that the dataframe contains all of the columns specified in the schema'
        missing = set(self.schema.keys()) - set(self.df.columns)

        if len(missing) == 0:
            return True
        else:
            raise MissingFieldsError('DataFrame missing expected fields: {}'.format(missing))

    def _empty_df(self):
        return pd.DataFrame(columns=self.schema.keys())

class SaDataSubject(DataSubject):
    def __init__(self, engine, table, **kwargs):
        super().__init__(**kwargs)
        self.engine = engine
        self.table = table

    def to_pd(self):
        with self.engine.connect() as conn:
            sql_df = pd.read_sql(self.table, conn, columns=self.schema.keys())

            df = pd.DataFrame()
            for column in list(sql_df):
                df[column] = sql_df[column].apply(self.schema[column].coerce)

        return df

    # TODO: Need to figure out how to translate field data types into db data types
    def from_pd(self, df, **to_sql_opts):
        to_sql_opts['if_exists'] = to_sql_opts.get('if_exists', 'append')
        to_sql_opts['index'] = to_sql_opts.get('index', False)

        df_to_sql = df.copy()
        for field in self.schema.values():
            if isinstance(field, JsonField):
                df_to_sql[field.name] = df_to_sql[field.name].apply(json.dumps)

        with self.engine.connect() as conn:
            df_to_sql.to_sql(self.table, conn, **to_sql_opts)

    def connect_from(self, other):
        self.engine.dispose()
        self.validate_schema()



class SparkDataSubject(DataSubject):
    def __init__(self, spark, df=None, **kwargs):
        super().__init__(**kwargs)
        self.spark = spark
        self.df = df

    def to_pd(self):
        converted_df = self.df.toPandas()
        pd_df = pd.DataFrame()
        for column in list(converted_df):
            pd_df[column] = converted_df[column].apply(self.schema[column].coerce)

        return pd_df

    def from_pd(self, df, **kwargs):
        self.df = self.spark.createDataFrame(df)

    def connect_from(self, other):
        self.spark = other.spark.builder.getOrCreate()
        self.df = other.df
        self.validate_schema()
