import json

import pandas as pd
import sqlalchemy as sa

import pemi
from pemi.fields import *

__all__ = [
    'PdDataSubject',
    'SaDataSubject',
    'SparkDataSubject'
]

class MissingFieldsError(Exception): pass

class DataSubject:
    '''
    A data subject is mostly just a schema and a generic data object
    Actually, it's mostly just a schema that knows which pipe it belongs to (if any)
    and can be converted from and to a pandas dataframe (really only needed for testing to work)
    '''

    def __init__(self, schema=None, name=None, pipe=None):
        self.schema = schema or pemi.Schema()
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

    def connect_from(self, _other):
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
        raise MissingFieldsError('DataFrame missing expected fields: {}'.format(missing))

    def _empty_df(self):
        return pd.DataFrame(columns=self.schema.keys())

class SaDataSubject(DataSubject):
    def __init__(self, engine, table, sql_schema=None, **kwargs):
        super().__init__(**kwargs)
        self.engine = engine
        self.table = table
        self.sql_schema = sql_schema

        self.cached_test_df = None

    def to_pd(self):
        if self.cached_test_df is not None:
            return self.cached_test_df

        with self.engine.connect() as conn:
            sql_df = pd.read_sql_table(
                self.table,
                conn,
                schema=self.sql_schema,
                columns=self.schema.keys()
            )

            df = pd.DataFrame()
            for column in list(sql_df):
                df[column] = sql_df[column].apply(self.schema[column].coerce)

            self.cached_test_df = df
        return df

    def from_pd(self, df, **to_sql_opts):
        self.cached_test_df = df
        pemi.log.debug('loading SaDataSubject with:\n%s', self.cached_test_df)

        to_sql_opts['if_exists'] = to_sql_opts.get('if_exists', 'append')
        to_sql_opts['index'] = to_sql_opts.get('index', False)
        if self.sql_schema:
            to_sql_opts['schema'] = self.sql_schema

        df_to_sql = df.copy()
        for field in self.schema.values():
            if isinstance(field, JsonField):
                df_to_sql[field.name] = df_to_sql[field.name].apply(json.dumps)

        with self.engine.connect() as conn:
            df_to_sql.to_sql(self.table, conn, **to_sql_opts)

    def connect_from(self, _other):
        self.engine.dispose()
        self.validate_schema()

    def __getstate__(self):
        return (
            [],
            {
                'url': self.engine.url,
                'table': self.table,
                'sql_schema': self.sql_schema
            }
        )

    def __setstate__(self, state):
        _args, kwargs = state
        self.engine = sa.create_engine(kwargs['url'])
        self.table = kwargs['table']
        self.sql_schema = kwargs['sql_schema']



class SparkDataSubject(DataSubject):
    def __init__(self, spark, df=None, **kwargs):
        super().__init__(**kwargs)
        self.spark = spark
        self.df = df

        self.cached_test_df = None

    def to_pd(self):
        if self.cached_test_df is not None:
            return self.cached_test_df

        converted_df = self.df.toPandas()
        self.cached_test_df = pd.DataFrame()
        for column in list(converted_df):
            self.cached_test_df[column] = converted_df[column].apply(self.schema[column].coerce)

        return self.cached_test_df

    def from_pd(self, df, **kwargs):
        self.df = self.spark.createDataFrame(df)

    def connect_from(self, other):
        self.spark = other.spark.builder.getOrCreate()
        self.df = other.df
        self.validate_schema()
