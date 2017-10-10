import sqlalchemy as sa
import pandas as pd

import pemi
from pemi.pipes.patterns import SourcePipe

import pemi.pd_mapper

class SaSqlSourcePipe(SourcePipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.schema = self.params['schema']
        self.sql = self.params['sql']
        self.engine = self.params['engine']
        self.field_maps = pemi.pd_mapper.schema_maps(self.schema)

    def extract(self):
        pemi.log().info("Extracting '{}' via:\n{}".format(self.name, self.sql))
        with self.engine.connect() as conn:
            sql_df = pd.read_sql(self.sql, conn)

        return sql_df

    def parse(self, sql_df):
        pemi.log().info("Parsing '{}' results".format(self.name))
        mapper = pemi.pd_mapper.PdMapper(sql_df, maps = self.field_maps).apply()
        self.targets['main'].df = mapper.mapped_df
        self.targets['errors'].df = mapper.errors_df
        return self.targets['main'].df
