import pandas as pd

import pemi
import pemi.pd_mapper
import pemi.pipes.patterns

class SaSqlSourcePipe(pemi.pipes.patterns.SourcePipe):
    def __init__(self, *, sql, engine, **params):
        super().__init__(**params)

        self.sql = sql
        self.engine = engine
        self.field_maps = pemi.pd_mapper.schema_maps(self.schema)

    def extract(self):
        pemi.log.info("Extracting '%s' via:\n%s", self.name, self.sql)
        chunk_size = self.params['chunksize'] if 'chunksize' in self.params else None
        sql_df = pd.DataFrame()
        with self.engine.connect() as conn:
            if chunk_size is None:
                sql_df = pd.read_sql(self.sql, conn)
            else:
                for chunk in pd.read_sql(self.sql, conn, chunksize=chunk_size):
                    sql_df = sql_df.append(chunk, ignore_index=True)

        return sql_df

    def parse(self, data):
        pemi.log.info("Parsing '%s' results", self.name)
        mapper = pemi.pd_mapper.PdMapper(data, maps=self.field_maps).apply()
        self.targets['main'].df = mapper.mapped_df
        self.targets['errors'].df = mapper.errors_df
        return self.targets['main'].df

    def flow(self):
        self.parse(self.extract())
