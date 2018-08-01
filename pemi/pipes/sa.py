import pandas as pd

import pemi

class SaSqlSourcePipe(pemi.Pipe):
    def __init__(self, *, sql, engine, schema=None, result=True, chunk_size=None):
        super().__init__()

        self.sql = sql
        self.engine = engine
        self.schema = schema
        self.result = result
        self.chunk_size = chunk_size

        self.target(
            pemi.PdDataSubject,
            name='main',
            schema=self.schema
        )


    def _get_result(self, conn):
        sql_df = pd.DataFrame()
        if self.chunk_size is None:
            sql_df = pd.read_sql(self.sql, conn)
        else:
            for chunk in pd.read_sql(self.sql, conn, chunksize=self.chunk_size):
                sql_df = sql_df.append(chunk, ignore_index=True)
        return sql_df


    def extract(self):
        pemi.log.info("Executing SQL '%s' via:\n%s", self.name, self.sql)

        data = None
        with self.engine.connect() as conn:
            if self.result:
                data = self._get_result(conn)
            else:
                conn.execute(self.sql)

        return data

    def parse(self, data):
        pemi.log.info("Parsing '%s' results", self.name)

        if data is None:
            return None

        if self.schema is None:
            self.targets['main'].df = data
        else:
            mapper = data.mapping(
                [(name, name, field.coerce) for name, field in self.schema.items()],
                on_error='raise'
            )
            self.targets['main'].df = mapper.mapped

        return self.targets['main'].df

    def flow(self):
        self.parse(self.extract())
