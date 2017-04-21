import pandas as pd

class DataSubject():
    def __init__(self, schema, engine, **kargs):
        self.schema = schema
        self.engine = engine
        self.engine.schema = schema

    def to_pandas(self):
        return self.engine.to_pandas()

    def __getitem__(self, key):
        return self.engine[key]

class DataSource(DataSubject):
    def __init__(self, schema, engine):
        super().__init__(schema=schema, engine=engine)

class DataTarget(DataSubject):
    def __init__(self, schema, engine):
        super().__init__(schema=schema, engine=engine)
