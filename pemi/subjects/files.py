from pemi import DataSource

class LocalFilesSource(DataSource):
    # Requires an engine class that accepts filepaths argument
    def __init__(self, schema, engine_class, path, **engine_opts):
        super().__init__(schema, engine_class)
        self.path = path
        self.engine_opts = engine_opts

    def build_engine(self):
        # TODO: add logic to query filepaths
        filepaths = self.path
        return self.engine_class(self.schema, filepaths, **self.engine_opts)
