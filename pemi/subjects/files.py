from pemi import DataSource
from pemi import DataTarget

class LocalFilesSource(DataSource):
    # Requires an interface class that accepts filepaths argument
    def __init__(self, schema, interface_class, path, **interface_opts):
        super().__init__(schema, interface_class)
        self.path = path
        self.interface_opts = interface_opts

    def build_interface(self):
        # TODO: add logic to query filepaths
        filepaths = self.path
        return self.interface_class(self.schema, filepaths, **self.interface_opts)

class LocalFilesTarget(DataTarget):
    def __init__(self, schema, interface_class, path, **interface_opts):
        super().__init__(schema, interface_class)

        self.path = path
        self.interface_opts = interface_opts

    def build_interface(self):
        # TODO: add logic to query filepaths
        filepaths = self.path
        return self.interface_class(self.schema, filepaths, **self.interface_opts)
