class FilesResource:
    def __init__(self):
        pass

class Engine:
    def __init__(self):
        self.schema = None

    def to_pandas(self):
        raise NotImplementedError

    def from_pandas(self):
        raise NotImplementedError

    def to_spark(self):
        raise NotImplementedError

    def from_spark(self):
        raise NotImplementedError

    def __getitem__(self, key):
        raise NotImplementedError
