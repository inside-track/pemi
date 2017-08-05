import pemi

class DataSubject():
    '''
    A data subject is mostly just a schema and a generic data object
    '''

    def __init__(self, schema=pemi.Schema(), data=None, name=None, stype=None, pipe=None):
        self.schema = pemi.Schema(schema)
        self.data = data
        self.name = name
        self.stype = stype
        self.pipe = pipe

    def __str__(self):
        subject_str = '<{}({}) {}>'.format(self.__class__.__name__, self.name, id(self))
        if self.pipe:
            return '{}.{}'.format(self.pipe, subject_str)
        else:
            return subject_str


class DataSource(DataSubject):
    def __init__(self, **kwargs):
        super().__init__(stype='source', **kwargs)


class DataTarget(DataSubject):
    def __init__(self, **kwargs):
        super().__init__(stype='target', **kwargs)
