import pemi

class DataSubject():
    '''
    A data subject is just a schema and a generic data object
    '''

    def __init__(self, schema=pemi.Schema(), data=None, name=None, stype=None):
        self.schema = pemi.Schema(schema)
        self.data = data
        self.name = name
        self.stype = stype


    def __str__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.name)

class DataSource(DataSubject):
    def __init__(self, schema=pemi.Schema(), data=None, name=None):
        super().__init__(schema=schema, data=data, name=name, stype='source')


class DataTarget(DataSubject):
    def __init__(self, schema=pemi.Schema(), data=None, name=None):
        super().__init__(schema=schema, data=data, name=name, stype='target')
