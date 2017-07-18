import pemi

class DataSubject():
    '''
    A data subject is just a schema and a generic data object
    '''

    def __init__(self, schema=pemi.Schema(), data=None):
        self.schema = pemi.Schema(schema)
        self.data = data
