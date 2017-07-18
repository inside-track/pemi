import pemi

from collections import OrderedDict

class Pipe():
    '''
    A pipe is a parameterized collection of sources and targets which can be executed.
    '''

    def __init__(self, **params):
        self.params = params
        self.sources = OrderedDict()
        self.targets = OrderedDict()

    def execute(self):
        raise NotImplementedError

class SourcePipe(Pipe):
    '''
    A source pipe extracts data from some external source, and parses
    it into a data structure that can be used by subsequent pipes.
    '''

    def __init__(self, **params):
        super().__init__(**params)

        self.targets['standard'] = pemi.DataSubject()
        self.targets['error'] = pemi.DataSubject()

    def extract(self):
        #e.g., S3SourceExtractor.extract()
        raise NotImplementedError
        return extracted_data_that_can_be_parsed

    def parse(self, data):
        #e.g., CsvParser.parse(data)
        raise NotImplementedError
        return parsed_data

    def execute(self):
        self.targets['standard'].data = self.parse(self.extract())
        return self.targets['standard'].data


class TargetPipe(Pipe):
    '''
    A target pipe takes data provided to it, encodes it into a structure that can be
    understand by some external target, and then loads the data into that external target.
    '''

    def __init__(self, **params):
        super().__init__(**params)

        self.sources['standard'] = pemi.DataSubject()
        self.targets['load_response'] = pemi.DataSubject()

    def encode(self):
        #e.g., CsvTargetEncoder.encode()
        raise NotImplementedError
        return source_standard_data_encoded_for_loader

    def load(self, encoded_data):
        #e.g., S3Loader.load()
        raise NotImplementedError
        return results_from_load_operation

    def execute(self):
        return self.load(self.encode())
