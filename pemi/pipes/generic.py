import pemi

from collections import OrderedDict

class PipeConnection():
    def __init__(self, pipe, target):
        self.from_pipe = pipe
        self.from_subject = target

    def to(self, pipe, source):
        self.to_pipe = pipe
        self.to_subject = source
        return self

    def connect(self):
        self.to_pipe.sources[self.to_subject] = self.from_pipe.targets[self.from_subject]

    def __str__(self):
        return 'PipeConnection: {from_pipe}[{from_subject}] -> {to_pipe}[{to_subject}]'.format(
            from_pipe=self.from_pipe,
            from_subject=self.from_subject,
            to_pipe=self.to_pipe,
            to_subject=self.to_subject
        )

    def __repr__(self):
        return '<{}>'.format(self.__str__())


class Pipe():
    '''
    A pipe is a parameterized collection of sources and targets which can be executed (flow).
    '''

    def __init__(self, name=None, **params):
        self.name = name
        self.params = params
        self.sources = OrderedDict()
        self.targets = OrderedDict()
        self.pipes = OrderedDict()
        self.connections = []
        self.config()

    def config(self):
        'Override this to configure attributes of specific pipes (sources, targets, connections, etc)'
        pass

    def source(self, name, schema=pemi.Schema()):
        self.sources[name] = pemi.DataSource(
            name=name,
            schema=schema
        )

    def target(self, name, schema=pemi.Schema()):
        self.targets[name] = pemi.DataTarget(
            name=name,
            schema=schema
        )

    def pipe(self, name, pipe):
        pipe.name = name
        self.pipes[name] = pipe


    def connect(self, pipe, target):
        conn = PipeConnection(pipe, target)
        self.connections.append(conn)
        return conn


    def flow(self):
        raise NotImplementedError

    def __str__(self):
        return "<{}({})>".format(self.__class__.__name__, self.name)


class SourcePipe(Pipe):
    '''
    A source pipe extracts data from some external source, and parses
    it into a data structure that can be used by subsequent pipes.
    '''

    def __init__(self, **params):
        super().__init__(**params)

        self.target(name='main')
        self.target(name='error')

    def extract(self):
        #e.g., S3SourceExtractor.extract()
        raise NotImplementedError
        return extracted_data_that_can_be_parsed

    def parse(self, data):
        #e.g., CsvParser.parse(data)
        raise NotImplementedError
        return parsed_data

    def flow(self):
        self.targets['main'].data = self.parse(self.extract())
        return self.targets['main'].data


class TargetPipe(Pipe):
    '''
    A target pipe takes data provided to it, encodes it into a structure that can be
    understand by some external target, and then loads the data into that external target.
    '''

    def __init__(self, **params):
        super().__init__(**params)

        self.source(name='main')
        self.target(name='load_response')

    def encode(self):
        #e.g., CsvTargetEncoder.encode()
        raise NotImplementedError
        return source_main_data_encoded_for_loader

    def load(self, encoded_data):
        #e.g., S3Loader.load()
        raise NotImplementedError
        return results_from_load_operation

    def flow(self):
        return self.load(self.encode())
