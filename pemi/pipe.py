from collections import OrderedDict

import pemi
from pemi.data_subject import PdDataSubject

class PipeConnection():
    def __init__(self, parent, from_subject):
        self.parent = parent
        self.from_pipe_name = from_subject.pipe.name
        self.from_subject_name = from_subject.name

    def to(self, to_subject):
        self.to_pipe_name = to_subject.pipe.name
        self.to_subject_name = to_subject.name
        return self

    @property
    def from_pipe(self):
        return self.parent.pipes[self.from_pipe_name]

    @property
    def to_pipe(self):
        return self.parent.pipes[self.to_pipe_name]

    @property
    def from_subject(self):
        return self.from_pipe.targets[self.from_subject_name]

    @property
    def to_subject(self):
        return self.to_pipe.sources[self.to_subject_name]

    def connect(self):
        self.to_subject.connect_from(self.from_subject)

    def __str__(self):
        return 'PipeConnection: {}.{} -> {}.{}'.format(
            self.from_pipe,
            self.from_subject,
            self.to_pipe,
            self.to_subject
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

    def source(self, subject_class, name, schema=pemi.Schema(), **kwargs):
        self.sources[name] = subject_class(
            pipe=self,
            name=name,
            schema=schema,
            **kwargs
        )

    def target(self, subject_class, name, schema=pemi.Schema(), **kwargs):
        self.targets[name] = subject_class(
            pipe=self,
            name=name,
            schema=schema,
            **kwargs
        )

    def pipe(self, name, pipe):
        pipe.name = name
        self.pipes[name] = pipe


    def connect(self, connect_from):
        conn = PipeConnection(self, connect_from)

        self.connections.append(conn)
        return conn


    def flow(self):
        raise NotImplementedError

    def __str__(self):
        return "<{}({}) {}>".format(self.__class__.__name__, self.name, id(self))


class SourcePipe(Pipe):
    '''
    A source pipe extracts data from some external source, and parses
    it into a data structure that can be used by subsequent pipes.
    '''

    def __init__(self, subject_class=PdDataSubject, **params):
        super().__init__(**params)

        self.target(subject_class, name='main')
        self.target(subject_class, name='error')

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

    def __init__(self, subject_class=PdDataSubject, **params):
        super().__init__(**params)

        self.source(subject_class, name='main')
        self.target(subject_class, name='load_response')

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
