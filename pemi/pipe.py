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
