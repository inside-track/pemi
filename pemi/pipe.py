import re
from collections import OrderedDict

import pemi
from pemi.data_subject import PdDataSubject

class PipeConnection():
    def __init__(self, parent, from_pipe_name, from_subject_name):
        self.parent = parent
        self.from_pipe_name = from_pipe_name
        self.from_subject_name = from_subject_name
        self.group = None


    def to(self, to_pipe_name, to_subject_name):
        self.to_pipe_name = to_pipe_name
        self.to_subject_name = to_subject_name
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

    @property
    def from_self(self):
        return self.parent is self.from_pipe

    @property
    def to_self(self):
        return self.parent is self.to_pipe

    def connect(self):
        self.to_subject.connect_from(self.from_subject)

    def group_as(self, name):
        self.group = name
        return self

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

        # TODOC: special case of "self" pipe
        self.pipes['self'] = self

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


    def connect(self, from_pipe_name, from_subject_name):
        conn = PipeConnection(self, from_pipe_name, from_subject_name)

        self.connections.append(conn)
        return conn

    def connect_graph(self, graph, group_as=None):
        re_conn = re.compile(r'\s*(\w+)\[(\w+)\]\s*->\s*(\w+)\[(\w+)\]\s*')
        for conn_str in graph.split('\n'):
            if conn_str.strip() == '':
                continue

            matches = re.match(re_conn, conn_str)
            if matches:
                self.connect(matches[1], matches[2]).to(matches[3], matches[4]).group_as(group_as)
            else:
                raise ValueError('Unable to parse connection in graph, must be of the form "pipe1[subject1] -> pipe2[subject2]": {}'.format(conn_str))


    def flow(self):
        raise NotImplementedError

    def __str__(self):
        return "<{}({}) {}>".format(self.__class__.__name__, self.name, id(self))
