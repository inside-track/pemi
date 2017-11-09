import re
from collections import OrderedDict

import pemi
import pemi.connections
from pemi.data_subject import PdDataSubject


class Pipe():
    '''
    A pipe is a parameterized collection of sources and targets which can be executed (flow).
    '''

    def __init__(self, *, name='self', **params):
        self.name = name
        self.params = params
        self.sources = OrderedDict()
        self.targets = OrderedDict()
        self.pipes = OrderedDict()
        self.connections = pemi.connections.PipeConnections()

        # TODOC: special case of "self" pipe
        self.pipes['self'] = self

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
        conn = pemi.connections.PipeConnection(self, from_pipe_name, from_subject_name)

        self.connections.append(conn)
        return conn

    def flow(self):
        raise NotImplementedError

    def __str__(self):
        return "<{}({}) {}>".format(self.__class__.__name__, self.name, id(self))
