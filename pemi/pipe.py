import re
import pickle
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


    def to_pickle(self, picklepipe=None):
        picklepipe = picklepipe or Pipe()

        for name, source in self.sources.items():
            picklepipe.source(source.__class__, name=name)
            picklepipe.sources[name] = source

        for name, target in self.targets.items():
            picklepipe.target(target.__class__, name=name)
            picklepipe.targets[name] = target

        for name, nestedpipe in self.pipes.items():
            if nestedpipe == self: continue

            nestedpicklepipe = Pipe()
            picklepipe.pipe(name=name, pipe=nestedpicklepipe)
            picklepipe.pipes[name] = nestedpipe.to_pickle(nestedpicklepipe)

        return pickle.dumps(picklepipe)


    def from_pickle(self, picklepipe=None):
        picklepipe = pickle.loads(picklepipe)

        for name, source in picklepipe.sources.items():
            self.sources[name] = source

        for name, target in picklepipe.targets.items():
            self.targets[name] = target

        for name, nestedpicklepipe in picklepipe.pipes.items():
            if nestedpicklepipe == picklepipe: continue

            self.pipes[name].from_pickle(nestedpicklepipe)

        return self


    def flow(self):
        raise NotImplementedError

    def __str__(self):
        return "<{}({}) {}>".format(self.__class__.__name__, self.name, id(self))
