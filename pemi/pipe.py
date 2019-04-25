import pickle
import copy
from collections import OrderedDict

import pemi
import pemi.connections


class Pipe:
    '''
    A pipe is a parameterized collection of sources and targets which can be executed (flow).


    Args:
        name (str): Assign a name to the pipe
        **params: Additional keyword parameters

    Attributes:
        name (str): The name of the pipe.
        sources (dict): A dictionary where the keys are the names of source data subjects\
                        and the values are instances of a data subject class.
        targets (dict): A dictionary where the keys are the names of target data subjects\
                        and the values are instances of a data subject class.
        pipes (dict): A dictionary referencing nested pipes where the keys are the names\
                      of the nested pipes and the values are the nested pipe instances.
                      All pipes come with at least one nested pipe called 'self'.
        connections (PipeConnection): Pemi connection object.

    '''

    def __init__(self, *, name='self', **params):

        self.name = name
        self.params = params
        self.sources = OrderedDict()
        self.targets = OrderedDict()
        self.pipes = OrderedDict()
        self.connections = pemi.connections.PipeConnections()

        self.pipes['self'] = self

    def source(self, subject_class, name, schema=None, **kwargs):
        '''
        Define a source data subject for this pipe.

        Args:
            subject_class (class): The :class:`DataSubject` class this source uses.
            name (str) : Name of this data subject.
            schema (schema): :class:`Schema` associated with this source.

        Example:
            Creating a source::

                class MyPipe(pemi.Pipe):
                    def __init__(self):
                        super().__init__()

                        self.source(
                            pemi.PdDataSubject,
                            name='main'
                        )

            >>> MyPipe().sources.keys()
            ['main']
        '''

        schema = schema or pemi.Schema()
        self.sources[name] = subject_class(
            pipe=self,
            name=name,
            schema=schema,
            **kwargs
        )

    def target(self, subject_class, name, schema=None, **kwargs):
        '''
        Define a target data subject for this pipe.

        Args:
            subject_class (class): The :class:`DataSubject` class this target uses.
            name (str) : Name of this data subject.
            schema (schema): :class:`Schema` associated with this target.

        Example:
            Creating a target::

                class MyPipe(pemi.Pipe):
                    def __init__(self):
                        super().__init__()

                        self.target(
                            pemi.PdDataSubject,
                            name='main'
                        )

            >>> MyPipe().targets.keys()
            ['main']
        '''

        schema = schema or pemi.Schema()
        self.targets[name] = subject_class(
            pipe=self,
            name=name,
            schema=schema,
            **kwargs
        )

    def pipe(self, name, pipe):
        '''
        Defines a named pipe nested in this pipe.

        Args:
            name (str): Name of the nested pipe.
            pipe (pipe): The nested pipe instance.

        Example:
            Creating a target::

                class MyPipe(pemi.Pipe):
                    def __init__(self):
                        super().__init__()

                        self.pipe(
                            name='nested',
                            pipe=pemi.Pipe()
                        )

            >>> MyPipe().pipes.keys()
            ['self', 'nested']
        '''

        pipe.name = name
        self.pipes[name] = pipe


    def connect(self, from_pipe_name, from_subject_name):
        '''
        Connect one nested pipe to another

        Args:
            from_pipe_name (str): Name of the nested pipe that contains the\
                source of the connection.
            from_subject_name (str): Name of the data subject in the nested pipe that\
                contains the source of the connection.  This data subject needs to be a *target*
                of the pipe referenced by `from_pipe_name`.

        Returns:
            :class:`PipeConnection`: the PipeConnection object.

        Example:
            Connecting the target of one pipe to the source of another::

                class MyPipe(pemi.Pipe):
                    def __init__(self):
                        super().__init__()

                        self.pipe(
                            name='get_awesome_data',
                            pipe=GetAwesomeDataPipe()
                        )
                        self.connect('get_awesome_data', 'main').to('load_awesome_data', 'main')

                        self.pipe(
                            name='load_awesome_data',
                            pipe=LoadAwesomeDataPipe()
                        )

        '''

        conn = pemi.connections.PipeConnection(self, from_pipe_name, from_subject_name)

        self.connections.append(conn)
        return conn


    def to_pickle(self, picklepipe=None):
        '''
        Recursively pickle all of the data subjects in this and all nested pipes

        Args:
            picklepipe: A pickled representation of a pipe.  Only used for recursion\
                           not meant to be set by user.

        Returns:
            bytes: A bytes object containing the pickled pipe.

        Example:
            Pickling a pipe:

            >>> MyPipe.to_pickle()
            b'.. <bytes> ..'


        '''
        picklepipe = picklepipe or Pipe()

        for name, source in self.sources.items():
            psource = copy.copy(source)
            psource.pipe = picklepipe
            picklepipe.sources[name] = psource

        for name, target in self.targets.items():
            ptarget = copy.copy(target)
            ptarget.pipe = picklepipe
            picklepipe.targets[name] = ptarget

        for name, nestedpipe in self.pipes.items():
            if nestedpipe == self: continue

            nestedpicklepipe = Pipe()
            picklepipe.pipe(name=name, pipe=nestedpicklepipe)
            picklepipe.pipes[name] = nestedpipe.to_pickle(nestedpicklepipe)

        return pickle.dumps(picklepipe)



    def from_pickle(self, picklepipe=None):
        '''
        Recursively load all data subjects in all nested pipes from a pickled bytes object
        created by `to_pickle`.

        Args:
            picklepipe: The bytes object created by `to_pickle`

        Returns:
            self:

        Example:
            De-pickling a pickled pipe::

                my_pipe = MyPipe()
                pickled = my_pipe.to_pickle()

                my_other_pipe = MyPipe().from_pickle(pickled)
        '''

        picklepipe = pickle.loads(picklepipe)

        for name, source in picklepipe.sources.items():
            source.pipe = self
            self.sources[name] = source

        for name, target in picklepipe.targets.items():
            target.pipe = self
            self.targets[name] = target

        for name, nestedpicklepipe in picklepipe.pipes.items():
            if nestedpicklepipe == picklepipe: continue

            self.pipes[name].from_pickle(nestedpicklepipe)

        return self


    def flow(self):
        '''
        Execute this pipe.  This method is meant to be defined in a child class.

        Example:
            A simple hello-world pipe::

                class MyPipe(pemi.Pipe):
                    def flow(self):
                        print('hello world')

            >>> MyPipe().flow()
            'hello world'
        '''

        raise NotImplementedError

    def __str__(self):
        return "<{}({}) {}>".format(self.__class__.__name__, self.name, id(self))


class MockPipe(Pipe):
    def flow(self):
        pemi.log.debug('FLOWING mocked pipe: %s', self)

def mock_pipe(parent_pipe, pipe_name):
    pipe = parent_pipe.pipes[pipe_name]
    mocked = MockPipe(name=pipe.name)
    for source in pipe.sources:
        mocked.sources[source] = pipe.sources[source]
        mocked.sources[source].pipe = mocked

    for target in pipe.targets:
        mocked.targets[target] = pipe.targets[target]
        mocked.targets[target].pipe = mocked

    parent_pipe.pipes[pipe_name] = mocked
