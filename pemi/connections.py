from collections import defaultdict

import dask
import dask.dot

import pemi

class DagValidationError(Exception): pass

class PipeConnection:
    def __init__(self, parent, from_pipe_name, from_subject_name):
        self.parent = parent
        self.from_pipe_name = from_pipe_name
        self.from_subject_name = from_subject_name
        self.group = None
        self.to_pipe_name = None
        self.to_subject_name = None


    def to(self, to_pipe_name, to_subject_name): #pylint: disable=invalid-name
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
        if self.from_pipe_name == 'self':
            return self.from_pipe.sources[self.from_subject_name]

        return self.from_pipe.targets[self.from_subject_name]

    @property
    def to_subject(self):
        if self.to_pipe_name == 'self':
            return self.to_pipe.targets[self.to_subject_name]

        return self.to_pipe.sources[self.to_subject_name]

    @property
    def is_from_self(self):
        return self.parent is self.from_pipe

    @property
    def is_to_self(self):
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

class DaskPipe: #pylint: disable=too-few-public-methods
    'DaskPipe is just a wrapper around Pipe that allows us to use the Pipes in the context of Dask'

    def __init__(self, pipe, flow=True):
        self.pipe = pipe
        self.flow = flow

    def __call__(self, *args):
        pemi.log.info('DaskPipe flowing pipe %s', self.pipe)
        if self.flow:
            self.pipe.flow()
        return self

    def __str__(self):
        return self.pipe.name


class PipeConnections:
    def __init__(self, connections=None):
        self.connections = connections or []

    def append(self, conn):
        self.connections.append(conn)

    def group(self, group):
        return self.__class__([c for c in self.connections if c.group == group])

    def flow(self, dask_get=dask.get):
        dask_dag = self.dask_dag()
        return dask_get(dask_dag, list(dask_dag.keys()))


    def graph(self):
        return self.dask_graph()

    def dask_graph(self):
        return dask.dot.dot_graph(self.dask_dag(), rankdir='TB')

    def dask_dag(self):
        self.validate_dag()
        dag = {}
        for conn in self.connections:
            for node, edge in self._node_edge(conn).items():
                if node in dag:
                    dag[node][1].extend(edge[1])
                else:
                    dag[node] = edge

        return dag


    def validate_dag(self):
        targets = defaultdict(list)
        sources = defaultdict(list)
        for conn in self.connections:
            to_str = '{}.sources[{}]'.format(conn.to_pipe_name, conn.to_subject_name)
            from_str = '{}.targets[{}]'.format(conn.from_pipe_name, conn.from_subject_name)
            sources[from_str].append(to_str)
            targets[to_str].append(from_str)

            from_type = 'target'
            from_subjects = conn.parent.pipes[conn.from_pipe_name].targets
            if conn.is_from_self:
                from_type = 'source'
                from_subjects = conn.parent.pipes[conn.from_pipe_name].sources

            to_type = 'source'
            to_subjects = conn.parent.pipes[conn.to_pipe_name].sources
            if conn.is_to_self:
                to_type = 'target'
                to_subjects = conn.parent.pipes[conn.to_pipe_name].targets


            if not conn.from_pipe_name in conn.parent.pipes:
                msg = "Pipe '{}' not defined in parent pipe '{}'".format(
                    conn.from_pipe_name, conn.parent.name
                )
                raise DagValidationError(msg)
            if not conn.to_pipe_name in conn.parent.pipes:
                msg = "Pipe '{}' not defined in parent pipe '{}'".format(
                    conn.to_pipe_name, conn.parent.name
                )
                raise DagValidationError(msg)
            if not conn.from_subject_name in from_subjects:
                msg = "Pipe '{}' has no {} named '{}'".format(
                    conn.from_pipe_name, from_type, conn.from_subject_name
                )
                raise DagValidationError(msg)
            if not conn.to_subject_name in to_subjects:
                msg = "Pipe '{}' has no {} named '{}'".format(
                    conn.to_pipe_name, to_type, conn.to_subject_name
                )
                raise DagValidationError(msg)

        target_dupes = {k:v for k, v in targets.items() if len(v) > 1}
        if len(target_dupes) > 0:
            msg = 'Multiple connections to the same target.  ' \
                + 'Use a concatenator pipe instead.  Details: {}'.format(target_dupes)
            raise DagValidationError(msg)

        source_dupes = {k:v for k, v in sources.items() if len(v) > 1}
        if len(source_dupes) > 0:
            msg = 'Multiple connections from the same source.  ' \
                + 'Use a fork pipe instead.  Details: {}'.format(source_dupes)
            raise DagValidationError(msg)

    @staticmethod
    def _connect_to(source, connection):
        def __connect_to(target):
            pemi.log.debug('connecting %s to %s', target, source)
            connection.connect()
            return target
        __connect_to.__name__ = 'connect_to'
        return __connect_to

    @staticmethod
    def _get_subject(dstype, name):
        def __get_subject(daskpipe):
            pemi.log.debug('Getting %s[%s] from pipe %s', dstype, name, daskpipe.pipe)
            return getattr(daskpipe.pipe, dstype)[name]
        __get_subject.__name__ = '[{}]'.format(name)
        return __get_subject

    def _node_edge(self, conn):
        '''
        A dask graph is a dictionary mapping keys to computations.

        Here, the keys are either a collection of data subjects (sources or targets),
        or a specific subject.  Computations are either flowing a pipe, connecting
        a data subject of one pipe to a data subject of another pipe, or getting a specific
        data subject from a collection.

        This method builds a graph needed by dask to execute according to the right topology.
        '''

        keys = {}
        tasks = {}

        from_type = 'targets'
        to_type = 'sources'

        if conn.is_from_self:
            from_type = 'sources'
        if conn.is_to_self:
            to_type = 'targets'


        keys = {
            'from_pipe_subjects': '{pipe}.{dstype}'.format(
                pipe=conn.from_pipe_name,
                dstype=from_type
            ),
            'from_pipe_subject': '{pipe}.{dstype}[{subject}]'.format(
                pipe=conn.from_pipe_name,
                dstype=from_type,
                subject=conn.from_subject_name
            ),
            'to_pipe_subject': '{pipe}.{dstype}[{subject}]'.format(
                pipe=conn.to_pipe_name,
                dstype=to_type,
                subject=conn.to_subject_name
            ),
            'to_pipe_results': '{pipe}.targets'.format(
                pipe=conn.to_pipe_name
            ),
        }

        tasks = {
            'flow_from_pipe': (DaskPipe(conn.from_pipe, flow=(not conn.is_from_self)), []),
            'get_subject': (
                self._get_subject(from_type, conn.from_subject_name),
                keys['from_pipe_subjects']
            ),
            'connect_data': (
                self._connect_to(getattr(conn.to_pipe, to_type)[conn.to_subject_name], conn),
                keys['from_pipe_subject']
            ),
            'flow_to_pipe': (DaskPipe(conn.to_pipe), [keys['to_pipe_subject']])
        }


        if conn.is_to_self:
            del keys['to_pipe_results']
            del tasks['flow_to_pipe']

        graph_outline = {
            'from_pipe_subjects': 'flow_from_pipe',
            'from_pipe_subject': 'get_subject',
            'to_pipe_subject': 'connect_data',
            'to_pipe_results': 'flow_to_pipe'
        }

        return {
            keys[key]: tasks[task] for key, task in graph_outline.items() if key in keys
        }
