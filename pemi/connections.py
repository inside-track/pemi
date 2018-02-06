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

class DaskPipe: #pylint: disable=too-few-public-methods
    'DaskPipe is just a wrapper around Pipe that allows us to use the Pipes in the context of Dask'

    def __init__(self, pipe, is_parent):
        self.pipe = pipe
        self.is_parent = is_parent

    def __call__(self, *args):
        pemi.log.info('DaskPipe flowing pipe %s', self.pipe)
        if not self.is_parent:
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

    def flow(self):
        return dask.get(self.dask_dag(), list(self.dask_dag().keys()))


    def graph(self):
        #TODO: Make my own graph that simplifies some of the dask boilerplate
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
            if not conn.from_subject_name in conn.parent.pipes[conn.from_pipe_name].targets:
                msg = "Pipe '{}' has no target named '{}'".format(
                    conn.from_pipe_name, conn.from_subject_name
                )
                raise DagValidationError(msg)
            if not conn.to_subject_name in conn.parent.pipes[conn.to_pipe_name].sources:
                msg = "Pipe '{}' has no source named '{}'".format(
                    conn.from_pipe_name, conn.from_subject_name
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
    def _get_target(name):
        def __get_target(daskpipe):
            pemi.log.debug('Getting target %s from pipe %s', name, daskpipe.pipe)
            return daskpipe.pipe.targets[name]
        __get_target.__name__ = '[{}]'.format(name)
        return __get_target

    def _node_edge(self, conn):
        return {
            '{}.targets'.format(conn.from_pipe_name):
                (DaskPipe(conn.from_pipe, conn.from_self), []),
            '{}.targets[{}]'.format(conn.from_pipe_name, conn.from_subject_name):
                (self._get_target(conn.from_subject_name), '{}.targets'.format(
                    conn.from_pipe_name)),
            '{}.sources[{}]'.format(conn.to_pipe_name, conn.to_subject_name):
                (self._connect_to(conn.to_pipe.sources[conn.to_subject_name], conn),
                 '{}.targets[{}]'.format(conn.from_pipe_name, conn.from_subject_name)),
            '{}.targets'.format(conn.to_pipe_name):
                (DaskPipe(conn.to_pipe, conn.to_self),
                 ['{}.sources[{}]'.format(conn.to_pipe_name, conn.to_subject_name)])
        }
