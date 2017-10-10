from collections import defaultdict

import dask
import dask.dot

import pemi

class DagValidationError(Exception): pass

class DaskPipe():
    'DaskPipe is just a wrapper around Pipe that allows us to use the Pipes in the context of Dask'

    def __init__(self, pipe, is_parent):
        self.pipe = pipe
        self.is_parent = is_parent

    def __call__(self, *args):
        pemi.log().info('DaskPipe flowing pipe {}'.format(self.pipe))
        if not self.is_parent:
            self.pipe.flow()
        return self

    def __str__(self):
        return self.pipe.name

class DaskFlow():
    def __init__(self, connections, group=None):
        self.connections = [conn for conn in connections if conn.group == group]

    def _validate_dag(self):
        targets = defaultdict(list)
        sources = defaultdict(list)
        for conn in self.connections:
            to_str = '{}.sources[{}]'.format(conn.to_pipe_name, conn.to_subject_name)
            from_str = '{}.targets[{}]'.format(conn.from_pipe_name, conn.from_subject_name)
            sources[from_str].append(to_str)
            targets[to_str].append(from_str)

        target_dupes = {k:v for k,v in targets.items() if len(v) > 1}
        if len(target_dupes) > 0:
            raise DagValidationError('Multiple connections to the same target.  Use a concatenator pipe instead.  Details: {}'.format(target_dupes))

        source_dupes = {k:v for k,v in sources.items() if len(v) > 1}
        if len(source_dupes) > 0:
            raise DagValidationError('Multiple connections from the same source.  Use a fork pipe instead.  Details: {}'.format(source_dupes))


    def dag(self):
        self._validate_dag()
        dag = {}
        for conn in self.connections:
            for node, edge in self._node_edge(conn).items():
                if node in dag:
                    dag[node][1].extend(edge[1])
                else:
                    dag[node] = edge

        return dag

    def flow(self):
        nodes = list(self.dag().keys())
        return dask.get(self.dag(), list(self.dag().keys()))

    def graph(self):
        return dask.dot.dot_graph(self.dag(), rankdir='TB')

    def _connect_to(self, source, connection):
        def __connect_to(target):
            pemi.log().debug('connecting {} to {}'.format(target, source))
            connection.connect()
            return target
        __connect_to.__name__ = 'connect_to'
        return __connect_to

    def _get_target(self, name):
        def __get_target(daskpipe):
            pemi.log().debug('Getting target {} from pipe {}'.format(name, daskpipe.pipe))
            return daskpipe.pipe.targets[name]
        __get_target.__name__ = '[{}]'.format(name)
        return __get_target

    def _node_edge(self, conn):
        return {
            '{}.targets'.format(conn.from_pipe_name): (DaskPipe(conn.from_pipe, conn.from_self), []),
            '{}.targets[{}]'.format(conn.from_pipe_name, conn.from_subject_name): (self._get_target(conn.from_subject_name), '{}.targets'.format(conn.from_pipe_name)),
            '{}.sources[{}]'.format(conn.to_pipe_name, conn.to_subject_name): (self._connect_to(conn.to_pipe.sources[conn.to_subject_name], conn), '{}.targets[{}]'.format(conn.from_pipe_name, conn.from_subject_name)),
            '{}.targets'.format(conn.to_pipe_name): (DaskPipe(conn.to_pipe, conn.to_self), ['{}.sources[{}]'.format(conn.to_pipe_name, conn.to_subject_name)])
        }
