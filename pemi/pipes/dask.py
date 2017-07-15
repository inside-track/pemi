import dask
import dask.dot

import pemi

class DaskPipe():
    'DaskPipe is just a wrapper around Pipe that allows us to use the Pipes in the context of Dask'

    def __init__(self, pipe):
        self.pipe = pipe

    def __call__(self, *args):
        pemi.log().info('DaskPipe flowing pipe {}'.format(self.pipe))
        self.pipe.flow()
        return self

    def __str__(self):
        return self.pipe.name

class DaskFlow():
    def __init__(self, connections):
        self.connections = connections

    def dag(self):
        dag = {}
        for conn in self.connections:
            for node, edge in self._node_edge(conn).items():
                if node in dag:
                    dag[node][1].extend(edge[1])
                else:
                    dag[node] = edge
        return dag

    def flow(self):
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
            '{}.targets'.format(conn.from_pipe.name): (DaskPipe(conn.from_pipe), []),
            '{}.targets[{}]'.format(conn.from_pipe.name, conn.from_subject): (self._get_target(conn.from_subject), '{}.targets'.format(conn.from_pipe.name)),
            '{}.sources[{}]'.format(conn.to_pipe.name, conn.to_subject): (self._connect_to(conn.to_pipe.sources[conn.to_subject], conn), '{}.targets[{}]'.format(conn.from_pipe.name, conn.from_subject)),
            '{}.targets'.format(conn.to_pipe.name): (DaskPipe(conn.to_pipe), ['{}.sources[{}]'.format(conn.to_pipe.name, conn.to_subject)])
        }
