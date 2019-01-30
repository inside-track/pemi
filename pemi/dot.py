from IPython.display import display
from graphviz import Digraph


GRAPH_OPTS = {
    'source': {'shape': 'invhouse', 'color': 'lightgreen', 'style': 'filled'},
    'target': {'shape': 'house', 'color': 'lightblue', 'style': 'filled'},
    'pipe': {'shape': 'box', 'color': 'orange', 'style': 'filled, rounded'}
}

def graph(pipe):
    '''
    Running this in a Jupyter notebook will draw a graph of the connections between
    sources, targets, and pipes.
    '''


    dot = Digraph()

    for pname in pipe.pipes:
        if pname != 'self':
            dot.node(pname, **GRAPH_OPTS['pipe'])

        for source in pipe.pipes[pname].sources:
            if pname == 'self':
                label = 'self[{}]'.format(source)
            else:
                label = source

                dot.edge('s.{}[{}]'.format(pname, source), pname)


            dot.node(
                's.{}[{}]'.format(pname, source),
                label=label,
                **GRAPH_OPTS['source']
            )



        for target in pipe.pipes[pname].targets:
            if pname == 'self':
                label = 'self[{}]'.format(target)
            else:
                label = target

                dot.edge(pname, 't.{}[{}]'.format(pname, target))

            dot.node(
                't.{}[{}]'.format(pname, target),
                label=label,
                **GRAPH_OPTS['target']
            )


    for conn in pipe.connections.connections:
        from_ind = 't'
        to_ind = 's'
        if conn.from_pipe_name == 'self':
            from_ind = 's'
        if conn.to_pipe_name == 'self':
            to_ind = 't'

        dot.edge(
            '{}.{}[{}]'.format(from_ind, conn.from_pipe_name, conn.from_subject_name),
            '{}.{}[{}]'.format(to_ind, conn.to_pipe_name, conn.to_subject_name),
            arrowhead='dot', arrowtail='dot', dir='both'
        )

    display(dot)
