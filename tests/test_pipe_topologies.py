import pandas as pd

import pytest

import pemi
import pemi.testing
import pemi.connections
import pemi.pipes.pd
from pemi.fields import *

#pylint: disable=too-few-public-methods

class APipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.target(
            pemi.PdDataSubject,
            name='a1',
            schema=pemi.Schema(msg=StringField())
        )

        self.target(
            pemi.PdDataSubject,
            name='a2',
            schema=pemi.Schema(msg=StringField())
        )

    def flow(self):
        self.targets['a1'].df = pd.DataFrame({'msg': ['a1 from APipe']})
        self.targets['a2'].df = pd.DataFrame({'msg': ['a2 from APipe']})

class BPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.target(
            pemi.PdDataSubject,
            name='b1',
            schema=pemi.Schema(msg=StringField())
        )

        self.target(
            pemi.PdDataSubject,
            name='b2',
            schema=pemi.Schema(msg=StringField())
        )

    def flow(self):
        self.targets['b1'].df = pd.DataFrame({'msg': ['b1 from BPipe']})
        self.targets['b2'].df = pd.DataFrame({'msg': ['b2 from BPipe']})

class XPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.source(
            pemi.PdDataSubject,
            name='x1',
            schema=pemi.Schema(msg=StringField())
        )

        self.source(
            pemi.PdDataSubject,
            name='x2',
            schema=pemi.Schema(msg=StringField())
        )

    def flow(self):
        pass

class YPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.source(
            pemi.PdDataSubject,
            name='y1',
            schema=pemi.Schema(msg=StringField())
        )

        self.source(
            pemi.PdDataSubject,
            name='y2',
            schema=pemi.Schema(msg=StringField())
        )

    def flow(self):
        pass




# One-to-one pipe & subjects
class TestAa1ToXx1():
    class Aa1ToXx1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect('A', 'a1').to('X', 'x1')

        def flow(self):
            self.connections.flow()


    def test_connections(self):
        pipe = self.Aa1ToXx1Pipe()
        pipe.flow()

        expected = 'a1 from APipe'
        actual = pipe.pipes['X'].sources['x1'].df['msg'][0]
        assert actual == expected


# One-to-one pipe & subjects, with multiple subjects
class TestAa1a2ToXx1x2():
    class Aa1a2ToXx1x2Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect('A', 'a1').to('X', 'x1')
            self.connect('A', 'a2').to('X', 'x2')

        def flow(self):
            self.connections.flow()


    def test_connections(self):
        pipe = self.Aa1a2ToXx1x2Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a2 from APipe']
        actual = [
            pipe.pipes['X'].sources['x1'].df['msg'][0],
            pipe.pipes['X'].sources['x2'].df['msg'][0]
        ]
        assert actual == expected


# Many-to-one pipes, one-to-one subjects
class TestAa1Bb1ToXx1x2():
    class Aa1Bb1ToXx1x2Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='B',
                pipe=BPipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect('A', 'a1').to('X', 'x1')
            self.connect('B', 'b1').to('X', 'x2')

        def flow(self):
            self.connections.flow()


    def test_connections(self):
        pipe = self.Aa1Bb1ToXx1x2Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'b1 from BPipe']
        actual = [
            pipe.pipes['X'].sources['x1'].df['msg'][0],
            pipe.pipes['X'].sources['x2'].df['msg'][0]
        ]
        assert actual == expected

# One-to-many pipes, one-to-one subjects
class TestAa1a2ToXx1Yy1():
    class Aa1a2ToXx1Yy1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.pipe(
                name='Y',
                pipe=YPipe()
            )

            self.connect('A', 'a1').to('X', 'x1')
            self.connect('A', 'a2').to('Y', 'y1')

        def flow(self):
            self.connections.flow()


    def test_connections(self):
        pipe = self.Aa1a2ToXx1Yy1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a2 from APipe']
        actual = [
            pipe.pipes['X'].sources['x1'].df['msg'][0],
            pipe.pipes['Y'].sources['y1'].df['msg'][0]
        ]
        assert actual == expected



# One-to-many pipes, one-to-many subjects (via Fork)
class TestAa1ToXx1Yy1():
    class Aa1ToXx1Yy1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='Fork',
                pipe=pemi.pipes.pd.PdForkPipe(forks=['fork0', 'fork1'])
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.pipe(
                name='Y',
                pipe=YPipe()
            )

            self.connect('A', 'a1').to('Fork', 'main')
            self.connect('Fork', 'fork0').to('X', 'x1')
            self.connect('Fork', 'fork1').to('Y', 'y1')

        def flow(self):
            self.connections.flow()


    class Aa1ToXx1Yy1NoForkPipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.pipe(
                name='Y',
                pipe=YPipe()
            )

            self.connect('A', 'a1').to('X', 'x1')
            self.connect('A', 'a1').to('Y', 'y1')

        def flow(self):
            self.connections.flow()


    def test_connections(self):
        pipe = self.Aa1ToXx1Yy1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a1 from APipe']
        actual = [
            pipe.pipes['X'].sources['x1'].df['msg'][0],
            pipe.pipes['Y'].sources['y1'].df['msg'][0]
        ]
        assert actual == expected

    def test_fails_without_a_fork(self):
        pipe = self.Aa1ToXx1Yy1NoForkPipe()
        with pytest.raises(pemi.connections.DagValidationError):
            pipe.flow()


# One-to-one pipes, many-to-one subjects (via Concat)
class TestAa1a2ToXx1():
    class Aa1a2ToXx1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='Concat',
                pipe=pemi.pipes.pd.PdConcatPipe(sources=['Aa1', 'Aa2'])
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect('A', 'a1').to('Concat', 'Aa1')
            self.connect('A', 'a2').to('Concat', 'Aa2')
            self.connect('Concat', 'main').to('X', 'x1')

        def flow(self):
            self.connections.flow()


    class Aa1a2ToXx1NoConcatPipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect('A', 'a1').to('X', 'x1')
            self.connect('A', 'a2').to('X', 'x1')

        def flow(self):
            self.connections.flow()

    def test_connections(self):
        pipe = self.Aa1a2ToXx1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a2 from APipe']
        actual = list(pipe.pipes['X'].sources['x1'].df['msg'])
        assert actual == expected

    def test_fails_without_concat(self):
        pipe = self.Aa1a2ToXx1NoConcatPipe()
        with pytest.raises(pemi.connections.DagValidationError):
            pipe.flow()


# Many-to-one pipes, many-to-one subjects (via Concat)
class TestAa1Bb1ToXx1():
    class Aa1Bb1ToXx1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='B',
                pipe=BPipe()
            )

            self.pipe(
                name='Concat',
                pipe=pemi.pipes.pd.PdConcatPipe(sources=['Aa1', 'Bb1'])
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect('A', 'a1').to('Concat', 'Aa1')
            self.connect('B', 'b1').to('Concat', 'Bb1')
            self.connect('Concat', 'main').to('X', 'x1')

        def flow(self):
            self.connections.flow()


    def test_connections(self):
        pipe = self.Aa1Bb1ToXx1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'b1 from BPipe']
        actual = list(pipe.pipes['X'].sources['x1'].df['msg'])
        assert actual == expected


# Connecting from external source pipes
class TestExternalSourcePipes():
    class FromAa1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )
            self.connect('A', 'a1').to('self', 'main')

            self.target(
                pemi.PdDataSubject,
                name='main',
                schema=pemi.Schema(msg=StringField())
            )


        def flow(self):
            self.connections.flow()


    def test_connections(self):
        pipe = self.FromAa1Pipe()
        pipe.flow()

        expected = 'a1 from APipe'
        actual = pipe.targets['main'].df['msg'][0]
        assert actual == expected


# Connecting to external target pipes
class TestExternalTargetPipes():
    class ToXx1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.source(
                pemi.PdDataSubject,
                name='main',
                schema=pemi.Schema(msg=StringField())
            )
            self.connect('self', 'main').to('X', 'x1')

            self.pipe(
                name='X',
                pipe=XPipe()
            )


        def flow(self):
            self.sources['main'].df = pd.DataFrame({'msg': ['generated in self']})
            self.connections.flow()


    def test_connections(self):
        pipe = self.ToXx1Pipe()
        pipe.flow()

        expected = 'generated in self'
        actual = pipe.pipes['X'].sources['x1'].df['msg'][0]
        assert actual == expected


# Connected from external source pipes and external target pipes
class TestExternalSourceAndTargetPipes():
    class FromAa1ToXx1Pipe(pemi.Pipe):
        def __init__(self, **params):
            super().__init__(**params)

            self.pipe(
                name='A',
                pipe=APipe()
            )
            self.connect('A', 'a1').to('X', 'x1')

            self.pipe(
                name='X',
                pipe=XPipe()
            )

        def flow(self):
            self.connections.flow()

    def test_connections(self):
        pipe = self.FromAa1ToXx1Pipe()
        pipe.flow()

        expected = 'a1 from APipe'
        actual = pipe.pipes['X'].sources['x1'].df['msg'][0]
        assert actual == expected
