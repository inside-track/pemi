import unittest

import pandas as pd

import pemi
import pemi.testing
import pemi.pipes.dask
import pemi.pipes.patterns
from pemi.fields import *

class APipe(pemi.Pipe):
    def config(self):
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
    def config(self):
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
    def config(self):
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
    def config(self):
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
class TestAa1ToXx1(unittest.TestCase):
    class Aa1ToXx1Pipe(pemi.Pipe):
        def config(self):
            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    def test_connections(self):
        pipe = self.Aa1ToXx1Pipe()
        pipe.flow()

        expected = 'a1 from APipe'
        actual = pipe.pipes['X'].sources['x1'].df['msg'][0]
        self.assertEqual(actual, expected)


# One-to-one pipe & subjects, with multiple subjects
class TestAa1a2ToXx1x2(unittest.TestCase):
    class Aa1a2ToXx1x2Pipe(pemi.Pipe):
        def config(self):
            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.connect(
                self.pipes['A'].targets['a2']
            ).to(
                self.pipes['X'].sources['x2']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    def test_connections(self):
        pipe = self.Aa1a2ToXx1x2Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a2 from APipe']
        actual = [pipe.pipes['X'].sources['x1'].df['msg'][0], pipe.pipes['X'].sources['x2'].df['msg'][0]]
        self.assertEqual(actual, expected)


# Many-to-one pipes, one-to-one subjects
class TestAa1Bb1ToXx1x2(unittest.TestCase):
    class Aa1Bb1ToXx1x2Pipe(pemi.Pipe):
        def config(self):
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

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.connect(
                self.pipes['B'].targets['b1']
            ).to(
                self.pipes['X'].sources['x2']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    def test_connections(self):
        pipe = self.Aa1Bb1ToXx1x2Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'b1 from BPipe']
        actual = [pipe.pipes['X'].sources['x1'].df['msg'][0], pipe.pipes['X'].sources['x2'].df['msg'][0]]
        self.assertEqual(actual, expected)

# One-to-many pipes, one-to-one subjects
class TestAa1a2ToXx1Yy1(unittest.TestCase):
    class Aa1a2ToXx1Yy1Pipe(pemi.Pipe):
        def config(self):
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

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.connect(
                self.pipes['A'].targets['a2']
            ).to(
                self.pipes['Y'].sources['y1']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    def test_connections(self):
        pipe = self.Aa1a2ToXx1Yy1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a2 from APipe']
        actual = [pipe.pipes['X'].sources['x1'].df['msg'][0], pipe.pipes['Y'].sources['y1'].df['msg'][0]]
        self.assertEqual(actual, expected)


# One-to-many pipes, one-to-many subjects (via Fork)
class TestAa1ToXx1Yy1(unittest.TestCase):
    class Aa1ToXx1Yy1Pipe(pemi.Pipe):
        def config(self):
            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='Fork',
                pipe=pemi.pipes.patterns.PdForkPipe(forks=['fork0', 'fork1'])
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.pipe(
                name='Y',
                pipe=YPipe()
            )

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['Fork'].sources['main']
            )

            self.connect(
                self.pipes['Fork'].targets['fork0']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.connect(
                self.pipes['Fork'].targets['fork1']
            ).to(
                self.pipes['Y'].sources['y1']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    class Aa1ToXx1Yy1NoForkPipe(pemi.Pipe):
        def config(self):
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

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['Y'].sources['y1']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    def test_connections(self):
        pipe = self.Aa1ToXx1Yy1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a1 from APipe']
        actual = [pipe.pipes['X'].sources['x1'].df['msg'][0], pipe.pipes['Y'].sources['y1'].df['msg'][0]]
        self.assertEqual(actual, expected)

    def test_fails_without_a_fork(self):
        pipe = self.Aa1ToXx1Yy1NoForkPipe()
        self.assertRaises(pemi.pipes.dask.DagValidationError, pipe.flow)


# One-to-one pipes, many-to-one subjects (via Concat)
class TestAa1a2ToXx1(unittest.TestCase):
    class Aa1a2ToXx1Pipe(pemi.Pipe):
        def config(self):
            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='Concat',
                pipe=pemi.pipes.patterns.PdConcatPipe(sources=['Aa1', 'Aa2'])
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['Concat'].sources['Aa1']
            )

            self.connect(
                self.pipes['A'].targets['a2']
            ).to(
                self.pipes['Concat'].sources['Aa2']
            )

            self.connect(
                self.pipes['Concat'].targets['main']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    class Aa1a2ToXx1NoConcatPipe(pemi.Pipe):
        def config(self):
            self.pipe(
                name='A',
                pipe=APipe()
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.connect(
                self.pipes['A'].targets['a2']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()

    def test_connections(self):
        pipe = self.Aa1a2ToXx1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'a2 from APipe']
        actual = list(pipe.pipes['X'].sources['x1'].df['msg'])
        self.assertEqual(actual, expected)

    def test_fails_without_concat(self):
        pipe = self.Aa1a2ToXx1NoConcatPipe()
        self.assertRaises(pemi.pipes.dask.DagValidationError, pipe.flow)


# Many-to-one pipes, many-to-one subjects (via Concat)
class TestAa1Bb1ToXx1(unittest.TestCase):
    class Aa1Bb1ToXx1Pipe(pemi.Pipe):
        def config(self):
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
                pipe=pemi.pipes.patterns.PdConcatPipe(sources=['Aa1', 'Bb1'])
            )

            self.pipe(
                name='X',
                pipe=XPipe()
            )

            self.connect(
                self.pipes['A'].targets['a1']
            ).to(
                self.pipes['Concat'].sources['Aa1']
            )

            self.connect(
                self.pipes['B'].targets['b1']
            ).to(
                self.pipes['Concat'].sources['Bb1']
            )

            self.connect(
                self.pipes['Concat'].targets['main']
            ).to(
                self.pipes['X'].sources['x1']
            )

            self.dask = pemi.pipes.dask.DaskFlow(self.connections)

        def flow(self):
            self.dask.flow()


    def test_connections(self):
        pipe = self.Aa1Bb1ToXx1Pipe()
        pipe.flow()

        expected = ['a1 from APipe', 'b1 from BPipe']
        actual = list(pipe.pipes['X'].sources['x1'].df['msg'])
        self.assertEqual(actual, expected)
