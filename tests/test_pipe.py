import unittest

from pandas.util.testing import assert_frame_equal

import pemi
import pemi.data
import pemi.pipes.pd
from pemi.fields import *

class SomeSourcePipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.target(
            pemi.PdDataSubject,
            name='main'
        )

    def flow(self):
        self.targets['main'].df = pemi.data.Table(
            schema=pemi.Schema(
                style=StringField(),
                color=StringField(),
                abv=IntegerField()
            )
        ).df

class SomeTargetPipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.source(
            pemi.PdDataSubject,
            name='main'
        )

    def flow(self):
        pass


class JobPipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.pipe(
            name='s1',
            pipe=SomeSourcePipe()
        )
        self.connect('s1', 'main').to('concat', 's1')

        self.pipe(
            name='s2',
            pipe=SomeSourcePipe()
        )
        self.connect('s2', 'main').to('concat', 's2')

        self.pipe(
            name='concat',
            pipe=pemi.pipes.pd.PdConcatPipe(
                sources=['s1','s2']
            )
        )
        self.connect('concat', 'main').to('t1', 'main')

        self.pipe(
            name='t1',
            pipe=SomeTargetPipe()
        )

    def flow(self):
        self.connections.flow()



class TestPickling(unittest.TestCase):
    # TODO: good case for pytest paramtrized tests uses the result of job.flow() as a fixture computed once
    def test_it_unpickles_pickled_pipes(self):
        job = JobPipe()
        job.flow()

        pickled = job.to_pickle()
        unpickled_job = JobPipe().from_pickle(pickled)

        assert_frame_equal(job.pipes['s1'].targets['main'].df, unpickled_job.pipes['s1'].targets['main'].df)
        assert_frame_equal(job.pipes['s2'].targets['main'].df, unpickled_job.pipes['s2'].targets['main'].df)
        assert_frame_equal(job.pipes['concat'].sources['s1'].df, unpickled_job.pipes['concat'].sources['s1'].df)
        assert_frame_equal(job.pipes['concat'].sources['s2'].df, unpickled_job.pipes['concat'].sources['s2'].df)
        assert_frame_equal(job.pipes['concat'].targets['main'].df, unpickled_job.pipes['concat'].targets['main'].df)
        assert_frame_equal(job.pipes['t1'].sources['main'].df, unpickled_job.pipes['t1'].sources['main'].df)
