import os

import pytest
import sqlalchemy as sa

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
                sources=['s1', 's2']
            )
        )
        self.connect('concat', 'main').to('t1', 'main')

        self.pipe(
            name='t1',
            pipe=SomeTargetPipe()
        )

    def flow(self):
        self.connections.flow()

def test_parent_pipe():
    job_pipe = JobPipe()
    assert job_pipe.pipes['concat'].parent_pipe == job_pipe


class TestPickling():
    @pytest.fixture(scope='class')
    def job(self):
        job = JobPipe()
        job.flow()
        return job

    @pytest.fixture(scope='class')
    def unpickled_job(self, job):
        pickled = job.to_pickle()
        return JobPipe().from_pickle(pickled)


    @pytest.mark.parametrize('sub_pipe,subject', [
        ('concat', 's1'),
        ('concat', 's2'),
        ('t1', 'main'),
    ])
    def test_it_unpickles_pickled_pipe_sources(self, sub_pipe, subject, job, unpickled_job):
        assert_frame_equal(
            job.pipes[sub_pipe].sources[subject].df,
            unpickled_job.pipes[sub_pipe].sources[subject].df
        )

    @pytest.mark.parametrize('sub_pipe,subject', [
        ('s1', 'main'),
        ('s2', 'main'),
        ('concat', 'main'),
    ])
    def test_it_unpickles_pickled_pipe_targets(self, sub_pipe, subject, job, unpickled_job):
        assert_frame_equal(
            job.pipes[sub_pipe].targets[subject].df,
            unpickled_job.pipes[sub_pipe].targets[subject].df
        )


    def test_it_pickles_when_pipe_has_unpickleable_things(self):
        class NotPickleablePipe(JobPipe):
            def __init__(self, **params):
                super().__init__(**params)
                self.pipe(
                    name='s1',
                    pipe=SomeSourcePipe(addone=lambda v: v + 1)
                )

        job = NotPickleablePipe()
        job.flow()

        pickled = job.to_pickle()
        unpickled_job = NotPickleablePipe().from_pickle(pickled)

        assert unpickled_job.pipes['s1'].params['addone'](3) == 4

    def test_unpickled_references_original_pipes(self):
        job = JobPipe()
        job.flow()

        pickled = job.to_pickle()
        unpickled_job = JobPipe().from_pickle(pickled)

        assert unpickled_job.pipes['s1'].targets['main'].pipe.__class__ \
            == job.pipes['s1'].targets['main'].pipe.__class__


    def test_pickles_w_sa_data_subject(self):
        class WithSaSubjectPipe(pemi.Pipe):
            def __init__(self):
                super().__init__()

                engine = sa.create_engine(
                    'postgresql://{user}:{password}@{host}/{dbname}'.format(
                        user=os.environ.get('POSTGRES_USER'),
                        password=os.environ.get('POSTGRES_PASSWORD'),
                        host=os.environ.get('POSTGRES_HOST'),
                        dbname=os.environ.get('POSTGRES_DB')
                    )
                )

                self.source(
                    pemi.SaDataSubject,
                    name='mytable',
                    schema=pemi.Schema(),
                    engine=engine,
                    table='mytable'
                )

                self.pipe(
                    name='s1',
                    pipe=SomeSourcePipe(addone=lambda v: v + 1)
                )


            def flow(self):
                self.pipes['s1'].flow()


        job = WithSaSubjectPipe()
        job.flow()

        pickled = job.to_pickle()
        unpickled_job = WithSaSubjectPipe().from_pickle(pickled)

        assert_frame_equal(
            job.pipes['s1'].targets['main'].df,
            unpickled_job.pipes['s1'].targets['main'].df
        )
