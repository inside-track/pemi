import os

import pytest

import sqlalchemy as sa

import pemi
import pemi.testing
import pemi.pipes.sa
from pemi.fields import *

def truncate_sa_sources(pipe):
    sa_sources = [source for source in pipe.sources.values() if isinstance(source, pemi.SaDataSubject)]
    for source in sa_sources:
        with source.engine.connect().begin() as trans:
            trans.connection.execute('TRUNCATE {}'.format(source.table))


@pytest.fixture(scope='module')
def db_schema_init():
    with sa_engine().connect() as conn:
        conn.execute(
            '''
            DROP TABLE IF EXISTS sales_fact;
            CREATE TABLE sales_fact (
              beer_id INT,
              name VARCHAR(80),
              sold_at DATE,
              quantity INT
            );
            '''
        )

@pytest.fixture
def db_clean_pipe(pipe, db_schema_init):
    truncate_sa_sources(pipe)
    yield
    truncate_sa_sources(pipe)


@pytest.fixture
def sa_engine():
    return sa.create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'),
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DB')
    ))

@pytest.fixture
def sales_schema():
    return pemi.Schema(
            beer_id  = IntegerField(),
            name     = StringField(),
            sold_at  = DateField(),
            quantity = IntegerField()
        )


@pytest.mark.usefixtures('db_clean_pipe')
class TestSaSqlSourcePipe():

    @pytest.fixture
    def pipe(self, sa_engine, sales_schema):
        pipe = pemi.Pipe()
        pipe.pipe(
            name='sql_source',
            pipe=pemi.pipes.sa.SaSqlSourcePipe(
                engine=sa_engine,
                schema=sales_schema,
                sql='SELECT * FROM sales_fact;'
            )
        )

        pipe.source(
            pemi.SaDataSubject,
            name='sales',
            schema=sales_schema,
            engine=sa_engine,
            table='sales_fact'
        )
        return pipe

    @pytest.fixture
    def rules(self, pipe):
        return pemi.testing.Rules(
            source_subjects=[
                pipe.sources['sales']
            ],
            target_subjects=[
                pipe.pipes['sql_source'].targets['main']
            ]
        )

    @pytest.fixture
    def scenario(self, pipe, rules):
        return pemi.testing.Scenario(
            runner = pipe.pipes['sql_source'].flow,
            source_subjects=[
                pipe.sources['sales']
            ],
            target_subjects=[
                pipe.pipes['sql_source'].targets['main']
            ]
        )


    def test_it_queries_data_stored(self, pipe, rules, scenario):
        ex_sales = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | 1       | 2017-01-01 | 3        |
            | 2       | 2017-01-02 | 2        |
            | 3       | 2017-01-03 | 5        |
            ''',
            schema=pipe.sources['sales'].schema
        )

        scenario.when(
            rules.when_example_for_source(ex_sales)
        ).then(
            rules.then_target_matches_example(ex_sales)
        )
        scenario.run()


    def test_it_works_with_na(self, pipe, rules, scenario):
        ex_sales = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | 1       | 2017-01-01 | 3        |
            | 2       | 2017-01-02 |          |
            | 3       | 2017-01-03 | 5        |
            ''',
            schema=pipe.sources['sales'].schema
        )

        scenario.when(
            rules.when_example_for_source(ex_sales)
        ).then(
            rules.then_target_matches_example(ex_sales)
        )
        scenario.run()
