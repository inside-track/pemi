import os

import pytest

import sqlalchemy as sa

import pemi
import pemi.testing as pt
import pemi.pipes.sa
from pemi.fields import *

sa_engine = sa.create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
    host=os.environ.get('POSTGRES_HOST'),
    dbname=os.environ.get('POSTGRES_DB')
))


def truncate_sa_sources(scenario_sources):
    sa_sources = [
        source.subject
        for source in scenario_sources if isinstance(source.subject, pemi.SaDataSubject)
    ]
    for source in sa_sources:
        with source.engine.connect().begin() as trans:
            trans.connection.execute('TRUNCATE {}'.format(source.table))

#pylint: disable=unused-argument
@pytest.fixture(scope='module')
def db_schema_init():
    with sa_engine.connect() as conn:
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
def db_case_clean(case, db_schema_init):
    if not case.scenario.has_run:
        truncate_sa_sources(case.scenario.sources.values())
        yield
        truncate_sa_sources(case.scenario.sources.values())
    else:
        yield
#pylint: enable=unused-argument

with pt.Scenario('SaSqlSourcePipe', usefixtures=['db_case_clean']) as scenario:
    sales_schema = pemi.Schema(
        beer_id=IntegerField(),
        name=StringField(),
        sold_at=DateField(),
        quantity=IntegerField()
    )

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


    def case_keys():
        ids = list(range(1000))
        for i in ids:
            yield {
                'sales': {'beer_id': i},
                'sql_source': {'beer_id': i}
            }

    scenario.setup(
        runner=pipe.pipes['sql_source'].flow,
        case_keys=case_keys(),
        sources={
            'sales': pipe.sources['sales']
        },
        targets={
            'sql_source': pipe.pipes['sql_source'].targets['main']
        }
    )

    with scenario.case('it queries data stored') as case:
        ex_sales = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | {b[1]}  | 2017-01-01 | 3        |
            | {b[2]}  | 2017-01-02 | 2        |
            | {b[3]}  | 2017-01-03 | 5        |
            '''.format(b=scenario.case_keys.cache('sales', 'beer_id')),
            schema=pipe.sources['sales'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['sales'], ex_sales)
        ).then(
            pt.then.target_matches_example(scenario.targets['sql_source'], ex_sales)
        )

    with scenario.case('it works with na') as case:
        ex_sales = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | {b[1]}  | 2017-01-01 | 3        |
            | {b[2]}  | 2017-01-02 |          |
            | {b[3]}  | 2017-01-03 | 5        |
            '''.format(b=scenario.case_keys.cache('sales', 'beer_id')),
            schema=pipe.sources['sales'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['sales'], ex_sales)
        ).then(
            pt.then.target_matches_example(scenario.targets['sql_source'], ex_sales)
        )
