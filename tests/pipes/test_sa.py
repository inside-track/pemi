import os

import pytest
import factory

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


class BeersKeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence(lambda n: n)

sa_pipe = pemi.pipes.sa.SaSqlSourcePipe(
    engine=sa_engine,
    schema=pemi.Schema(
        beer_id=IntegerField(),
        name=StringField(),
        sold_at=DateField(),
        quantity=IntegerField()
    ),
    sql='SELECT * FROM sales_fact;'
)

sa_pipe.source(
    pemi.SaDataSubject,
    name='dummy_source',
    schema=sa_pipe.schema,
    engine=sa_engine,
    table='sales_fact'
)

with pt.Scenario(
    name='SaSqlSourcePipe',
    pipe=sa_pipe,
    factories={
        'beers': BeersKeyFactory
    },
    sources={
        'dummy_source': lambda pipe: pipe.sources['dummy_source']
    },
    targets={
        'main': lambda pipe: pipe.targets['main']
    },
    target_case_collectors={
        'main': pt.CaseCollector(subject_field='beer_id', factory='beers', factory_field='id')
    },
    usefixtures=['db_case_clean']
) as scenario:

    with scenario.case('it queries data stored') as case:
        ex_sales = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | {b[1]}  | 2017-01-01 | 3        |
            | {b[2]}  | 2017-01-02 | 2        |
            | {b[3]}  | 2017-01-03 | 5        |
            '''.format(
                b=scenario.factories['beers']['id']
            ),
            schema=scenario.sources['dummy_source'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['dummy_source'], ex_sales)
        ).then(
            pt.then.target_matches_example(scenario.targets['main'], ex_sales)
        )

    with scenario.case('it works with na') as case:
        ex_sales = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | {b[1]}  | 2017-01-01 | 3        |
            | {b[2]}  | 2017-01-02 |          |
            | {b[3]}  | 2017-01-03 | 5        |
            '''.format(
                b=scenario.factories['beers']['id']
            ),
            schema=scenario.sources['dummy_source'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['dummy_source'], ex_sales)
        ).then(
            pt.then.target_matches_example(scenario.targets['main'], ex_sales)
        )
