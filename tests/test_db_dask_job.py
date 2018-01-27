import os

import pytest

import pandas as pd
import sqlalchemy as sa

import pemi
import pemi.testing as pt
from pemi.data_subject import SaDataSubject
from pemi.fields import *

import logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

import sys
this = sys.modules[__name__]

this.params = {
    'sa_conn_str': 'postgresql://{user}:{password}@{host}/{dbname}'.format(
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'),
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DB')
    )
}

# Setup (table creation done in some other task)
with sa.create_engine(this.params['sa_conn_str']).connect() as conn:
    conn.execute(
        '''
        DROP TABLE IF EXISTS dumb_sales;
        DROP TABLE IF EXISTS dumb_beers;

        DROP TABLE IF EXISTS sales;
        CREATE TABLE sales (
          beer_id INT,
          sold_at DATE,
          quantity INT,
          bumpkin VARCHAR(80)
        );

        DROP TABLE IF EXISTS beers;
        CREATE TABLE beers (
          id INT,
          name VARCHAR(80),
          style VARCHAR(80),
          abv FLOAT,
          price DECIMAL(16,2)
        );

        DROP TABLE IF EXISTS dumb_sales;
        CREATE TABLE dumb_sales (LIKE sales);
        DROP TABLE IF EXISTS dumb_beers;
        CREATE TABLE dumb_beers (LIKE beers);
        '''
    )


# This is a really dumb pipe.  It just copies a table to dumb_table.  It's meant to demo
# how queries can be run in parallel via Dask.
class DumbSaPipe(pemi.Pipe):
    def __init__(self, *, schema, table, engine_opts, **params):
        super().__init__(**params)

        self.schema = schema
        self.table = table
        self.sa_engine = sa.create_engine(engine_opts['conn_str'])

        self.source(
            SaDataSubject,
            name='main',
            schema=self.schema,
            engine=self.sa_engine,
            table=self.table
        )

        self.target(
            SaDataSubject,
            name='main',
            schema=self.schema,
            engine=self.sa_engine,
            table='dumb_{}'.format(self.table)
        )

    def flow(self):
        with self.sa_engine.connect() as conn:
            conn.execute(
                '''
                DROP TABLE IF EXISTS dumb_{table};
                CREATE TABLE dumb_{table} (LIKE {table});
                INSERT INTO dumb_{table} (SELECT * FROM {table});
                '''.format(table=self.table)
            )

        self.targets['main'] = self.sources['main']


class DenormalizeBeersPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        sa_engine = sa.create_engine(this.params['sa_conn_str'])

        self.source(
            SaDataSubject,
            name='sales',
            schema=pemi.Schema(
                beer_id  = IntegerField(),
                sold_at  = DateField(format='%m/%d/%Y'),
                quantity = IntegerField()
            ),
            engine=sa_engine,
            table='dumb_sales'
        )

        self.source(
            SaDataSubject,
            name='beers',
            schema = pemi.Schema(
                id    = IntegerField(),
                name  = StringField(),
                style = StringField(),
                abv   = FloatField(),
                price = DecimalField(precision=16, scale=2)
            ),
            engine=sa_engine,
            table='dumb_beers'
        )

        self.target(
            SaDataSubject,
            name='beer_sales',
            schema=pemi.Schema(
                beer_id    = IntegerField(),
                name       = StringField(),
                style      = StringField(),
                sold_at    = DateField(format='%m/%d/%Y'),
                quantity   = IntegerField(),
                unit_price = DecimalField(precision=16, scale=2),
                sell_price = DecimalField(precision=16, scale=2)
            ),
            engine=sa_engine,
            table='beer_sales'
        )


        self.pipe(
            name='dumb_sales',
            pipe=DumbSaPipe(
                schema=self.sources['sales'].schema,
                table='sales',
                engine_opts={'conn_str': this.params['sa_conn_str']}
            )
        )

        self.pipe(
            name='dumb_beers',
            pipe=DumbSaPipe(
                schema=self.sources['beers'].schema,
                table='beers',
                engine_opts={'conn_str': this.params['sa_conn_str']}
            )
        )

        self.connect('dumb_sales', 'main').to('self', 'sales')
        self.connect('dumb_beers', 'main').to('self', 'beers')

    def flow(self):
        self.connections.flow()

        sa_beer_sales = self.targets['beer_sales']
        with sa_beer_sales.engine.connect() as conn:
            conn.execute(
                '''
                DROP TABLE IF EXISTS beer_sales;
                CREATE TABLE beer_sales AS (
                  SELECT
                    sales.beer_id,
                    beers.name,
                    beers.style,
                    sales.sold_at,
                    sales.quantity,
                    beers.price as unit_price,
                    beers.price * sales.quantity as sell_price
                  FROM
                    dumb_sales sales
                  LEFT JOIN
                    dumb_beers beers
                  ON
                    sales.beer_id = beers.id
                );

                DROP TABLE IF EXISTS dumb_sales;
                DROP TABLE IF EXISTS dumb_beers;
                '''
            )


class TestDenormalizeBeersPipe():
    pipe = DenormalizeBeersPipe()

    pt.mock_pipe(pipe, 'dumb_sales')
    pt.mock_pipe(pipe, 'dumb_beers')

    def case_keys():
        ids = list(range(1000))
        for i in ids:
            yield {
                'dumb_sales': {'beer_id': i},
                'dumb_beers': {'id': i},
                'beer_sales': {'beer_id': i}
            }

    scenario = pt.Scenario(
        runner = pipe.flow,
        case_keys = case_keys(),
        sources={
            'dumb_sales': pipe.pipes['dumb_sales'].targets['main'],
            'dumb_beers': pipe.pipes['dumb_beers'].targets['main']
        },
        targets={
            'beer_sales': pipe.targets['beer_sales']
        }
    )

    with scenario.case('it joins sales to beers') as case:
        sales_table = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | {b[1]}  | 01/01/2017 | 3        |
            | {b[2]}  | 01/02/2017 | 3        |
            | {b[3]}  | 01/03/2017 | 5        |
            | {b[4]}  | 01/04/2017 | 8        |
            | {b[5]}  | 01/04/2017 | 6        |
            | {b[1]}  | 01/06/2017 | 1        |
            '''.format(b = scenario.case_keys.cache('dumb_sales', 'beer_id')),
            schema=pipe.sources['sales'].schema
        )

        beers_table = pemi.data.Table(
            '''
            | id     | name          | style |
            | -      | -             | -     |
            | {b[1]} | SpinCyle      | IPA   |
            | {b[2]} | OldStyle      | Pale  |
            | {b[3]} | Pipewrench    | IPA   |
            | {b[4]} | AbstRedRibbon | Lager |
            '''.format(b = scenario.case_keys.cache('dumb_beers', 'id')),
            schema=pipe.sources['beers'].schema,
            fake_with={
                'abv': {'valid': lambda: pemi.data.fake.pydecimal(2, 2, positive=True)},
                'price': {'valid': lambda: pemi.data.fake.pydecimal(2, 2, positive=True)}
            }
        )

        beer_sales_table = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity | name          | style |
            | -       | -          | -        | -             | -     |
            | {b[1]}  | 01/01/2017 | 3        | SpinCyle      | IPA   |
            | {b[2]}  | 01/02/2017 | 3        | OldStyle      | Pale  |
            | {b[3]}  | 01/03/2017 | 5        | Pipewrench    | IPA   |
            | {b[4]}  | 01/04/2017 | 8        | AbstRedRibbon | Lager |
            | {b[5]}  | 01/04/2017 | 6        |               |       |
            | {b[1]}  | 01/06/2017 | 1        | SpinCyle      | IPA   |
            '''.format(b = scenario.case_keys.cache('beer_sales', 'beer_id')),
            schema=pipe.targets['beer_sales'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['dumb_sales'], sales_table),
            pt.when.example_for_source(scenario.sources['dumb_beers'], beers_table)
        ).then(
            pt.then.target_matches_example(scenario.targets['beer_sales'], beer_sales_table)
        )


    @pytest.mark.scenario(scenario)
    def test_scenario(self, case):
        case.assert_case()
