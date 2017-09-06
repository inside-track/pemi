import os
import unittest

import pandas as pd
import sqlalchemy as sa

import pemi
import pemi.testing
import pemi.pipes.dask
from pemi.data_subject import SaDataSubject

import logging
pemi.log('pemi').setLevel(logging.WARN)
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


    # conn.execute(
    #     '''
    #     INSERT INTO sales VALUES
    #       (1,'2017-01-01',1, 'x'),
    #       (2,'2017-01-01',3, 'y'),
    #       (4,'2017-01-02',3, 'z'),
    #       (2,'2017-01-03',5, 'flerg'),
    #       (3,'2017-01-03',7, 'blerg'),
    #       (3,'2017-01-04',1, 'blorf'),
    #       (1,'2017-01-10',2, 'glorf')
    #     ;

    #     INSERT INTO beers VALUES
    #       (1,'SpinCycle','IPA',7.2,4.99),
    #       (2,'OldStyle','Pale',4.2,3.99),
    #       (3,'Pipewrench','IPA',8.3,10.99),
    #       (4,'AbstRedRibbon','Lager',3.2,1.99)
    #     ;
    #     '''
    # )


# This is a really dumb pipe.  It just copies a table to dumb_table.  It's meant to demo
# how queries can be run in parallel via Dask.
class DumbSaPipe(pemi.Pipe):
    def __init__(self, **params):
        self.schema = pemi.Schema(params['schema'])
        self.table = params['table']
        self.sa_engine = sa.create_engine(params['engine_opts']['conn_str'])

        super().__init__(**params)

    def config(self):
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
    def config(self):
        sa_engine = sa.create_engine(this.params['sa_conn_str'])

        self.source(
            SaDataSubject,
            name='sales',
            schema={
                'beer_id':  {'ftype': 'integer', 'required': True},
                'sold_at':  {'ftype': 'date', 'in_format': '%m/%d/%Y', 'required': True},
                'quantity': {'ftype': 'integer', 'required': True}
            },
            engine=sa_engine,
            table='dumb_sales'
        )

        self.source(
            SaDataSubject,
            name='beers',
            schema={
                'id':       {'ftype': 'integer', 'required': True},
                'name':     {'ftype': 'string', 'required': True},
                'style':    {'ftype': 'string'},
                'abv':      {'ftype': 'float'},
                'price':    {'ftype': 'decimal', 'precision': 16, 'scale': 2}
            },
            engine=sa_engine,
            table='dumb_beers'
        )

        self.target(
            SaDataSubject,
            name='beer_sales',
            schema={
                'beer_id':    {'ftype': 'integer', 'required': True},
                'name':       {'ftype': 'string'},
                'style':      {'ftype': 'string'},
                'sold_at':    {'ftype': 'date', 'in_format': '%m/%d/%Y', 'required': True},
                'quantity':   {'ftype': 'integer', 'required': True},
                'unit_price': {'ftype': 'decimal', 'precision': 16, 'scale': 2},
                'sell_price': {'ftype': 'decimal', 'precision': 16, 'scale': 2}
            },
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

        self.pipe(
            name='self',
            pipe=self
        )

        self.connect(
            self.pipes['dumb_sales'].targets['main']
        ).to(
            self.pipes['self'].sources['sales']
        )

        self.connect(
            self.pipes['dumb_beers'].targets['main']
        ).to(
            self.pipes['self'].sources['beers']
        )

        self.dask = pemi.pipes.dask.DaskFlow(self.connections)

    def flow(self):
        self.dask.flow()

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


class TestDenormalizeBeersPipe(unittest.TestCase):
    def setUp(self):
        self.pipe = DenormalizeBeersPipe()

        self.pipe.pipes['dumb_sales'] = pemi.testing.mock_pipe(self.pipe.pipes['dumb_sales'])
        self.pipe.pipes['dumb_beers'] = pemi.testing.mock_pipe(self.pipe.pipes['dumb_beers'])

        self.rules = pemi.testing.Rules(
            source_subjects=[
                self.pipe.pipes['dumb_sales'].targets['main'],
                self.pipe.pipes['dumb_beers'].targets['main']
            ],
            target_subjects=[self.pipe.targets['beer_sales']]
        )

        self.scenario = pemi.testing.Scenario(
            runner=self.pipe.flow,
            source_subjects=[
                self.pipe.pipes['dumb_sales'].targets['main'],
                self.pipe.pipes['dumb_beers'].targets['main']
            ],
            target_subjects=[self.pipe.targets['beer_sales']],
            givens=self.rules.when_sources_conform_to_schemas()
        )


    def example_sales(self):
        sales_table = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity |
            | -       | -          | -        |
            | 1       | 01/01/2017 | 3        |
            | 2       | 01/02/2017 | 3        |
            | 3       | 01/03/2017 | 5        |
            | 4       | 01/04/2017 | 8        |
            | 5       | 01/04/2017 | 6        |
            | 1       | 01/06/2017 | 1        |
            ''',
            schema=self.pipe.sources['sales'].schema.merge(pemi.Schema({'bumpkin': {'ftype': 'string'}})),
            fake_with={
                'beer_id': { 'valid': lambda: pemi.data.fake.random_int(1,4) },
                'sold_at': { 'valid': lambda: pemi.data.fake.date_time_this_decade().date() },
                'quantity': {'valid': lambda: pemi.data.fake.random_int(1,100) },
                'bumpkin': { 'valid': lambda: pemi.data.fake.word(['bumpkin A', 'bumpkin B', 'bumpkin C']) }
            }
        )
        return sales_table


    def example_beers(self):
        beers_table = pemi.data.Table(
            '''
            | id | name          | style |
            | -  | -             | -     |
            | 1  | SpinCyle      | IPA   |
            | 2  | OldStyle      | Pale  |
            | 3  | Pipewrench    | IPA   |
            | 4  | AbstRedRibbon | Lager |
            ''',
            schema=self.pipe.sources['beers'].schema,
            fake_with={
                'abv': {'valid': lambda: pemi.data.fake.pydecimal(2, 2, positive=True)},
                'price': {'valid': lambda: pemi.data.fake.pydecimal(2, 2, positive=True)}
            }
        )
        return beers_table

    def example_beer_sales(self):
        beer_sales_table = pemi.data.Table(
            '''
            | beer_id | sold_at    | quantity | name          | style |
            | -       | -          | -        | -             | -     |
            | 1       | 01/01/2017 | 3        | SpinCyle      | IPA   |
            | 2       | 01/02/2017 | 3        | OldStyle      | Pale  |
            | 3       | 01/03/2017 | 5        | Pipewrench    | IPA   |
            | 4       | 01/04/2017 | 8        | AbstRedRibbon | Lager |
            | 5       | 01/04/2017 | 6        |               |       |
            | 1       | 01/06/2017 | 1        | SpinCyle      | IPA   |
            ''',
            schema=self.pipe.targets['beer_sales'].schema
        )
        return beer_sales_table


    def test_it_joins_sales_to_beers(self):
        self.scenario.when(
            self.rules.when_example_for_source(
                self.example_sales(),
                source_subject=self.pipe.pipes['dumb_sales'].targets['main']
            ),
            self.rules.when_example_for_source(
                self.example_beers(),
                source_subject=self.pipe.pipes['dumb_beers'].targets['main']
            )
        ).then(
            self.rules.then_target_matches_example(
                self.example_beer_sales(),
                target_subject=self.pipe.targets['beer_sales']
            )
        )
        return self.scenario.run()

if __name__ == '__main__':
    job = DenormalizeBeersPipe()
    job.flow()
