import os
import logging

import factory
import sqlalchemy as sa
import dask.threaded

import pemi
import pemi.testing as pt
from pemi.data_subject import SaDataSubject
from pemi.fields import *

logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

SA_ENGINE = sa.create_engine( #pylint: disable=invalid-name
    'postgresql://{user}:{password}@{host}/{dbname}'.format(
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'),
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DB')
    )
)

SCHEMAS = { #pylint: disable=invalid-name
    'sales': pemi.Schema(
        beer_id=IntegerField(),
        sold_at=DateField(format='%m/%d/%Y'),
        quantity=IntegerField()
    ),
    'beers': pemi.Schema(
        id=IntegerField(),
        name=StringField(),
        style=StringField(),
        abv=FloatField(),
        price=DecimalField(precision=14, scale=2)
    ),
    'beer_sales': pemi.Schema(
        beer_id=IntegerField(),
        name=StringField(),
        style=StringField(),
        sold_at=DateField(format='%m/%d/%Y'),
        quantity=IntegerField(),
        unit_price=DecimalField(precision=16, scale=2),
        sell_price=DecimalField(precision=16, scale=2)
    ),
}

# Setup (in real jobs, table creation done in some other task)
with SA_ENGINE.connect() as conn:
    conn.execute(
        '''
        DROP TABLE IF EXISTS renamed_sales;
        DROP TABLE IF EXISTS renamed_beers;

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

        DROP TABLE IF EXISTS renamed_sales;
        CREATE TABLE renamed_sales (LIKE sales);
        DROP TABLE IF EXISTS renamed_beers;
        CREATE TABLE renamed_beers (LIKE beers);
        '''
    )


# This pipe just renames a tables and is contrived to show
# how queries can be run in parallel via Dask.
class RenameSaPipe(pemi.Pipe):
    def __init__(self, *, schema, table, **params):
        super().__init__(**params)

        self.schema = schema
        self.table = table

        self.source(
            SaDataSubject,
            name='main',
            schema=self.schema,
            engine=SA_ENGINE,
            table=self.table
        )

        self.target(
            SaDataSubject,
            name='main',
            schema=self.schema,
            engine=SA_ENGINE,
            table='renamed_{}'.format(self.table)
        )

    def flow(self):
        with SA_ENGINE.connect() as conn:
            conn.execute(
                '''
                DROP TABLE IF EXISTS renamed_{table};
                CREATE TABLE renamed_{table} (LIKE {table});
                INSERT INTO renamed_{table} (SELECT * FROM {table});
                '''.format(table=self.table)
            )

        self.targets['main'] = self.sources['main']




class DenormalizeBeersJob(pemi.Pipe):
    def __init__(self):
        super().__init__()

        self.pipe(
            name='renamed_sales',
            pipe=RenameSaPipe(
                schema=SCHEMAS['sales'],
                table='sales'
            )
        )
        self.connect('renamed_sales', 'main').to('denormalizer', 'sales')

        self.pipe(
            name='renamed_beers',
            pipe=RenameSaPipe(
                schema=SCHEMAS['beers'],
                table='beers'
            )
        )
        self.connect('renamed_beers', 'main').to('denormalizer', 'beers')

        self.pipe(
            name='denormalizer',
            pipe=DenormalizeBeersPipe()
        )

        self.target(
            SaDataSubject,
            name='beer_sales',
            schema=SCHEMAS['beer_sales'],
            engine=SA_ENGINE,
            table='beer_sales'
        )

    def flow(self):
        self.connections.flow(dask_get=dask.threaded.get)


class DenormalizeBeersPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.source(
            SaDataSubject,
            name='sales',
            schema=SCHEMAS['sales'],
            engine=SA_ENGINE,
            table='renamed_sales'
        )

        self.source(
            SaDataSubject,
            name='beers',
            schema=SCHEMAS['beers'],
            engine=SA_ENGINE,
            table='renamed_beers'
        )

        self.target(
            SaDataSubject,
            name='beer_sales',
            schema=SCHEMAS['beer_sales'],
            engine=SA_ENGINE,
            table='beer_sales'
        )


    def flow(self):
        with SA_ENGINE.connect() as conn:
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
                    renamed_sales sales
                  LEFT JOIN
                    renamed_beers beers
                  ON
                    sales.beer_id = beers.id
                );

                DROP TABLE IF EXISTS renamed_sales;
                DROP TABLE IF EXISTS renamed_beers;
                '''
            )

class SalesKeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence(lambda n: n)

class BeersKeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence(lambda n: n)


denormalize_beers_pipe = DenormalizeBeersJob()
pt.mock_pipe(denormalize_beers_pipe, 'renamed_sales')
pt.mock_pipe(denormalize_beers_pipe, 'renamed_beers')

with pt.Scenario(
    name='DenormalizeBeersJob',
    pipe=denormalize_beers_pipe,
    factories={
        'sales': SalesKeyFactory,
        'beers': BeersKeyFactory
    },
    sources={
        'renamed_sales': lambda pipe: pipe.pipes['renamed_sales'].targets['main'],
        'renamed_beers': lambda pipe: pipe.pipes['renamed_beers'].targets['main']
    },
    targets={
        'beer_sales': lambda pipe: pipe.targets['beer_sales']
    },
    target_case_collectors={
        'beer_sales': pt.CaseCollector(subject_field='beer_id', factory='beers', factory_field='id')
    }
) as scenario:

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
            '''.format(
                b=scenario.factories['beers']['id']
            ),
            schema=SCHEMAS['sales']
        )

        beers_table = pemi.data.Table(
            '''
            | id     | name          | style |
            | -      | -             | -     |
            | {b[1]} | SpinCyle      | IPA   |
            | {b[2]} | OldStyle      | Pale  |
            | {b[3]} | Pipewrench    | IPA   |
            | {b[4]} | AbstRedRibbon | Lager |
            '''.format(
                b=scenario.factories['beers']['id']
            ),
            schema=SCHEMAS['beers']
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
            '''.format(
                b=scenario.factories['beers']['id']
            ),
            schema=SCHEMAS['beer_sales']
        )

        case.when(
            pt.when.example_for_source(scenario.sources['renamed_sales'], sales_table),
            pt.when.example_for_source(scenario.sources['renamed_beers'], beers_table)
        ).then(
            pt.then.target_matches_example(scenario.targets['beer_sales'], beer_sales_table)
        )
