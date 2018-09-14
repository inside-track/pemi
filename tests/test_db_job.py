import os
import sys
import logging

import factory
import sqlalchemy as sa

import pemi
import pemi.testing as pt
from pemi.data_subject import SaDataSubject
from pemi.fields import *

logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

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
        '''
    )



class DenormalizeBeersPipe(pemi.Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        sa_engine = sa.create_engine(this.params['sa_conn_str'])

        self.source(
            SaDataSubject,
            name='sales',
            schema=pemi.Schema(
                beer_id=IntegerField(),
                sold_at=DateField(format='%m/%d/%Y'),
                quantity=IntegerField()
            ),
            engine=sa_engine,
            table='sales'
        )

        self.source(
            SaDataSubject,
            name='beers',
            schema=pemi.Schema(
                #pylint: disable=bad-whitespace
                id    = IntegerField(),
                name  = StringField(),
                style = StringField(),
                abv   = FloatField(),
                price = DecimalField(precision=16, scale=2)
                #pylint: enable=bad-whitespace

            ),
            engine=sa_engine,
            table='beers'
        )

        self.target(
            SaDataSubject,
            name='beer_sales',
            schema=pemi.Schema(
                #pylint: disable=bad-whitespace
                beer_id    = IntegerField(),
                name       = StringField(),
                style      = StringField(),
                sold_at    = DateField(format='%m/%d/%Y'),
                quantity   = IntegerField(),
                unit_price = DecimalField(precision=16, scale=2),
                sell_price = DecimalField(precision=16, scale=2)
                #pylint: enable=bad-whitespace
            ),
            engine=sa_engine,
            table='beer_sales'
        )

    def flow(self):
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
                    sales
                  LEFT JOIN
                    beers
                  ON
                    sales.beer_id = beers.id
                );
                '''
            )

class BeersKeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence(lambda n: n)


with pt.Scenario(
    name='DenormalizeBeersPipe',
    pipe=DenormalizeBeersPipe(),
    factories={
        'beers': BeersKeyFactory
    },
    sources={
        'sales': lambda pipe: pipe.sources['sales'],
        'beers': lambda pipe: pipe.sources['beers']
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
            schema=scenario.sources['sales'].schema
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
            schema=scenario.sources['beers'].schema.merge(pemi.Schema(
                abv=DecimalField(faker=lambda: pemi.data.fake.pydecimal(2, 2, positive=True)),
                price=DecimalField(faker=lambda: pemi.data.fake.pydecimal(2, 2, positive=True)),
            ))
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
            schema=scenario.targets['beer_sales'].schema
        )

        case.when(
            pt.when.example_for_source(scenario.sources['sales'], sales_table),
            pt.when.example_for_source(scenario.sources['beers'], beers_table)
        ).then(
            pt.then.target_matches_example(scenario.targets['beer_sales'], beer_sales_table)
        )
