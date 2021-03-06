import pytest
import factory

import pyspark

import pemi
import pemi.testing as pt
from pemi.data_subject import SparkDataSubject
from pemi.fields import *

pytestmark = pytest.mark.spark

class DenormalizeBeersPipe(pemi.Pipe):
    def __init__(self, spark_session, **params):
        super().__init__(**params)

        self.source(
            SparkDataSubject,
            name='sales',
            schema=pemi.Schema(
                beer_id=IntegerField(),
                sold_at=DateField(format='%m/%d/%Y'),
                quantity=IntegerField()
            ),
            spark=spark_session
        )

        self.source(
            SparkDataSubject,
            name='beers',
            schema=pemi.Schema(
                id=IntegerField(),
                name=StringField(),
                style=StringField(),
                abv=FloatField(),
                price=DecimalField(precision=16, scale=2)
            ),
            spark=spark_session
        )

        self.target(
            SparkDataSubject,
            name='beer_sales',
            schema=pemi.Schema(
                beer_id=IntegerField(),
                name=StringField(),
                style=StringField(),
                sold_at=DateField(format='%m/%d/%Y'),
                quantity=IntegerField(),
                unit_price=DecimalField(precision=16, scale=2),
                sell_price=DecimalField(precision=16, scale=2)
            ),
            spark=spark_session
        )

    def flow(self):
        self.sources['sales'].df.createOrReplaceTempView('sales')
        self.sources['beers'].df.createOrReplaceTempView('beers')

        self.targets['beer_sales'].df = self.targets['beer_sales'].spark.sql('''
            SELECT
              sales.beer_id,
              beers.name,
              beers.style,
              sales.sold_at,
              sales.quantity,
              CAST(beers.price AS DECIMAL(16,2)) AS unit_price,
              CAST(beers.price * sales.quantity AS DECIMAL(16,2)) AS sell_price
            FROM
              sales
            LEFT JOIN
              beers
            ON
              sales.beer_id = beers.id
        ''')

        self.targets['beer_sales'].df.createOrReplaceTempView('beer_sales')

class BeersKeyFactory(factory.Factory):
    class Meta:
        model = dict
    id = factory.Sequence(lambda n: n)

spark_session = pyspark.sql.SparkSession \
    .builder \
    .master("spark://spark-master:7077") \
    .appName("PemiSpark") \
    .config("spark.sql.warehouse.dir", "/tmp/data/spark-warehouse") \
    .getOrCreate()

with pt.Scenario(
    name='DenormalizeBeersPipe',
    pipe=DenormalizeBeersPipe(spark_session),
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
            schema=scenario.sources['beers'].schema
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
            pt.then.target_matches_example(scenario.targets['beer_sales'], beer_sales_table,
                                           by=['beer_id', 'sold_at'])
        )
