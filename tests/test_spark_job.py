import os
import unittest

import pandas as pd
import pyspark

import pemi
import pemi.testing
from pemi.data_subject import SparkDataSubject
from pemi.fields import *

import sys
this = sys.modules[__name__]


spark = pyspark.sql.SparkSession \
    .builder \
    .master("spark://spark-master:7077") \
    .appName("PemiSpark") \
    .config("spark.sql.warehouse.dir", "/tmp/data/spark-warehouse") \
    .getOrCreate()


class DenormalizeBeersPipe(pemi.Pipe):
    def config(self):
        self.source(
            SparkDataSubject,
            name='sales',
            schema=pemi.Schema(
                beer_id  = IntegerField(),
                sold_at  = DateField(format='%m/%d/%Y'),
                quantity = IntegerField()
            ),
            spark=spark
        )

        self.source(
            SparkDataSubject,
            name='beers',
            schema = pemi.Schema(
                id    = IntegerField(),
                name  = StringField(),
                style = StringField(),
                abv   = FloatField(),
                price = DecimalField(precision=16, scale=2)
            ),
            spark=spark
        )

        self.target(
            SparkDataSubject,
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
            spark=spark
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



class TestDenormalizeBeersPipe(unittest.TestCase):
    def setUp(self):
        self.pipe = DenormalizeBeersPipe()

        self.rules = pemi.testing.Rules(
            source_subjects=[
                self.pipe.sources['sales'],
                self.pipe.sources['beers']
            ],
            target_subjects=[self.pipe.targets['beer_sales']]
        )

        self.scenario = pemi.testing.Scenario(
            runner=self.pipe.flow,
            source_subjects=[
                self.pipe.sources['sales'],
                self.pipe.sources['beers']
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
            schema=self.pipe.sources['sales'].schema.merge(pemi.Schema(bumpkin=StringField())),
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
                source_subject=self.pipe.sources['sales']
            ),
            self.rules.when_example_for_source(
                self.example_beers(),
                source_subject=self.pipe.sources['beers']
            )
        ).then(
            self.rules.then_target_matches_example(
                self.example_beer_sales(),
                target_subject=self.pipe.targets['beer_sales'],
                by=['beer_id', 'sold_at']
            )
        )
        return self.scenario.run()

if __name__ == '__main__':
    job = DenormalizeBeersPipe()
    job.flow()
