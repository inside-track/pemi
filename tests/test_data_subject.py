import os

import pytest

import sqlalchemy as sa
import pandas as pd
from pandas.util.testing import assert_frame_equal

import pemi
from pemi.fields import *

class TestPdDataSubject():
    def test_it_creates_an_empty_dataframe(self):
        dsubj = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f2=StringField()
        ))

        assert_frame_equal(dsubj.df, pd.DataFrame(columns=['f1', 'f2']))

    def test_it_raises_schema_invalid_on_connection(self):
        ds1 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f2=StringField()
        ))

        ds2 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f3=StringField()
        ))

        ds1.df = pd.DataFrame({
            'f1': [1, 2, 3],
            'f2': [4, 5, 6]
        })

        with pytest.raises(pemi.data_subject.MissingFieldsError):
            ds2.connect_from(ds1)


    def test_it_catch_inexact_schema(self):
        ds1 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f2=StringField()
            ),
            strict_match_schema=True)
        ds1.df = pd.DataFrame({
            'f1': [1, 2, 3],
            'f2': [1, 2, 3],
            'f3': [1, 2, 3]
            })
        with pytest.raises(pemi.data_subject.MissingFieldsError):
            ds1.validate_schema()

    def test_it_pass_exact_df_and_scema(self):
        ds1 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f2=StringField(),
            f3=StringField()
            ),
            strict_match_schema=True)
        ds1.df = pd.DataFrame({
            'f1': [1, 2, 3],
            'f2': [1, 2, 3],
            'f3': [1, 2, 3]
            })
        assert ds1.validate_schema()



    def test_it_creates_an_empty_df_with_schema_when_connected_to_empty(self):
        ds1 = pemi.PdDataSubject()

        ds2 = pemi.PdDataSubject(schema=pemi.Schema(
            f1=StringField(),
            f3=StringField()
        ))

        ds2.connect_from(ds1)
        assert_frame_equal(ds2.df, pd.DataFrame(columns=['f1', 'f3']))

class TestSaDataSubject:

    @pytest.fixture
    def sa_engine(self):
        return sa.create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
            user=os.environ.get('POSTGRES_USER'),
            password=os.environ.get('POSTGRES_PASSWORD'),
            host=os.environ.get('POSTGRES_HOST'),
            dbname=os.environ.get('POSTGRES_DB')
        ))

    @pytest.fixture
    def sa_subject(self, sa_engine):
        return pemi.SaDataSubject(
            engine=sa_engine,
            schema=pemi.Schema(
                json_field=JsonField()
            ),
            table='some_data'
        )

    @pytest.fixture(autouse=True)
    def dbconn(self, sa_engine):
        with sa_engine.connect() as conn:
            conn.execute(
                '''
                DROP TABLE IF EXISTS some_data;
                CREATE TABLE some_data (
                  json_field JSON
                );
                '''
            )

        yield

        with sa_engine.connect() as conn:
            conn.execute('DROP TABLE IF EXISTS some_data;')


    def test_it_loads_some_json_data(self, sa_subject):
        df = pd.DataFrame({
            'json_field': [{'a': 'alpha', 'three': 3}] * 2
        })
        sa_subject.from_pd(df)

        assert_frame_equal(df, sa_subject.to_pd())
