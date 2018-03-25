import random
import os
import pemi
import pemi.pd_mapper
import boto3
import pandas as pd

import pemi.pipes.patterns
import sqlalchemy as sa
import pemi.pipes.sa
from pandas import DataFrame
from pemi.fields import *

class DwExtractorLoaderPipe(pemi.pipes.patterns.SourcePipe):
    # add params for microservice and table and chunk size
    def __init__(self, *, sql, engine, table, **params):
        super().__init__(**params)
        self.bucket = 'itk-redshift-staging'
        self.sql = sql
        self.table = table
        self.engine = engine
        self.field_maps = pemi.pd_mapper.schema_maps(self.schema)

    def extract(self):
        pemi.log.info("Extracting '%s' via:\n%s", self.name, self.sql)
        folder_name = os.path.join(pemi.work_dir, '%030x' % random.randrange(16**30))
        os.mkdir(folder_name)
        ret_dict = {'files' : []}

        with self.engine.connect() as conn:

            for chunk in  pd.read_sql(self.sql, conn, chunksize=30000):
                #pemi.log.info('downloading a chunk of size %i'% len(chunk))
                file_name = os.path.join(folder_name, '%030x' % random.randrange(16**30) + '.csv')
                ret_dict['files'].append(file_name)
                chunk = self.clean_fields(chunk)

                chunk.to_csv(
                    file_name,
                    index=False,
                    date_format='%Y-%m-%d %H:%M:%S.%f',
                    sep='|', escapechar="\\"
                )
        pemi.log.info('returning file handle')
        return ret_dict

    def load(self, ret_dict):

        pemi.log.info(ret_dict)
        for file in ret_dict['files']:
            key = 'de-jobs/{}/{}'.format(self.table, '/'.join(file.split('/')[3:]))
            boto3.resource('s3').Bucket(self.bucket).upload_file(file, key)
            pemi.log.info('Archiving job to s3://%s/%s', self.bucket, key)

        return '/'.join(key.split('/')[:-1])

    def copy(self, folder_key):

        sa_engine = sa.create_engine(
            'redshift://{user}:{password}@{host}:{port}/{dbname}'.format(
                user=os.getenv('ITK_DW_USERNAME'),
                password=os.getenv('ITK_DW_PASSWORD'),
                host=os.getenv('ITK_DW_DB_HOST'),
                dbname=os.getenv('ITK_DW_DB_NAME'),
                port=os.getenv('ITK_DW_PORT')
            )
        )
        updated_col_sql = self.create_sql_column_list()
        temp_table_name = "working_data_" + self.table
        table_complete = "de_test.%s" % self.table
            #  ESCAPE
            # REMOVEQUOTES
        sql_str = '''
          BEGIN;
          drop table if exists {temp_table_name};
          CREATE TEMPORARY TABLE {temp_table_name} ({update_cols});

          COPY {copy_table_name} ({field_list})
            FROM 's3://itk-redshift-staging/{folder_key}'
            CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_access_key}'
            REGION 'us-west-2'

            DELIMITER AS '|'
            IGNOREHEADER AS 1
            CSV
          ;

          DROP TABLE IF EXISTS {table_complete};
          CREATE TABLE {table_complete} ({update_cols});
          INSERT INTO {table_complete} (SELECT * FROM {temp_table_name});
          drop table if exists {temp_table_name};

          END;
        '''.format(
            temp_table_name=temp_table_name,
            update_cols=updated_col_sql,
            copy_table_name=temp_table_name,
            field_list=', '.join(self.schema.keys()),
            folder_key=folder_key,
            access_key=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            table_complete=table_complete
        )
        pemi.log.info(sql_str)

        pipe = pemi.pipes.sa.SaSqlSourcePipe(
            engine=sa_engine,
            sql=sql_str,
            schema=self.schema
        )
        pipe.flow()

    def clean_fields(self, chunk):
        for k in self.schema.keys():
            if isinstance(self.schema[k], IntegerField):
                chunk[k] = pd.Series(chunk[k].apply(self.write_int), dtype='object')
            elif isinstance(self.schema[k], BooleanField):
                chunk[k] = pd.Series(chunk[k].apply(lambda x: int(x)), dtype='int32')
        return chunk

    def write_int(self, v):
        if pemi.transforms.isblank(v):
            return ''
        else:
            return str(int(v))

    def create_sql_column_list(self):

        ret_arr = []
        for scheme in self.schema.keys():
            val = scheme + ' ' + self.schema[scheme].get_type_as('redshift')
            ret_arr.append(val)
        return ', '.join(ret_arr)


    def flow(self):
        self.copy(self.load(self.extract()))
