from pathlib import Path

import pandas as pd

import pemi
import pemi.pipes.csv
import pemi.pipes.dask

class JoinSalesToBeersPipe(pemi.Pipe):
    def config(self):
# Maybe we only need the *REQUIRED* fields in the schemas for data subjects?
# OR!!! We don't even really need the schemas if they're just "pass-through"
# Schemas are used to do data-validation and formatting.  If we don't need to
# do validation or formatting , we don't need the schema.

# Blarg..... but we do need the schemas in order to stub the data.


# So... here's what it might look like if we did default values....

        self.source(
            name='sales',
            schema={
                'beer_id': {'type': 'integer', 'required': True}
            }
        )

        self.source(
            name='beers',
            schema={
                'id':       {'type': 'integer', 'required': True},
                'style_id': {'type': 'string'}
            }
        )

        self.target(
            name='joined',
            schema={
                'beer_id':  {'type': 'integer', 'required': True},
                'style_id': {'type': 'string'}
            }
        )

        self.target(
            name='styles',
            schema={
                'style_id': {'type': 'string'}
            }
        )

    def flow(self):
        sales_df = self.sources['sales'].data
        beers_df = self.sources['beers'].data
        joined_df = pd.merge(sales_df, beers_df, how='inner', left_on='beer_id', right_on='id')
        self.targets['joined'].data = joined_df

        self.targets['styles'].data = joined_df[['style_id']]


class LookupStylePipe(pemi.Pipe):
    def config(self):
        self.source(
            name='ids',
            schema={
                'style_id': {'type': 'string'}
            }
        )

        self.target(
            name='names',
            schema={
                'style': {'type': 'string'}
            }

        )

        self.style_dict = {
            '1': 'IPA',
            '2': 'Pale',
            '3': 'Stout'
        }

    def lookup(self, key):
        result = self.style_dict.get(key)
        if result == None:
            result = 'Unknown id {}'.format(key)
        return result

    def flow(self):
        self.targets['names'].data = pd.DataFrame()
        self.targets['names'].data['style'] = self.sources['ids'].data['style_id'].apply(self.lookup)



class AddLookupPipe(pemi.Pipe):
    def config(self):
        self.source(
            name='joined',
            schema={
                'beer_id':  {'type': 'integer', 'required': True},
                'sold_at':  {'type': 'date', 'in_format': '%d/%m/%Y', 'required': True},
            }
        )

        self.source(
            name='style',
            schema={
                'style': {'type': 'string'}
            }
        )

        self.target(
            name='beer_sales',
            schema={
                'beer_id':    {'type': 'integer', 'required': True},
                'sold_at':    {'type': 'date', 'in_format': '%d/%m/%Y', 'required': True},
                'style':      {'type': 'string'}
            }
        )

    def flow(self):
        beer_sales_df = self.sources['joined'].data
        beer_sales_df['style'] = self.sources['style'].data['style']
        self.targets['beer_sales'].data = beer_sales_df



class MyJob(pemi.Pipe):
    def config(self):
        self.schemas = {
            'sources': {
                'sales_file': {
                    'beer_id':  {'type': 'integer', 'required': True},
                    'sold_at':  {'type': 'date', 'in_format': '%m/%d/%Y', 'required': True},
                    'quantity': {'type': 'integer', 'required': True}
                },
                'beers_file': {
                    'id':       {'type': 'integer', 'required': True},
                    'name':     {'type': 'string', 'required': True},
                    'style_id': {'type': 'string'},
                    'abv':      {'type': 'float'},
                    'price':    {'type': 'decimal', 'precision': 16, 'scale': 2}
                }
            },
            'targets': {
                'beer_sales_file': {
                    'beer_id':    {'type': 'integer', 'required': True},
                    'name':       {'type': 'string', 'required': True},
                    'style':      {'type': 'string'},
                    'sold_at':    {'type': 'date', 'in_format': '%m/%d/%Y', 'required': True},
                    'quantity':   {'type': 'integer', 'required': True},
                    'unit_price': {'type': 'decimal', 'precision': 16, 'scale': 2},
                    'sell_price': {'type': 'decimal', 'precision': 16, 'scale': 2}
                }
            }
        }



        # This "job" doesn't really have sources/targets
        # It's just a pipe connector
        # So, how do I deal with schemas?  Particularly the intermediate ones?
        #   We could put them in the parameters of the pipes
        #   Or make a distinction between required and inferred schemas.....

        # Named pipes
        # Call these pipes?  Pipes in pipes
        self.pipe(
            name='sales_file',
            pipe=pemi.pipes.csv.LocalCsvFileSourcePipe(
                schema=self.schemas['sources']['sales_file'],
                paths=[Path(__file__).parent / Path('fixtures') / Path('sales.csv')],
                csv_opts={
                    'sep': '|'
                }
            )
        )

        self.pipe(
            name='beers_file',
            pipe=pemi.pipes.csv.LocalCsvFileSourcePipe(
                schema=self.schemas['sources']['beers_file'],
                paths=[Path(__file__).parent / Path('fixtures') / Path('beers.csv')],
                csv_opts={
                    'sep': '|'
                }
            )
        )

        self.pipe(
            name='join_sales_to_beers',
            pipe=JoinSalesToBeersPipe()
        )

        self.pipe(
            name='lookup_style',
            pipe=LookupStylePipe()
        )

        self.pipe(
            name='add_lookup',
            pipe=AddLookupPipe()
        )

        self.pipe(
            name='beer_sales_file',
            pipe=pemi.pipes.csv.LocalCsvFileTargetPipe(
                schema = self.schemas['targets']['beer_sales_file'],
                path='beer_sales.csv'
            )
        )


        # Connections
        self.connect(
            self.pipes['sales_file'], 'main'
        ).to(
            self.pipes['join_sales_to_beers'], 'sales'
        )

        self.connect(
            self.pipes['beers_file'], 'main'
        ).to(
            self.pipes['join_sales_to_beers'], 'beers'
        )

        self.connect(
            self.pipes['join_sales_to_beers'], 'joined'
        ).to(
            self.pipes['add_lookup'], 'joined'
        )

        self.connect(
            self.pipes['join_sales_to_beers'], 'styles'
        ).to(
            self.pipes['lookup_style'], 'ids'
        )

        self.connect(
            self.pipes['lookup_style'], 'names'
        ).to(
            self.pipes['add_lookup'], 'style'
        )

        self.connect(
            self.pipes['add_lookup'], 'beer_sales'
        ).to(
            self.pipes['beer_sales_file'], 'main'
        )

        self.dask = pemi.pipes.dask.DaskFlow(self.connections)


    def flow(self):
        self.dask.flow()
