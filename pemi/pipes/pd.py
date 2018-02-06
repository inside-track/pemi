import pandas as pd

import pemi
import pemi.pipes.patterns
import pemi.transforms
from pemi.pd_mapper import *

class PdForkPipe(pemi.pipes.patterns.ForkPipe):
    def flow(self):
        for target in self.targets.values():
            target.df = self.sources['main'].df.copy()


class PdConcatPipe(pemi.pipes.patterns.ConcatPipe):
    def __init__(self, *, concat_opts={}, **params):
        super().__init__(**params)
        self.concat_opts = concat_opts


    def flow(self):
        source_dfs = [source.df for source in self.sources.values() if source.df is not None]
        if len(source_dfs) == 0:
            self.targets['main'].df = pd.DataFrame()
        else:
            self.targets['main'].df = pd.concat(source_dfs, **self.concat_opts)

# TODOC: Note that RowHandler('recode') will not work here
class PdLookupJoinPipe(pemi.Pipe):
    def __init__(self, main_key, lookup_key,
                 suffixes=('', '_lkp'),
                 missing_handler=None,
                 indicator=None,
                 lookup_prefix='',
                 fillna=None,
                 **kwargs): #pylint: disable=too-many-arguments
        super().__init__(**kwargs)

        self.main_key = main_key
        self.lookup_key = lookup_key
        self.suffixes = suffixes
        self.missing_handler = missing_handler or RowHandler('exclude')
        self.indicator = indicator
        self.lookup_prefix = lookup_prefix
        self.fillna = fillna

        self.source(
            pemi.PdDataSubject,
            name='main'
        )

        self.source(
            pemi.PdDataSubject,
            name='lookup'
        )

        self.target(
            pemi.PdDataSubject,
            name='main'
        )

        self.target(
            pemi.PdDataSubject,
            name='errors'
        )

    def flow(self):
        pemi.log.debug('PdLookupJoinPipe - main source columns: %s',
                       self.sources['main'].df.columns)
        pemi.log.debug('PdLookupJoinPipe - main lookup columns: %s',
                       self.sources['lookup'].df.columns)

        lkp_df = self.sources['lookup'].df

        if self.lookup_prefix != '':
            lkp_df = lkp_df.rename(
                columns={col: self.lookup_prefix + col
                         for col in lkp_df.columns if not col in self.lookup_key},
            )

        missing_keys = lkp_df[self.lookup_key].apply(
            lambda v: v.apply(pemi.transforms.isblank).any(), axis=1
        )
        if len(missing_keys) > 0:
            lkp_df = lkp_df[~missing_keys]

        uniq_lkp_df = lkp_df.sort_values(
            self.lookup_key
        ).groupby(
            self.lookup_key
        ).first().reset_index()

        merged_df = pd.merge(
            self.sources['main'].df,
            uniq_lkp_df,
            left_on=self.main_key,
            right_on=self.lookup_key,
            how='left',
            suffixes=self.suffixes,
            indicator='__indicator__'
        )

        def raise_on_mismatch(row):
            if row['__indicator__'] == 'left_only':
                raise KeyError('Lookup key "{}" not found'.format(dict(row[self.main_key])))
            return row


        mapper = PdMapper(merged_df, mapped_df=merged_df, maps=[
            PdMap(source=[*self.main_key, '__indicator__'], transform=raise_on_mismatch,
                  handler=self.missing_handler)
        ]).apply()

        if not self.indicator:
            del mapper.mapped_df['__indicator__']
        else:
            indicator_map = lambda v: True if v == 'both' else False
            mapper.mapped_df[self.indicator] = mapper.mapped_df['__indicator__'].apply(
                indicator_map
            ).astype('bool')

        if self.fillna:
            mapper.mapped_df.fillna(**self.fillna, inplace=True)

        self.targets['main'].df = mapper.mapped_df
        self.targets['errors'].df = mapper.errors_df

        pemi.log.debug('PdLookupJoinPipe - main target columns: %s',
                       self.targets['main'].df.columns)



class PdFieldValueForkPipe(pemi.Pipe):
    def __init__(self, field, forks, **kwargs):
        super().__init__(**kwargs)

        self.field = field
        self.forks = forks

        self.source(
            pemi.PdDataSubject,
            name='main'
        )

        for fork in self.forks:
            self.target(
                pemi.PdDataSubject,
                name=fork
            )

        self.target(
            pemi.PdDataSubject,
            name='remainder'
        )

    def flow(self):
        grouped = self.sources['main'].df.groupby(self.field)

        for fork in self.forks:
            if fork in grouped.groups:
                self.targets[fork].df = grouped.get_group(fork).copy()
            else:
                self.targets[fork].df = pd.DataFrame(columns=self.sources['main'].df.columns)

        remainder = set(grouped.groups.keys()) - set(self.forks)
        if len(remainder) > 0:
            self.targets['remainder'].df = pd.concat(
                [grouped.get_group(r) for r in remainder]
            ).sort_index()
