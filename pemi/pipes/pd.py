import pandas as pd

import pemi
import pemi.pipes.patterns
from pemi.pd_mapper import *

class PdForkPipe(pemi.pipes.patterns.ForkPipe):
    def flow(self):
        for target in self.targets.values():
            target.df = self.sources['main'].df


class PdConcatPipe(pemi.pipes.patterns.ConcatPipe):
    def __init__(self, *, concat_opts={}, **params):
        super().__init__(**params)
        self.concat_opts = concat_opts


    def flow(self):
        source_dfs = [source.df for source in self.sources.values()]
        self.targets['main'].df = pd.concat(source_dfs, **self.concat_opts)

class PdLookupJoinPipe(pemi.Pipe):
    def __init__(self, main_key, lookup_key, suffixes=('', '_lkp'), missing_handler=None, indicator='_indicator_', lookup_prefix='', **kwargs):
        super().__init__(**kwargs)

        self.main_key = main_key
        self.lookup_key = lookup_key
        self.suffixes = suffixes
        self.missing_handler = missing_handler or RowHandler('exclude')
        self.indicator = indicator
        self.lookup_prefix = lookup_prefix

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
        lkp_df = self.sources['lookup'].df
        lkp_df.rename(
            columns={col: self.lookup_prefix + col for col in lkp_df.columns if not col in self.lookup_key},
            inplace=True
        )

        uniq_lkp_df = lkp_df.sort_values(
            self.lookup_key
        ).groupby(
            self.lookup_key
        ).first().reset_index()

        merged_df = pd.merge(
            self.sources['main'].df,
            uniq_lkp_df,
            left_on = self.main_key,
            right_on = self.lookup_key,
            how='left',
            suffixes=self.suffixes,
            indicator=self.indicator
        )

        def raise_on_mismatch(row):
            if row[self.indicator] == 'left_only':
                raise KeyError('Lookup key "{}" not found'.format(dict(row[self.main_key])))
            return row


        mapper = PdMapper(merged_df, mapped_df=merged_df, maps=[
            PdMap(source=[*self.main_key, self.indicator], transform=raise_on_mismatch, handler=self.missing_handler)
        ]).apply()

        del mapper.mapped_df[self.indicator]
        self.targets['main'].df = mapper.mapped_df
        self.targets['errors'].df = mapper.errors_df
