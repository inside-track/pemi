import pandas as pd

import pemi
import pemi.pipes.patterns
import pemi.transforms

class PdForkPipe(pemi.pipes.patterns.ForkPipe):
    def flow(self):
        for target in self.targets.values():
            target.df = self.sources['main'].df.copy()


class PdConcatPipe(pemi.pipes.patterns.ConcatPipe):
    def __init__(self, *, concat_opts=None, **params):
        concat_opts = concat_opts or {}
        super().__init__(**params)
        self.concat_opts = concat_opts


    def flow(self):
        source_dfs = [source.df for source in self.sources.values() if source.df is not None]
        if len(source_dfs) == 0:
            self.targets['main'].df = pd.DataFrame()
        else:
            self.targets['main'].df = pd.concat(source_dfs, **self.concat_opts, sort=False)

class PdLookupJoinPipe(pemi.Pipe):
    def __init__(self, main_key, lookup_key,
                 suffixes=('', '_lkp'),
                 on_missing='redirect', #redirect, ignore, warn
                 indicator=None,
                 lookup_prefix='',
                 fillna=None,
                 **kwargs): #pylint: disable=too-many-arguments
        super().__init__(**kwargs)

        self.main_key = main_key
        self.lookup_key = lookup_key
        self.suffixes = suffixes
        self.on_missing = on_missing
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


    def _drop_lookup_duplicates(self, _na):
        return self.sources['lookup'].df.drop_duplicates(self.lookup_key)

    def _prefix_lookup_columns(self, lkp_df):
        if self.lookup_prefix == '':
            return lkp_df

        return lkp_df.rename(columns={
            col: self.lookup_prefix + col
            for col in lkp_df.columns if not col in self.lookup_key
        })

    def _drop_missing_lookup_keys(self, lkp_df):
        missing_keys = lkp_df[self.lookup_key].apply(
            lambda v: v.apply(pemi.transforms.isblank).any(), axis=1
        )
        if len(missing_keys) > 0:
            return lkp_df[~missing_keys]
        return lkp_df

    def _perform_lookup_merge(self, lkp_df):
        return pd.merge(
            self.sources['main'].df,
            lkp_df,
            left_on=self.main_key,
            right_on=self.lookup_key,
            how='left',
            suffixes=self.suffixes,
            indicator='__indicator__'
        )

    def _fill_missing(self, merged_df):
        if self.fillna:
            merged_df['__indicator__'] = merged_df['__indicator__'].astype('str')
            merged_df.fillna(**self.fillna, inplace=True)
        return merged_df


    def _direct_targets(self, merged_df):
        matches = (merged_df['__indicator__'] == 'both')
        if self.on_missing == 'redirect':
            self.targets['main'].df = merged_df[matches].copy()
            self.targets['errors'].df = merged_df[~matches].copy()
        else:
            self.targets['main'].df = merged_df
            self.targets['errors'].df = pd.DataFrame([], columns=merged_df.columns)

        if self.on_missing == 'warn':
            merged_df[~matches][self.main_key].apply(
                lambda row: pemi.log.warning('No lookup values found for %s', dict(row)),
                axis=1
            )

    def _remove_indicator(self, _na):
        if self.indicator:
            indicator_bool = self.targets['main'].df['__indicator__'].apply(
                lambda v: v == 'both'
            ).astype('bool')

            self.targets['main'].df[self.indicator] = indicator_bool

        del self.targets['main'].df['__indicator__']
        del self.targets['errors'].df['__indicator__']

    def flow(self):
        pemi.log.debug('PdLookupJoinPipe - main source columns: %s',
                       self.sources['main'].df.columns)
        pemi.log.debug('PdLookupJoinPipe - main lookup columns: %s',
                       self.sources['lookup'].df.columns)

        arg = None
        for func in [
                self._drop_lookup_duplicates,
                self._prefix_lookup_columns,
                self._drop_missing_lookup_keys,
                self._perform_lookup_merge,
                self._fill_missing,
                self._direct_targets,
                self._remove_indicator
        ]:
            arg = func(arg)


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
        else:
            self.targets['remainder'].df = pd.DataFrame(columns=self.sources['main'].df.columns)

class PdLambdaPipe(pemi.Pipe):
    '''
    This pipe is used to build quick Pandas transformations where building a full pipe class
    may feel like overkill.  You would use this pipe if you don't need to test it in isolation
    (e.g., it only makes sense in a larger context), or you don't need control over the schemas.

    Args:
      fun (function): A function that accepts a dataframe as argument (source) and returns
        a dataframe (target).

    :Data Sources:
      **main** (*pemi.PdDataSubject*) - The source dataframe that gets pass to ``fun``.

    :Data Targets:
      **main** (*pemi.PdDataSubject*) - The target dataframe that gets populated from
        the return value of ``fun``.

    '''
    def __init__(self, fun):
        super().__init__()

        self.fun = fun

        self.source(
            pemi.PdDataSubject,
            name='main'
        )

        self.target(
            pemi.PdDataSubject,
            name='main'
        )

    def flow(self):
        self.targets['main'].df = self.fun(self.sources['main'].df)
