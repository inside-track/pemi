import pandas as pd

import pemi
import pemi.transforms

__all__ = [
    'RowHandler',
    'PdMapper',
    'PdMap'
]


class RowHandler:
    def __init__(self, mode='raise', recode=None):
        # ignore - Errors get returned as None and they don't go into the errors datadaset
        # warn - Errors get returned as None, but they still go into the errors datadaset
        # exclude - The entire record is excluded from the results, put into the errors dataset
        # recode - Alternate transform applied, records still go into the errors dataset
        # raise - Full stop, all subsequent processing halts

        self.mode = mode
        self.recode = recode
        self.errors = []

        self.handler = getattr(self, '_{}'.format(mode))

    def apply(self, transform, arg, idx):
        try:
            return transform(arg)
        except Exception as err:
            return self.handler(err, arg, idx)

    def mapping_error(self, err, idx):
        return {'mode': self.mode, 'index': idx, 'type': err.__class__.__name__, 'message': str(err)}

    def catch_error(self, err, idx):
        self.errors.append(self.mapping_error(err, idx))

    def _raise(self, err, arg, idx):
        pemi.log.error(self.mapping_error(err, idx))
        raise err

    def _recode(self, err, arg, idx):
        self.catch_error(err, idx)
        return self.recode(arg)

    def _warn(self, err, arg, idx):
        self.catch_error(err, idx)
        pemi.log.warning(self.mapping_error(err, idx))
        return None

    def _ignore(self, err, arg, idx):
        return None

    def _exclude(self, err, arg, idx):
        self.catch_error(err, idx)
        pemi.log.error(self.mapping_error(err, idx))
        return None


class PdMapper():
    def __init__(self, source_df, mapped_df=None, maps=[]):
        self.source_df = source_df
        if mapped_df is None:
            self.mapped_df = pd.DataFrame(index=self.source_df.index)
        else:
            self.mapped_df = mapped_df

        self.errors_df = pd.DataFrame([])
        self.maps = maps

    def apply(self):
        for pdmap in self.maps:
            pdmap(source_df=self.source_df, mapped_df=self.mapped_df)

        self.collect_errors()
        self.build_errors_df()
        self.exclude_errors()
        return self

    def collect_errors(self):
        self.errors = [err for pdmap in self.maps for err in pdmap.handler.errors]

    def build_errors_df(self):
        index = [err['index'] for err in self.errors]
        df = pd.DataFrame(self.errors, index=index)
        renamer = {k: '__error_{}__'.format(k) for k in df.columns}
        df.rename(columns=renamer, inplace=True)

        self.errors_df = df.join(self.source_df, how='left')

    def exclude_errors(self):
        exclude_idx = set([err['index'] for err in self.errors if err['mode'] == 'exclude'])
        if len(exclude_idx) > 0:
            self.mapped_df.drop(exclude_idx, inplace=True)


class PdMap():
    def __init__(self, source=[], target=[], transform=None, handler=None, **kwargs):
        if isinstance(source, str):
            self.source = source
            source = [source]

        if isinstance(target, str):
            self.target = target
            target = [target]

        self.sources = list(source)
        self.targets = list(target)
        self.transform = transform
        self.handler = handler or RowHandler()
        self.kwargs = kwargs

        if len(self.sources) == 1 and len(self.targets) == 1 and self.transform == None:
            self.apply = getattr(self, '_apply_copy')
        elif len(self.sources) == 1 and len(self.targets) == 1:
            self.apply = getattr(self, '_apply_one_to_one')
        elif len(self.sources) == 1 and len(self.targets) == 0:
            self.apply = getattr(self, '_apply_one_to_zero')
        elif len(self.sources) == 0 and len(self.targets) == 1:
            self.apply = getattr(self, '_apply_zero_to_one')
        elif len(self.targets) == 1:
            self.apply = getattr(self, '_apply_many_to_one')
        else:
            self.apply = getattr(self, '_apply_many_to_many')

    def _transform(self, arg):
        return self.transform(arg)

    def _transform_one_to_one(self, row):
        return self.handler.apply(self._transform, row[self.source], row.name)

    def _transform_zero_to_one(self, row):
        return self.handler.apply(self._transform, row['__none__'], row.name)

    def _transform_many_to_one(self, row):
        return self.handler.apply(self._transform, row, row.name)

    def _transform_many_to_many(self, row):
        return self.handler.apply(self._transform, row, row.name)

    def _apply_copy(self):
        self.mapped_df[self.target] = self.source_df[self.source].copy()

    def _apply_one_to_one(self):
        self.mapped_df[self.target] = self.source_df[[self.source]].apply(self._transform_one_to_one, axis=1)

    def _apply_zero_to_one(self):
        empty_df = pd.DataFrame([], index=self.source_df.index)
        empty_df['__none__'] = None
        self.mapped_df[self.target] = empty_df.apply(self._transform_zero_to_one, axis=1)

    def _apply_one_to_zero(self):
        self.source_df[[self.source]].apply(self._transform_one_to_one, axis=1)

    def _apply_many_to_one(self):
        self.mapped_df[self.target] = self.source_df[self.sources].apply(self._transform_many_to_one, axis=1)

    def _apply_many_to_many(self):
        work_df = self.source_df[self.sources].apply(self._transform_many_to_many, axis=1)

        for target in self.targets:
            self.mapped_df[target] = work_df[target]

    def __call__(self, source_df, mapped_df=None, error_df=None):
        self.source_df = source_df
        self.mapped_df = mapped_df
        self.error_df = error_df
        self.apply()
        return self

def schema_maps(schema):
    field_maps = []
    for name, field in schema.items():
        fm = PdMap(source=name, target=name,
                   transform=field.coerce,
                   handler=RowHandler('exclude')
        )
        field_maps.append(fm)

        if field.metadata.get('allow_null', True) == False:
            fm = PdMap(source=name, target=name,
                       transform=pemi.transforms.validate_no_null(field),
                       handler=RowHandler('exclude')
            )
            field_maps.append(fm)
    return field_maps


if __name__ == '__main__':
    df = pd.DataFrame({
        'num': [1,2,3,4],
        'name': ['one', 'two', 'three', 'four']
    })

    def awful(row):
        if row[0] == 2:
            raise ValueError('haha, it''s 2')
        return '{}-{}'.format(*row)

    def invert(row):
#        if row[0] == 2:
#            raise ValueError('haha, it''s owt')
        row['mun'] = row['name']
        row['eman'] = row['num']
        return row

    pdmap = lambda **kwargs: PdMap(**{**{'handler': RowHandler(mode='exclude')},**kwargs})
    mapper = PdMapper(df, maps=[
        pdmap(source='name', target='name_copy'),
        pdmap(source='name', target='name_star', transform=lambda v: '*{}*'.format(v)),
        pdmap(source=['num', 'name'], target='name_num', transform=lambda row: '{}-{}'.format(*row)),
        pdmap(source=['num', 'name'], target=['mun', 'eman'], transform=invert, handler=RowHandler(mode='raise')),
        pdmap(source=['num', 'name'], target='name_num', transform=awful),
    ]).apply()

    print(mapper.mapped_df)
    print(mapper.errors)
    print(mapper.errors_df)
