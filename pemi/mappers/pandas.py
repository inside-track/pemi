from collections import OrderedDict
import pandas as pd

class UncaughtMappingError(RuntimeError): pass
class PandasMapper:
    def __init__(self, df, *fieldmaps):
        self.df = df
        self.fieldmaps = fieldmaps

        self.mapped_df = pd.DataFrame([])
        self.mapping_errors = OrderedDict()
        self.errors_df = pd.DataFrame([])

    def execute(self, raise_errors=True):
        for fieldmap in self.fieldmaps:
            fieldmap.many_to_many_wrapper = self.many_to_many_wrapper()
            fieldmap.work_frame_builder = self.work_frame_builder

            fieldmap.apply(self.df, self.mapped_df)
            for idx, err in fieldmap.handler.errors.items():
                if idx not in self.mapping_errors:
                    self.mapping_errors[idx] = [[]]
                self.mapping_errors[idx][0].append(err)

        if raise_errors and len(self.mapping_errors) > 0:
            raise UncaughtMappingError(self.mapping_errors)
        else:
            return (self._mapped_df(), self._errors_df())

    def many_to_many_wrapper(self):
        return lambda row: pd.Series(row)

    def work_frame_builder(self, index):
        return pd.DataFrame(index=index)

    def _mapped_df(self):
        caught_idx = []
        for idx, errs in self.mapping_errors.items():
            has_caught = any([True for err in errs[0] if err['mode'] == 'catch'])
            if has_caught:
                caught_idx.append(idx)

        mapped_idx = [idx for idx in self.df.index if idx not in caught_idx]
        self.mapped_df = self.mapped_df.loc[mapped_idx].astype(object)
        return self.mapped_df


    def _errors_df(self):
        if len(self.mapping_errors) > 0:
            self.errors_df = self.df.loc[self.mapping_errors.keys()]
            self.errors_df['__mapping_errors__'] = pd.DataFrame(self.mapping_errors).transpose()[0].astype(object)

        return self.errors_df
