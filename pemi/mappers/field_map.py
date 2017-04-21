from pemi.mappers.handler import Handler

class FieldMap:
    def __init__(self, source=None, target=None, transform=None, handler=Handler()):
        if isinstance(source, str):
            source = (source,)
        self.sources = source

        if isinstance(target, str):
            target = (target,)
        self.targets = target

        self.transform = transform
        self.handler = handler

        ## Must be set by the Mapper
        # Function to define how the many-to-many mapper converts a dict into a dataframe row
        self.many_to_many_wrapper = None
        # Function to define how to build a temp working dataframe (needs index)
        self.work_frame_builder = None

    def apply(self, source_df, target_df):
        work_df = self._build_work_df(source_df, target_df)

        if len(self.sources) == 1 and len(self.targets) == 1:
            target_df[self.targets[0]] = work_df.apply(self._one_to_one_map(), axis=1)
        elif len(self.targets) == 1:
            target_df[self.targets[0]] = work_df.apply(self._many_to_one_map(), axis=1)
        else:
            result_df = work_df.apply(self._many_to_many_map(), axis=1)
            for target in self.targets:
                target_df[target] = result_df[target]

        return None

    def _build_work_df(self, source_df, target_df):
        work_df = self.work_frame_builder(target_df.index)
        for source in self.sources:
            if source in target_df:
                work_df[source] = target_df[source]
            else:
                work_df[source] = source_df[source]
        return work_df

    def _one_to_one_map(self):
        func = lambda row: self.handler.apply(self.transform, row[self.sources[0]], row.name)
        return func

    def _many_to_one_map(self):
        func = lambda row: self.handler.apply(self.transform, row, row.name)
        return func

    def _many_to_many_map(self):
        wrapped = lambda inrow: self.many_to_many_wrapper(self.transform(inrow))
        func = lambda row: self.handler.apply(wrapped, row, row.name)
        return func
