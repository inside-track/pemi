import pandas as pd

import pemi
from pemi.data_subject import PdDataSubject

class SourcePipe(pemi.Pipe):
    '''
    A source pipe extracts data from some external source, and parses
    it into a data structure that can be used by subsequent pipes.
    '''

    def __init__(self, subject_class=PdDataSubject, **params):
        super().__init__(**params)

        self.target(subject_class, name='main')
        self.target(subject_class, name='errors')

    def extract(self):
        #e.g., S3SourceExtractor.extract()
        raise NotImplementedError
        return extracted_data_that_can_be_parsed

    def parse(self, data):
        #e.g., CsvParser.parse(data)
        raise NotImplementedError
        return parsed_data

    def flow(self):
        self.targets['main'].data = self.parse(self.extract())
        return self.targets['main'].data


class TargetPipe(pemi.Pipe):
    '''
    A target pipe takes data provided to it, encodes it into a structure that can be
    understand by some external target, and then loads the data into that external target.
    '''

    def __init__(self, subject_class=PdDataSubject, **params):
        super().__init__(**params)

        self.source(subject_class, name='main')
        self.target(subject_class, name='load_response')

    def encode(self):
        #e.g., CsvTargetEncoder.encode()
        raise NotImplementedError
        return source_main_data_encoded_for_loader

    def load(self, encoded_data):
        #e.g., S3Loader.load()
        raise NotImplementedError
        return results_from_load_operation

    def flow(self):
        return self.load(self.encode())


class ForkPipe(pemi.Pipe):
    ''' A fork pipe accepts a single source and delivers it to multiple named targets '''
    def __init__(self, subject_class=pemi.PdDataSubject, forks=[], **params):
        super().__init__(**params)

        self.source(subject_class, name='main')
        for fork in forks:
            self.target(subject_class, name=fork)

    def fork(self, source, target):
        raise NotImplementedError

    def flow(self):
        raise NotImplementedError


class PdForkPipe(ForkPipe):
    def flow(self):
        for target in self.targets.values():
            target.df = self.sources['main'].df


class ConcatPipe(pemi.Pipe):
    ''' A concat pipe accepts multiple sources and combines them into a single target '''
    def __init__(self, subject_class=pemi.PdDataSubject, sources=[], **params):
        super().__init__(**params)
        self.named_sources = sources

        for source in sources:
            self.source(subject_class, name=source)
        self.target(subject_class, name='main')

    def flow(self):
        raise NotImplementedError

class PdConcatPipe(ConcatPipe):
    def flow(self):
        opts = self.params.get('concat_opts', {})

        source_dfs = [source.df for source in self.sources.values()]
        self.targets['main'].df = pd.concat(source_dfs, **opts)
