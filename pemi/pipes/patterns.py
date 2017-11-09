import pandas as pd

import pemi
from pemi.data_subject import PdDataSubject

class SourcePipe(pemi.Pipe):
    '''
    A source pipe extracts data from some external source, and parses
    it into a data structure that can be used by subsequent pipes.
    '''
    def __init__(self, *, schema, **params):
        super().__init__(**params)

        self.schema = schema

        self.target(
            pemi.PdDataSubject,
            name='main',
            schema=self.schema
        )

        self.target(
            pemi.PdDataSubject,
            name='errors',
            # TODO: Merge this with standard error fields
            schema=self.schema
        )

    def extract(self):
        #e.g., S3SourceExtractor.extract()
        raise NotImplementedError
        return extracted_data_that_can_be_parsed

    def parse(self, data):
        #e.g., CsvParser.parse(data)
        raise NotImplementedError
        return parsed_data

    def flow(self):
        self.parse(self.extract())


class TargetPipe(pemi.Pipe):
    '''
    A target pipe takes data provided to it, encodes it into a structure that can be
    understand by some external target, and then loads the data into that external target.
    '''

    def __init__(self, *, schema, **params):
        super().__init__(**params)

        self.schema = schema

        self.source(
            pemi.PdDataSubject,
            name='main',
            schema=self.schema
        )

        self.target(
            pemi.PdDataSubject,
            name='errors'
        )

        self.target(
            pemi.PdDataSubject,
            name='response'
        )

    def encode(self):
        #e.g., CsvTargetEncoder.encode()
        raise NotImplementedError
        return source_main_data_encoded_for_loader

    def load(self, encoded_data):
        #e.g., S3Loader.load()
        raise NotImplementedError
        return results_from_load_operation

    def flow(self):
        self.load(self.encode())


class ForkPipe(pemi.Pipe):
    ''' A fork pipe accepts a single source and delivers it to multiple named targets '''
    def __init__(self, *, subject_class=pemi.PdDataSubject, forks=[], **params):
        super().__init__(**params)

        self.source(subject_class, name='main')
        for fork in forks:
            self.target(subject_class, name=fork)

    def fork(self, source, target):
        raise NotImplementedError

    def flow(self):
        raise NotImplementedError


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
