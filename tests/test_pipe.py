import unittest

import pemi
from pemi import Pipe
from pemi import SourcePipe
from pemi import TargetPipe
from pemi.data_subject import PdDataSubject

class DummySourcePipe(SourcePipe):
    def __init__(self, **params):
        super().__init__(**params)

        dummy_schema = pemi.Schema({
            'id':   { 'ftype': 'integer', 'required': True },
            'name': { 'ftype': 'string' }
        })

        self.targets['standard'] = PdDataSubject(dummy_schema)

    def execute(self):
        self.targets['standard'].data = {
            'id': [1, 2, 3],
            'name': ['one', 'two', 'three']
        }

class TestSourcePipe(unittest.TestCase):
    def test_execute(self):
        pipe = DummySourcePipe()
        pipe.execute()

        expected = {
            'id': [1, 2, 3],
            'name': ['one', 'two', 'three']
        }
        self.assertEqual(pipe.targets['standard'].data, expected)



class DummyTargetPipe(TargetPipe):
    def __init__(self, **params):
        super().__init__(**params)

        dummy_schema = pemi.Schema({
            'id':   { 'ftype': 'integer', 'required': True },
            'name': { 'ftype': 'string' }
        })

        self.sources['standard'] = PdDataSubject(dummy_schema)

    def execute(self):
        result = []
        for (key, values) in sorted(self.sources['standard'].data.items()):
            result.append("Key is {key}, values is {values}".format(key=key, values=values))

        return "\n".join(result)

class TestTargetPipe(unittest.TestCase):
    def test_execute(self):
        pipe = DummyTargetPipe()

        pipe.sources['standard'].data = {
            'id': [1, 2, 3],
            'name': ['one', 'two', 'three']
        }

        expected = "\n".join(
            [
                "Key is id, values is [1, 2, 3]",
                "Key is name, values is ['one', 'two', 'three']",
            ]
        )
        self.assertEqual(pipe.execute(), expected)



class DummyApiPipe(Pipe):
    def __init__(self, **params):
        super().__init__(**params)

        self.sources['standard'] = PdDataSubject({
            'number': { 'ftype': 'integer' }
        })

        self.targets['standard'] = PdDataSubject({
            'number': { 'ftype': 'integer' }
        })

    def execute(self):
        'Add one to the input data'

        self.targets['standard'].data = []
        for value in self.sources['standard'].data:
            self.targets['standard'].data.append(value + 1)


class TestApiPipe(unittest.TestCase):
    def test_execute(self):
        pipe = DummyApiPipe()
        pipe.sources['standard'].data = [1,2,3]

        expected = [2,3,4]

        pipe.execute()
        self.assertEqual(pipe.targets['standard'].data, expected)
