'''
Package containing core Pemi functionality
'''

import logging
logging.basicConfig()
logging.getLogger('pemi').setLevel(logging.WARN)
def log(name='pemi'):
    return logging.getLogger(name)


from pemi.schema import Schema
from pemi.field import Field

from pemi.pipes.pipe import Pipe
from pemi.pipes.pipe import SourcePipe
from pemi.pipes.pipe import TargetPipe

from pemi.field_map import FieldMap
from pemi.field_map import Handler
