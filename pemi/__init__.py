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
from pemi.data_subject import DataSubject
from pemi.data_subject import DataSource
from pemi.data_subject import DataTarget

from pemi.pipes.generic import Pipe
from pemi.pipes.generic import SourcePipe
from pemi.pipes.generic import TargetPipe

from pemi.field_map import FieldMap
from pemi.field_map import Handler
