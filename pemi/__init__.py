'''
Package containing core Pemi functionality
'''
import logging
#pylint: disable=wrong-import-position
log = logging.getLogger('pemi') #pylint: disable=invalid-name
log.setLevel(logging.WARN)

work_dir = '.' #pylint: disable=invalid-name
#pylint: enable=wrong-import-position

import pandas
import pandas_mapper

from pemi.version import __version__ as version


import pemi.schema
from pemi.schema import Schema

import pemi.pipe
from pemi.pipe import Pipe

import pemi.fields
from pemi.fields import *

import pemi.data_subject
from pemi.data_subject import *

import pemi.transforms
