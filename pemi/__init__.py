'''
Package containing core Pemi functionality
'''
from pemi.version import __version__ as version

import logging
logging.getLogger('pemi').setLevel(logging.WARN)
def log(name='pemi'):
    return logging.getLogger(name)

import pemi.schema
from pemi.schema import Schema

import pemi.pipe
from pemi.pipe import Pipe

import pemi.fields
from pemi.fields import *

import pemi.data_subject
from pemi.data_subject import *

import pemi.transforms
