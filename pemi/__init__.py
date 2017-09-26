'''
Package containing core Pemi functionality
'''
from pemi.version import __version__ as version

import logging
logging.basicConfig()
logging.getLogger('pemi').setLevel(logging.WARN)
def log(name='pemi'):
    return logging.getLogger(name)


from pemi.schema import Schema

from pemi.pipe import Pipe
