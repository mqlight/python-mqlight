import logging
import os

ENTRY_IND = '>-----------------------------------------------------------'
EXIT_IND = '<-----------------------------------------------------------'
FFDC_BANNER = '+------------------------------------------------------------------------------+'

startLevel = None
historyLevel = None
ffdcSequence = 0
ALL = 20000
DATA_OFTEN = 20000
EXIT_OFTEN = 20000
ENTRY_OFTEN = 20000
DEBUG = 800
EMIT = 1000
DATA = 1500
PARMS = 1500
EXIT = 3000
ENTRY = 3000
ENTRY_EXIT = 3000
ERROR = 5000
FFDC = 10000

NO_CLIENT_ID = '*'

def get_logger(name):
    return MQLightLog(name)

class MQLightLog():

    def __init__(self, name, ):
        self._log = logging.getLogger(__name__)
        start_level = os.getenv('MQLIGHT_PYTHON_LOG') if os.getenv('MQLIGHT_PYTHON_LOG') is not None else ALL
        self._log.setLevel(start_level)
        self._stack = ['<stack unwind error>']

    def _write(self, level, prefix, *args):
        if self._log.getEffectiveLevel() >= level:
            print str(level) + ' ' + prefix + ':', args

    def set_level(self, level):
        self._log.setLevel(level)

    def get_level(self):
        return self._log

    def entry(self, name, id):
        self._entry_level(ENTRY, name, id)

    def entry_often(self, name, id):
        self._entry_level(ENTRY_OFTEN, name , id)

    def _entry_level(self, level, name, id):
        self._write(level, ENTRY_IND[0:len(self._stack)], name)
        #if level < 20000:
        #    self._stack.append(name)

    def exit(self, name, id, rc):
        self._exit_level(EXIT, name, id, rc)

    def exit_often(self, name, id, rc):
        self._exit_level(EXIT_OFTEN, name , id, rc)

    def _exit_level(self, level, name, id, rc):
        self._write(level, EXIT_IND[0:len(self._stack)-1], name, id, rc)
        #if level < 20000:
        #    if len(self._stack) == 1:
        #       self.ffdc('MQLightLog._exit_level', 10, id, name)
        #    last = self._stack.pop()
        #    while last != name:
        #        if len(self._stack) == 1:
        #            self.ffdc('MQLightLog._exit_level', 10, id, name)
        #        last = self._stack.pop()

    def ffdc(self, name, probe_id, id, data):
        print 'ffdc'

    def debug(self, id, message):
        self._write(DEBUG, id, message)

    def parms(self, id, *args):
        self._write(PARMS, id, args)

    def data(self, id, *args):
        self._write(DATA, id, args)

    def emit(self, name, id, event, *args):
        self._write(EMIT, name, id, event, args)

    def error(self, name, id, *args):
        self._write(ERROR, name, id, args)

