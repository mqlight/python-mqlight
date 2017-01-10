# <copyright
# notice="lm-source-program"
# pids="5724-H72"
# years="2013,2016"
# crc="99677977">
# Licensed Materials - Property of IBM
#
# 5725-P60
#
# (C) Copyright IBM Corp. 2013, 2016
#
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
# </copyright>
from __future__ import absolute_import
import logging
import logging.handlers
import os
import sys
import signal
import socket
import platform
import threading
import traceback
import inspect
from time import localtime, gmtime, strftime
from pkg_resources import get_distribution, DistributionNotFound


ENTRY_IND = '>-----------------------------------------------------------'
EXIT_IND = '<-----------------------------------------------------------'
HEADER_BANNER = '+---------------------------------------------------------'\
                '---------------------+'

ALL = 100
EXIT_OFTEN = 200
ENTRY_OFTEN = 300
PARMS_OFTEN = 300
DEBUG = 400
FROM = 500
STATE = 800
DATA = 1000
PARMS = 1200
ENTRY_EXIT = 1500
EXIT = 1501
ENTRY = 1502
ERROR = 1800
FFDC = 2000

LEVELS = {
    ALL: 'ALL',
    EXIT_OFTEN: 'EXIT_OFTEN',
    ENTRY_OFTEN: 'ENTRY_OFTEN',
    DEBUG: 'DEBUG',
    FROM: 'FROM',
    STATE: 'STATE',
    DATA: 'DATA',
    PARMS: 'PARMS',
    EXIT: 'EXIT',
    ENTRY: 'ENTRY',
    ENTRY_EXIT: 'ENTRY_EXIT',
    ERROR: 'ERROR',
    FFDC: 'FFDC',
    'ALL': ALL,
    'EXIT_OFTEN': EXIT_OFTEN,
    'ENTRY_OFTEN': ENTRY_OFTEN,
    'DEBUG': DEBUG,
    'FROM': FROM,
    'STATE': STATE,
    'DATA': DATA,
    'PARMS': PARMS,
    'EXIT': EXIT,
    'ENTRY': ENTRY,
    'ENTRY_EXIT': ENTRY_EXIT,
    'ERROR': ERROR,
    'FFDC': FFDC
}

try:
    __version__ = get_distribution('mqlight').version
except DistributionNotFound:
    VERSION = 1.0

# Set the level of logging. By default only 'ffdc' entries will be logged, but
# this can be altered by setting the environment variable MQLIGHT_PYTHON_LOG to
# one of the defined levels.
DEFAULT_LEVEL = 'FFDC'

# Set the logging stream. By default stderr will be used, but this can be
# changed to stdout by setting the environment variable
# MQLIGHT_PYTHON_LOG_STREAM=stdout.
DEFAULT_STREAM = 'stderr'

# Set the amount of message data that will get logged. The default is 10
# megabytes, but this can be altered by setting the environment variable
# MQLIGHT_PYTHON_LOG_SIZE to a different number.
DEFAULT_LOG_SIZE = 10 * 1000 * 1000

NO_CLIENT_ID = '*'
IS_WIN = os.name == 'nt'
LOCK = threading.Lock()


def get_logger(name):
    return MQLightLog(name)


def _get_depth():
    return len(traceback.format_stack()) - 5


class MQLightLog(object):

    """
    Python logging wrapper
    """

    def __init__(self, name):
        self._log = logging.getLogger(name)
        self._level = LEVELS.get(
            os.getenv('MQLIGHT_PYTHON_LOG', DEFAULT_LEVEL).upper())
        if self._level is None:
            self._level = FFDC
        self._ffdc_sequence = 0

        # File Handler
        self._fd = None
        # stderr Handler
        self._fe = None
        # stdout Handler
        self._fo = None

        if os.getenv('MQLIGHT_PYTHON_NO_HANDLER') is None:
            # Set up a signal handler that will cause an ffdc to be generated
            # when the signal is caught. Set the environment variable
            # MQLIGHT_PYTHON_NO_HANDLER to stop the signal handler being
            # registered.
            if IS_WIN:
                sig = signal.SIGBREAK
            else:
                sig = signal.SIGUSR2

            signal.signal(sig, self._signal_handler)

        self._log_size = os.getenv('MQLIGHT_PYTHON_LOG_SIZE', DEFAULT_LOG_SIZE)
        self._stream = os.getenv('MQLIGHT_PYTHON_LOG_STREAM', DEFAULT_STREAM)
        formatter = logging.Formatter(
            '%(asctime)s [%(process)s:%(threadName)s] - %(name)s - '
            '%(client_id)s - %(lvl)s %(message)s')
        ffdc_formatter = logging.Formatter('%(message)s')

        self.multi_formatter = MultiFormatter(formatter, ffdc_formatter)

        self._set_handler(self._stream, self._log_size, self.multi_formatter)

    def show_trace_header(self):
        """
        Display the trace header information
        """
        keys = {'client_id': NO_CLIENT_ID}
        self._write(ENTRY_EXIT, HEADER_BANNER, keys)
        self._write(ENTRY_EXIT,
                    '| Trace started',
                    keys)
        self._write(ENTRY_EXIT,
                    "| Client version: {}".format(__version__),
                    keys)
        self._write(ENTRY_EXIT, HEADER_BANNER, keys)

    def _signal_handler(self, signum, frame):
        """
        Register signal handlers
        """
        self.ffdc(signum, 255, NO_CLIENT_ID, 'User-requested FFDC on signal')

        # Start logging at the 'debug' level if we're not doing so, or turn off
        # logging if we already are
        if DEFAULT_LEVEL > DEBUG:
            if self._level == DEFAULT_LEVEL:
                self._level = DEBUG
            else:
                self._level = DEFAULT_LEVEL

    def _set_handler(self, stream, log_size, formatter):
        """
        Set the appropriate logging handlers
        """
        if stream == 'stderr':
            if not self._fe:
                # Log to stderr
                self._fe = logging.StreamHandler(sys.stderr)
                self._fe.setFormatter(formatter)
                self._log.addHandler(self._fe)

            # Remove previous Handlers
            if self._fd:
                self._log.removeHandler(self._fd)
                self._fd = None
            if self._fo:
                self._log.removeHandler(self._fo)
                self._fo = None
        elif stream == 'stdout':
            if not self._fo:
                # Log to stdout
                self._fo = logging.StreamHandler(sys.stdout)
                self._fo.setFormatter(formatter)
                self._log.addHandler(self._fo)

            # Remove previous Handlers
            if self._fd:
                self._log.removeHandler(self._fd)
                self._fd = None
            if self._fe:
                self._log.removeHandler(self._fe)
                self._fe = None
        else:
            # Remove previous Handlers
            if self._fd:
                self._log.removeHandler(self._fd)
                self._fd = None
            if self._fe:
                self._log.removeHandler(self._fe)
                self._fe = None
            if self._fo:
                self._log.removeHandler(self._fo)
                self._fo = None

            # A file has been specified. As well as writing to stderr, we
            # additionally write the output to a file.
            self._fd = logging.handlers.RotatingFileHandler(
                stream, 'a', log_size, 0)
            self._fd.setFormatter(formatter)
            self._fe = logging.StreamHandler(sys.stderr)
            self._fe.setFormatter(formatter)
            self._log.addHandler(self._fd)
            self._log.addHandler(self._fe)

    def _write(self, level, message, extra):
        if self._level <= level:
            extra['lvl'] = LEVELS.get(level)
            self._log.log(level, message, extra=extra)

    def set_level(self, level):
        self._log.setLevel(level)

    def get_level(self):
        return self._log

    def entry(self, name, client_id):
        self._entry_level(ENTRY, name, client_id)
        self._from_where(FROM, name, client_id)

    def entry_often(self, name, client_id):
        self._entry_level(ENTRY_OFTEN, name, client_id)

    def _entry_level(self, level, name, client_id):
        with LOCK:
            msg = '{0} {1}'.format(ENTRY_IND[0:_get_depth()], name)
            keys = {'client_id': client_id}
            self._write(level, msg, keys)

    def _from_where(self, level, name, client_id):
        with LOCK:
            curframe = inspect.currentframe()
            if curframe:
                calframe = inspect.getouterframes(curframe, 2)
                lineno = calframe[3][2]
                funcName = calframe[3][3]
                msg = '{0}:{1}'.format(funcName, str(lineno))
                keys = {'client_id': client_id}
                self._write(level, msg, keys)

    def exit(self, name, client_id, return_code):
        self._exit_level(EXIT, name, client_id, return_code)

    def exit_often(self, name, client_id, return_code):
        self._exit_level(EXIT_OFTEN, name, client_id, return_code)

    def _exit_level(self, level, name, client_id, return_code):
        with LOCK:
            msg = '{0} {1} rc={2}'.format(
                EXIT_IND[0:_get_depth()],
                name, self._trim(return_code))
            keys = {'client_id': client_id}
            self._write(level, msg, keys)

    def _trim(self, value):
        if not isinstance(value, str):
            value = str(value)
        return (value[:300] + '...') if len(value) > 300 else value

    def ffdc(self, name, probe_id, client_id, err, data=None):
        opts = {
            'title': 'First Failure Data Capture',
            'fnc': name or 'User-requested FFDC by function',
            'probe_id': probe_id or 255,
            'ffdc_sequence': self._ffdc_sequence,
            'client_id': client_id or NO_CLIENT_ID,
        }
        self._ffdc_sequence += 1

        with LOCK:
            self.multi_formatter.set_FFDC(True)
            keys = {'client_id': opts['client_id']}
            self._write(FFDC, HEADER_BANNER, keys)
            self._write(FFDC, 'FFDC - IBM MQ Light Python Client', keys)
            self._write(FFDC, HEADER_BANNER, keys)
            self._write(FFDC,
                        'Date/Time:        {0} (UTC {1})'.format(
                            strftime('%H:%M:%S %d-%m-%y', localtime()),
                            strftime('%H:%M:%S', gmtime())), keys)
            self._write(FFDC,
                        'Host Name:        {0}'.format(socket.gethostname()),
                        keys)
            self._write(FFDC,
                        'Operating System: {0}'.format(platform.platform()),
                        keys)
            self._write(FFDC,
                        'Architecture:     {0}'.format(platform.machine()),
                        keys)
            self._write(FFDC,
                        'Client Version:   {0}'.format(__version__), keys)
            self._write(FFDC,
                        'Python Version:   {0}'.format(sys.version), keys)
            self._write(FFDC,
                        'Python Arguments: {0}'.format(sys.argv), keys)
            if not IS_WIN:
                self._write(FFDC,
                            'User Id:          {0}'.format(os.getuid()), keys)
                self._write(FFDC,
                            'Group Id:         {0}'.format(os.getgid()), keys)
            self._write(FFDC,
                        'Log Level:        {0}'.format(self._level), keys)
            self._write(FFDC,
                        'Function:         {0}'.format(opts['fnc']), keys)
            self._write(FFDC,
                        'Probe Id:         {0}'.format(opts['probe_id']), keys)
            self._write(FFDC,
                        'FFDC Sequence:    {0}'.format(opts['ffdc_sequence']),
                        keys)
            self._write(FFDC,
                        'Thread id:        {0}'.
                        format(threading.currentThread()),
                        keys)
            self._write(FFDC, HEADER_BANNER, keys)
            if isinstance(err, tuple):
                self._write(FFDC,
                            'Error:             {0} : {1}'.
                            format(err[0].__name__, err[1]), keys)
                self._write(FFDC, 'Traceback:', keys)
                for call in traceback.format_tb(err[2]):
                    self._write(FFDC, call.strip(), keys)
            else:
                self._write(FFDC, 'Error:             {0}'.format(err), keys)
            if data is not None:
                if not isinstance(data, str):
                    data = '{0}: {1}'.format(type(data).__name__, data)
                self._write(FFDC,
                            'Data:             {0}'.format(data), keys)
            self._write(FFDC, HEADER_BANNER, keys)
            self._write(FFDC, 'Call Stack:', keys)
            for call in traceback.format_stack():
                self._write(FFDC, call.strip(), keys)
            self._write(FFDC, HEADER_BANNER, keys)
            self._write(FFDC, 'Active Threads:', keys)
            for thread in threading.enumerate():
                self._write(FFDC, thread, keys)
            self._write(FFDC, HEADER_BANNER, keys)
            self.multi_formatter.set_FFDC(False)

    def debug(self, client_id, message):
        keys = {'client_id': client_id}
        self._write(DEBUG, message, keys)

    def parms_often(self, client_id, *args):
        msg = ' '.join(['{0}'.format(self._trim(arg)) for arg in args])
        keys = {'client_id': client_id}
        self._write(PARMS_OFTEN, msg, keys)

    def parms(self, client_id, *args):
        msg = ' '.join(['{0}'.format(self._trim(arg)) for arg in args])
        keys = {'client_id': client_id}
        self._write(PARMS, msg, keys)

    def data(self, client_id, *args):
        msg = ' '.join(['{0}'.format(self._trim(arg)) for arg in args])
        keys = {'client_id': client_id}
        self._write(DATA, msg, keys)

    def state(self, name, client_id, event, *args):
        msg = 'Client state changed to "{0}" by {1} '.format(event, name)
        msg += ' '.join(['{0}'.format(self._trim(arg)) for arg in args])
        keys = {'client_id': client_id}
        self._write(STATE, msg, keys)

    def error(self, name, client_id, err, exc=None):
        if isinstance(err, str):
            msg = '{0} by {1}'.format(err, name)
        else:
            msg = '{0} raised by {1}: {2} '.format(
                type(err).__name__, name, err)
        keys = {'client_id': client_id}
        self._write(ERROR, msg, keys)
        if exc is not None:
            for line in traceback.extract_tb(exc):
                self._write(ERROR, line, keys)


class MultiFormatter(logging.Formatter):
    def __init__(self, formatter, ffdc_formatter):
        self.formatter = formatter
        self.ffdc_formatter = ffdc_formatter
        self.ffdc = False

    def set_FFDC(self, ffdc):
        self.ffdc = ffdc

    def format(self, record):
        if self.ffdc:
            return self.ffdc_formatter.format(record)
        else:
            return self.formatter.format(record)
