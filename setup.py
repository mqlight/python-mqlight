"""
<copyright
notice="lm-source-program"
pids="5725-P60"
years="2014,2015"
crc="3568777996" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2014, 2015

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
import sys
from setuptools import setup, find_packages, Extension
from setuptools.command.test import test as TestCommand
from codecs import open as codecs_open
from os import path, environ
from platform import system

if not sys.version_info[0] == 2:
    print('ERROR: Python 3 is not currently supported.')
    sys.exit(1)
elif not sys.version_info[1] >= 7:
    print('ERROR: Python 2.7 or newer is required')
    sys.exit(1)

HERE = path.abspath(path.dirname(__file__))
with codecs_open(path.join(HERE, 'description.rst'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

if system() == 'Darwin':
    environ['ARCHFLAGS'] = '-arch x86_64'


def get_sources():
    """Return a list of source files to compile into the extension"""
    if system() == 'Windows':
        return [path.join('mqlight', 'cproton.cxx')]
    else:
        return [path.join('mqlight', 'cproton.c')]


def get_runtime_library_dirs():
    """Return a custom rpath to write into the extension"""
    if system() == 'Linux':
        return ['$ORIGIN']
    else:
        return []


def get_extra_link_args():
    """Return a list of extra arguments to supply at extension link time"""
    if system() == 'Darwin':
        return ['-Wl,-rpath,@loader_path/']
    else:
        return []


# pylint: disable=R0904
class PyTest(TestCommand):

    """TestCommand to run suite using py.test"""
    test_args = []
    test_suite = True
    pytest_args = []

    def initialize_options(self):
        TestCommand.initialize_options(self)

    def finalize_options(self):
        TestCommand.finalize_options(self)

    def run_tests(self):
        environ['MQLIGHT_PYTHON_LOG'] = 'ALL'
        import pytest
        # self.pytest_args.insert(0, 'tests/test_unsubscribe.py')
        self.pytest_args.insert(0, '--junitxml=junit.xml')
        self.pytest_args.insert(0, '--timeout=10')
        errno = pytest.main(self.pytest_args)
        errno += pytest.main(['--pep8', '-m pep8', 'mqlight/'])
        sys.exit(errno)

setup(
    name='mqlight',
    version='9.9.9999999999',
    description='IBM MQ Light Client Python Module',
    long_description=LONG_DESCRIPTION,
    url='https://developer.ibm.com/messaging/mq-light/',
    author='IBM',
    author_email=' ',
    license='proprietary',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Topic :: Communications',
        'Topic :: System :: Networking',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='ibm mqlight',
    packages=find_packages(
        exclude=['tests']),
    package_data={
        'mqlight': [
            '*.dll',
            'libqpid-proton*',
            'samples/*.py',
            'licenses/*',
            'README']},
    ext_package='mqlight',
    ext_modules=[
        Extension(
            name='_cproton',
            sources=get_sources(),
            include_dirs=[
                path.join(
                    HERE,
                    'include')],
            library_dirs=['mqlight'],
            libraries=['qpid-proton'],
            runtime_library_dirs=get_runtime_library_dirs(),
            extra_link_args=get_extra_link_args()),
    ],
    test_suite='tests',
    tests_require=[
        'pytest_pep8',
        'pytest_timeout',
        'pytest',
        'mock'],
    cmdclass={
        'test': PyTest}
)
