
# python-mqlight - high-level API by which you can interact with MQ Light
#
# Copyright 2015-2017 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from setuptools import setup, find_packages, Extension
from setuptools.command.test import test as TestCommand
from codecs import open as codecs_open
from os import path, environ
from platform import system, architecture

if not sys.version_info[:2] >= (2, 6):
    print('ERROR: Python 2.6 or newer is required')
    sys.exit(1)
if system() == 'Windows' and architecture()[0] == '32bit':
    print('ERROR: Mqlight requires 64bit Python on Windows.')
    sys.exit(1)

HERE = path.abspath(path.dirname(__file__))
with codecs_open(path.join(HERE, 'description.rst'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

if system() == 'Darwin':
    environ['ARCHFLAGS'] = '-arch x86_64 -mmacosx-version-min=10.8'


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


def get_extra_compile_args():
    """Return a list of extra arguments to supply at extension compile time"""
    if system() == 'Linux':
        return ['-Wno-address', '-Wno-unused-function']
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
        # environ['MQLIGHT_PYTHON_LOG'] = 'ALL'
        import pytest
        # self.pytest_args.insert(0, 'tests/test_unsubscribe.py')
        self.pytest_args.insert(0, '--junitxml=junit.xml')
        self.pytest_args.insert(0, '--timeout=10')
        self.pytest_args.insert(0, '--cov-report=term')
        self.pytest_args.insert(0, '--cov-report=html')
        self.pytest_args.insert(0, '--cov=mqlight')
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
    author_email='mqlight@uk.ibm.com',
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
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
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
            extra_compile_args=get_extra_compile_args(),
            extra_link_args=get_extra_link_args()),
    ],
    install_requires=[
        'argparse',
        'backports.ssl_match_hostname>=3.4.0.2'
    ],
    test_suite='tests',
    tests_require=[
        'pytest_cov',
        'pytest_pep8',
        'pytest_timeout',
        'pytest',
        'pbr==1.6.0'],
    cmdclass={
        'test': PyTest}
)
