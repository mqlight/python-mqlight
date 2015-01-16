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
from setuptools import setup, find_packages, Extension
from codecs import open as codecs_open
from os import path
from platform import system

HERE = path.abspath(path.dirname(__file__))
with codecs_open(path.join(HERE, 'description.rst'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()


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

setup(
    name='mqlight',
    version='1.0.0',
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
    packages=find_packages(exclude=['tests']),
    package_data={'mqlight': ['*.dll', 'libqpid-proton*', 'samples/*.py']},
    ext_package='mqlight',
    ext_modules=[
        Extension(name='_cproton',
                  sources=get_sources(),
                  include_dirs=[path.join(HERE, 'include')],
                  library_dirs=['mqlight'],
                  libraries=['qpid-proton'],
                  runtime_library_dirs=get_runtime_library_dirs()),
    ],
    extras_require={
        'test': ['mock'],
    },
    zip_safe=True
)
