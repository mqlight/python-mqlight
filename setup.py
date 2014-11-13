"""
<copyright
notice="lm-source-program"
pids="5725-P60"
years="2013,2014"
crc="3568777996" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2014

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
from setuptools import setup, find_packages
from codecs import open
from os import path

try:
    import cproton
except ImportError:
    print 'The Qpid Python bindings are required'
    exit(1)

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'description.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='mqlight',
    version='1.0.0',
    description='IBM MQ Light Client Python Module',
    long_description=long_description,
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
    ],
    keywords='ibm mqlight',
    packages=find_packages(),
    #install_requires=['qpid'],
    extras_require={
        'test': ['mock'],
    },
    zip_safe=True
)