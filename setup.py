from setuptools import setup, find_packages

version = '1.0'

setup(
    name='mqlight',
    version=version,
    url='https://developer.ibm.com/messaging/mq-light/',
    author='IBM',
    description='IBM MQ Light Client Module',
    license='proprietary',
    packages=find_packages(exclude=('tests')),
    include_package_data=True,
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
)
