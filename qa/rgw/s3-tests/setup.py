#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name='s3tests',
    version='0.0.1',
    packages=find_packages(),

    author='Tommi Virtanen',
    author_email='tommi.virtanen@dreamhost.com',
    description='Unofficial Amazon AWS S3 compatibility tests',
    license='MIT',
    keywords='s3 web testing',

    install_requires=[
        'boto >=2.0b4',
        'boto3 >=1.0.0',
        'PyYAML',
        'munch >=2.0.0',
        'gevent >=1.0',
        'isodate >=0.4.4',
        ],
    )
