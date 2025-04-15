#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name='bn_tests',
    version='0.0.1',
    packages=find_packages(),

    author='Kalpesh Pandya',
    author_email='kapandya@redhat.com',
    description='Bucket Notification compatibility tests',
    license='MIT',
    keywords='bn web testing',

    install_requires=[
        'boto >=2.0b4',
        'boto3 >=1.0.0'
        ],
    )
