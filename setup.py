#!/usr/bin/python
from setuptools import setup, find_packages, Extension

setup(
    name='orchestra',
    version='0.0.1',
    packages=find_packages(),

    author='Tommi Virtanen',
    author_email='tommi.virtanen@dreamhost.com',
    description='Orchestration of multiple machines over SSH',
    license='MIT',
    keywords='ssh cluster',

    install_requires=[
        'gevent ==0.13.6',
        'paramiko >=1.7.7',
        'nose >=1.0.0',
        'fudge >=1.0.3',
        ],

    )
