#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name='teuthology',
    version='0.0.1',
    packages=find_packages(),

    author='Tommi Virtanen',
    author_email='tommi.virtanen@dreamhost.com',
    description='Ceph test runner',
    license='MIT',
    keywords='ceph testing ssh cluster',

    install_requires=[
        'orchestra',
        'configobj',
        'PyYAML',
        'bunch >=1.0.0',
        ],

    )
