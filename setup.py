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
        'gevent ==0.13.6',
        'paramiko >=1.7.7',
        'configobj',
        'PyYAML',
        'bunch >=1.0.0',
        'argparse >=1.2.1',
        'httplib2',
        'beanstalkc >=0.2.0',
        'pexpect',
        ],

    # to find the code associated with entry point
    # A.B:foo first cd into directory A, open file B
    # and find sub foo
    entry_points={
        'console_scripts': [
            'teuthology = teuthology.run:main',
            'teuthology-nuke = teuthology.nuke:main',
            'teuthology-suite = teuthology.suite:main',
            'teuthology-ls = teuthology.suite:ls',
            'teuthology-worker = teuthology.queue:worker',
            'teuthology-lock = teuthology.lock:main',
            'teuthology-schedule = teuthology.run:schedule',
            'teuthology-updatekeys = teuthology.lock:update_hostkeys',
            'teuthology-coverage = teuthology.coverage:analyze',
            'teuthology-results = teuthology.suite:results',
            ],
        },

    )
