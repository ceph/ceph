# -*- coding: utf-8 -*-

from setuptools import setup

__version__ = '0.0.1'

setup(
    name='cephfs-top',
    version=__version__,
    description='top(1) like utility for Ceph Filesystem',
    keywords='cephfs, top',
    scripts=['cephfs-top'],
    install_requires=[
        'rados',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3'
    ],
    license='LGPLv2+',
)
