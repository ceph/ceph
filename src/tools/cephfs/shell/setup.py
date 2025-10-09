# -*- coding: utf-8 -*-

from setuptools import setup

__version__ = '0.0.1'

setup(
    name='cephfs-shell',
    version=__version__,
    description='Interactive shell for Ceph file system',
    keywords='cephfs, shell',
    scripts=['cephfs-shell'],
    install_requires=[
        'cephfs',
        'cmd2',
        'colorama',
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
