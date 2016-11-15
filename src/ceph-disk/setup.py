#
# Copyright (C) 2015 <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
import os
import sys
from setuptools import setup
from setuptools import find_packages

assert sys.version_info >= (2, 7), \
    "Python version lower than 2.7 is not supported"


def read(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    f = open(path)
    return f.read()

tests_require = read('test-requirements.txt').split()

setup(
    name='ceph-disk',
    version='1.0.0',
    packages=find_packages(),

    author='Loic Dachary',
    author_email='loic@dachary.org',
    description='prepare storage for a Ceph OSD',
    long_description=read('README.rst'),
    license='LGPLv2+',
    keywords='ceph',
    url="https://git.ceph.com/?p=ceph.git;a=summary",

    install_requires=['setuptools'],
    tests_require=tests_require,

    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Utilities',
    ],

    entry_points={

        'console_scripts': [
            'ceph-disk = ceph_disk.main:run',
            ],

        },
    )
