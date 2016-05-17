#
# Copyright (C) 2015 SUSE LINUX GmbH
# Copyright (C) 2015 <contact@redhat.com>
#
# Author: Owen Synge <osynge@suse.com>
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see `<http://www.gnu.org/licenses/>`.
#
import os
import sys

from setuptools import find_packages
from setuptools import setup


def read(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    f = open(path)
    return f.read()


def filter_included_modules(*m):
    modules = sum(m, [])
    if sys.version_info[0:2] <= (2, 6):
        return modules

    elif sys.version_info[0:2] == (3, 0):
        return modules

    elif sys.version_info[0:2] == (3, 1):
        return list(set(modules) - set(['importlib']))

    else:
        # Python 2.7+ and Python 3.2+
        return list(set(modules) - set(['argparse', 'importlib', 'sysconfig']))


install_requires = read('requirements.txt').split()
tests_require = read('test-requirements.txt').split()

setup(
    name='ceph-detect-init',
    version='1.0.1',
    packages=find_packages(),

    author='Owen Synge, Loic Dachary',
    author_email='osynge@suse.de, loic@dachary.org',
    description='display the normalized name of the init system',
    long_description=read('README.rst'),
    license='LGPLv2+',
    keywords='ceph',
    url="https://git.ceph.com/?p=ceph.git;a=summary",

    install_requires=filter_included_modules(['setuptools'],
                                             install_requires),
    tests_require=filter_included_modules(tests_require),

    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Utilities',
    ],

    entry_points={

        'console_scripts': [
            'ceph-detect-init = ceph_detect_init.main:run',
        ],

    },
)
