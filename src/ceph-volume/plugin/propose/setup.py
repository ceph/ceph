#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

requirements = [ ]

setup_requirements = [ ]

setup(
    author="Jan Fajerski",
    author_email='jfajerski@suse.com',
    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: FreeBSD',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="Scan a nodes block storage hardware and propose OSD deployment groups",
    install_requires=requirements,
    license="BSD license",
    include_package_data=True,
    keywords='ceph-volume-propose',
    name='ceph-volume-propose',
    packages=find_packages(include=['ceph_volume_propose']),
    # scripts=['bin/ceph-volume-propose'],
    setup_requires=setup_requirements,
    url='https://github.com/ceph/ceph/src/ceph-volume/plugin/propose',
    version='0.1.0',
    zip_safe=False,
    entry_points = dict(
        ceph_volume_handlers = [
            'propose = ceph_volume_propose.main:Proposal',
        ],
    ),
)
