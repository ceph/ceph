#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

requirements = [ ]

setup_requirements = [ ]

setup(
    author='Mohamad Gebai',
    author_email='mgebai@suse.com',
    description="Manage an LVM cache layer in front of OSDs",
    install_requires=requirements,
    license="BSD license",
    include_package_data=True,
    keywords='ceph-lvmcache',
    name='ceph-lvmcache',
    packages=find_packages(include=['ceph_volume_lvmcache']),
    setup_requires=setup_requirements,
    url='https://github.com/SUSE/ceph/tree/master/src/ceph-volume/plugin/lvmcache',
    version='0.1.0',
    zip_safe=False,
    entry_points = dict(
        ceph_volume_handlers = [
            'lvmcache = ceph_volume_lvmcache.main:LVMCache',
        ],
    ),
)
