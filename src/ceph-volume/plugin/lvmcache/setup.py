#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup

requirements = [ ]

setup_requirements = [ ]

setup(
    author='Mohamad Gebai',
    author_email='mgebai@suse.com',
    description="Manage an LVM cache layer in front of OSDs",
    install_requires=requirements,
    license="BSD license",
    url='https://github.com/ceph/ceph/src/ceph-volume/plugin/lvmcache',
    version='0.1.0',
    zip_safe=False,
    entry_points = dict(
        ceph_volume_handlers = [
            'lvmcache = ceph_volume_lvmcache.main:LVMCache',
        ],
    ),
)
