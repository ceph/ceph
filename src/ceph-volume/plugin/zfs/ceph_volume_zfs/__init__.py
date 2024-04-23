# -*- coding: utf-8 -*-

"""Top-level package for Ceph volume on ZFS."""

__author__ = """Willem Jan Withagen"""
__email__ = 'wjw@digiware.nl'

import ceph_volume_zfs.zfs

from collections import namedtuple

sys_info = namedtuple('sys_info', ['devices'])
sys_info.devices = dict()
