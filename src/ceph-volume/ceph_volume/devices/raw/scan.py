from __future__ import print_function
import argparse
import base64
import json
import logging
import os
from textwrap import dedent
from ceph_volume import decorators, terminal, conf
from ceph_volume.api import lvm
from ceph_volume.systemd import systemctl
from ceph_volume.util import arg_validators, system, disk, encryption
from ceph_volume.util.device import Device


logger = logging.getLogger(__name__)

class Scan(object):

    help = 'Capture metadata from all running ceph-disk OSDs, OSD data partition or directory'

    def __init__(self, argv):
        self.argv = argv
        self._etc_path = '/etc/ceph/osd/'
