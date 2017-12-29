import argparse
import os
from ceph_volume import terminal
from ceph_volume import decorators
from ceph_volume.util import disk


class LVPath(object):
    """
    A simple validator to ensure that a logical volume is specified like::

        <vg name>/<lv name>

    Or a full path to a device, like ``/dev/sda``

    Because for LVM it is better to be specific on what group does an lv
    belongs to.
    """

    def __call__(self, string):
        error = None
        if string.startswith('/'):
            if not os.path.exists(string):
                error = "Argument (device) does not exist: %s" % string
                raise argparse.ArgumentError(None, error)
            else:
                return string
        try:
            vg, lv = string.split('/')
        except ValueError:
            error = "Logical volume must be specified as 'volume_group/logical_volume' but got: %s" % string
            raise argparse.ArgumentError(None, error)

        if not vg:
            error = "Didn't specify a volume group like 'volume_group/logical_volume', got: %s" % string
        if not lv:
            error = "Didn't specify a logical volume like 'volume_group/logical_volume', got: %s" % string

        if error:
            raise argparse.ArgumentError(None, error)
        return string


class OSDPath(object):
    """
    Validate path exists and it looks like an OSD directory.
    """

    @decorators.needs_root
    def __call__(self, string):
        if not os.path.exists(string):
            error = "Path does not exist: %s" % string
            raise argparse.ArgumentError(None, error)

        arg_is_partition = disk.is_partition(string)
        if arg_is_partition:
            return os.path.abspath(string)
        absolute_path = os.path.abspath(string)
        if not os.path.isdir(absolute_path):
            error = "Argument is not a directory or device which is required to scan"
            raise argparse.ArgumentError(None, error)
        key_files = ['ceph_fsid', 'fsid', 'keyring', 'ready', 'type', 'whoami']
        dir_files = os.listdir(absolute_path)
        for key_file in key_files:
            if key_file not in dir_files:
                terminal.error('All following files must exist in path: %s' % ' '.join(key_files))
                error = "Required file (%s) was not found in OSD dir path: %s" % (
                    key_file,
                    absolute_path
                )
                raise argparse.ArgumentError(None, error)

        return os.path.abspath(string)
