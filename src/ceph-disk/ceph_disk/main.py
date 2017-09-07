#!/usr/bin/env python
#
# Copyright (C) 2015, 2016 Red Hat <contact@redhat.com>
# Copyright (C) 2014 Inktank <info@inktank.com>
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014 Catalyst.net Ltd
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

from __future__ import print_function

import argparse
import base64
import errno
import fcntl
import json
import logging
import os
import platform
import re
import subprocess
import stat
import sys
import tempfile
import uuid
import time
import shlex
import pwd
import grp
import textwrap
import glob

CEPH_OSD_ONDISK_MAGIC = 'ceph osd volume v026'
CEPH_LOCKBOX_ONDISK_MAGIC = 'ceph lockbox volume v001'

KEY_MANAGEMENT_MODE_V1 = 'ceph-mon v1'

PTYPE = {
    'regular': {
        'journal': {
            # identical because creating a journal is atomic
            'ready': '45b0969e-9b03-4f30-b4c6-b4b80ceff106',
            'tobe': '45b0969e-9b03-4f30-b4c6-b4b80ceff106',
        },
        'block': {
            # identical because creating a block is atomic
            'ready': 'cafecafe-9b03-4f30-b4c6-b4b80ceff106',
            'tobe': 'cafecafe-9b03-4f30-b4c6-b4b80ceff106',
        },
        'osd': {
            'ready': '4fbd7e29-9d25-41b8-afd0-062c0ceff05d',
            'tobe': '89c57f98-2fe5-4dc0-89c1-f3ad0ceff2be',
        },
        'lockbox': {
            'ready': 'fb3aabf9-d25f-47cc-bf5e-721d1816496b',
            'tobe': 'fb3aabf9-d25f-47cc-bf5e-721d181642be',
        },
    },
    'luks': {
        'journal': {
            'ready': '45b0969e-9b03-4f30-b4c6-35865ceff106',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff2be',
        },
        'block': {
            'ready': 'cafecafe-9b03-4f30-b4c6-35865ceff106',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff2be',
        },
        'osd': {
            'ready': '4fbd7e29-9d25-41b8-afd0-35865ceff05d',
            'tobe': '89c57f98-2fe5-4dc0-89c1-5ec00ceff2be',
        },
    },
    'plain': {
        'journal': {
            'ready': '45b0969e-9b03-4f30-b4c6-5ec00ceff106',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff2be',
        },
        'block': {
            'ready': 'cafecafe-9b03-4f30-b4c6-5ec00ceff106',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff2be',
        },
        'osd': {
            'ready': '4fbd7e29-9d25-41b8-afd0-5ec00ceff05d',
            'tobe': '89c57f98-2fe5-4dc0-89c1-5ec00ceff2be',
        },
    },
    'mpath': {
        'journal': {
            'ready': '45b0969e-8ae0-4982-bf9d-5a8d867af560',
            'tobe': '45b0969e-8ae0-4982-bf9d-5a8d867af560',
        },
        'block': {
            'ready': 'cafecafe-8ae0-4982-bf9d-5a8d867af560',
            'tobe': 'cafecafe-8ae0-4982-bf9d-5a8d867af560',
        },
        'osd': {
            'ready': '4fbd7e29-8ae0-4982-bf9d-5a8d867af560',
            'tobe': '89c57f98-8ae0-4982-bf9d-5a8d867af560',
        },
        'lockbox': {
            'ready': '7f4a666a-16f3-47a2-8445-152ef4d03f6c',
            'tobe': '7f4a666a-16f3-47a2-8445-152ef4d032be',
        },
    },
}


class Ptype(object):

    @staticmethod
    def get_ready_by_type(what):
        return [x['ready'] for x in PTYPE[what].values()]

    @staticmethod
    def get_ready_by_name(name):
        return [x[name]['ready'] for x in PTYPE.values() if name in x]

    @staticmethod
    def is_regular_space(ptype):
        return Ptype.is_what_space('regular', ptype)

    @staticmethod
    def is_mpath_space(ptype):
        return Ptype.is_what_space('mpath', ptype)

    @staticmethod
    def is_plain_space(ptype):
        return Ptype.is_what_space('plain', ptype)

    @staticmethod
    def is_luks_space(ptype):
        return Ptype.is_what_space('luks', ptype)

    @staticmethod
    def is_what_space(what, ptype):
        for name in Space.NAMES:
            if ptype == PTYPE[what][name]['ready']:
                return True
        return False

    @staticmethod
    def space_ptype_to_name(ptype):
        for what in PTYPE.values():
            for name in Space.NAMES:
                if ptype == what[name]['ready']:
                    return name
        raise ValueError('ptype ' + ptype + ' not found')

    @staticmethod
    def is_dmcrypt_space(ptype):
        for name in Space.NAMES:
            if Ptype.is_dmcrypt(ptype, name):
                return True
        return False

    @staticmethod
    def is_dmcrypt(ptype, name):
        for what in ('plain', 'luks'):
            if ptype == PTYPE[what][name]['ready']:
                return True
        return False


DEFAULT_FS_TYPE = 'xfs'
SYSFS = '/sys'

"""
OSD STATUS Definition
"""
OSD_STATUS_OUT_DOWN = 0
OSD_STATUS_OUT_UP = 1
OSD_STATUS_IN_DOWN = 2
OSD_STATUS_IN_UP = 3

MOUNT_OPTIONS = dict(
    btrfs='noatime,user_subvol_rm_allowed',
    # user_xattr is default ever since linux 2.6.39 / 3.0, but we'll
    # delay a moment before removing it fully because we did have some
    # issues with ext4 before the xatts-in-leveldb work, and it seemed
    # that user_xattr helped
    ext4='noatime,user_xattr',
    xfs='noatime,inode64',
)

MKFS_ARGS = dict(
    btrfs=[
        # btrfs requires -f, for the same reason as xfs (see comment below)
        '-f',
        '-m', 'single',
        '-l', '32768',
        '-n', '32768',
    ],
    xfs=[
        # xfs insists on not overwriting previous fs; even if we wipe
        # partition table, we often recreate it exactly the same way,
        # so we'll see ghosts of filesystems past
        '-f',
        '-i', 'size=2048',
    ],
)

INIT_SYSTEMS = [
    'upstart',
    'sysvinit',
    'systemd',
    'auto',
    'none',
]

STATEDIR = '/var/lib/ceph'

SYSCONFDIR = '/etc/ceph'

prepare_lock = None
activate_lock = None
SUPPRESS_PREFIX = None

# only warn once about some things
warned_about = {}

# Nuke the TERM variable to avoid confusing any subprocesses we call.
# For example, libreadline will print weird control sequences for some
# TERM values.
if 'TERM' in os.environ:
    del os.environ['TERM']

LOG_NAME = __name__
if LOG_NAME == '__main__':
    LOG_NAME = os.path.basename(sys.argv[0])
LOG = logging.getLogger(LOG_NAME)

# Allow user-preferred values for subprocess user and group
CEPH_PREF_USER = None
CEPH_PREF_GROUP = None


class FileLock(object):
    def __init__(self, fn):
        self.fn = fn
        self.fd = None

    def __enter__(self):
        assert not self.fd
        self.fd = os.open(self.fn, os.O_WRONLY | os.O_CREAT)
        fcntl.lockf(self.fd, fcntl.LOCK_EX)

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.fd
        fcntl.lockf(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)
        self.fd = None


class Error(Exception):
    """
    Error
    """

    def __str__(self):
        doc = _bytes2str(self.__doc__.strip())
        try:
            str_type = basestring
        except NameError:
            str_type = str
        args = [a if isinstance(a, str_type) else str(a) for a in self.args]
        return ': '.join([doc] + [_bytes2str(a) for a in args])


class MountError(Error):
    """
    Mounting filesystem failed
    """


class UnmountError(Error):
    """
    Unmounting filesystem failed
    """


class BadMagicError(Error):
    """
    Does not look like a Ceph OSD, or incompatible version
    """


class TruncatedLineError(Error):
    """
    Line is truncated
    """


class TooManyLinesError(Error):
    """
    Too many lines
    """


class FilesystemTypeError(Error):
    """
    Cannot discover filesystem type
     """


class CephDiskException(Exception):
    """
    A base exception for ceph-disk to provide custom (ad-hoc) messages that
    will be caught and dealt with when main() is executed
    """
    pass


class ExecutableNotFound(CephDiskException):
    """
    Exception to report on executables not available in PATH
    """
    pass


def is_systemd():
    """
    Detect whether systemd is running
    """
    with open('/proc/1/comm', 'r') as f:
        return 'systemd' in f.read()


def is_upstart():
    """
    Detect whether upstart is running
    """
    (out, err, _) = command(['init', '--version'])
    return 'upstart' in out


def maybe_mkdir(*a, **kw):
    """
    Creates a new directory if it doesn't exist, removes
    existing symlink before creating the directory.
    """
    # remove any symlink, if it is there..
    if os.path.exists(*a) and stat.S_ISLNK(os.lstat(*a).st_mode):
        LOG.debug('Removing old symlink at %s', *a)
        os.unlink(*a)
    try:
        os.mkdir(*a, **kw)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise


def which(executable):
    """find the location of an executable"""
    envpath = os.environ.get('PATH') or os.defpath
    PATH = envpath.split(os.pathsep)

    locations = PATH + [
        '/usr/local/bin',
        '/bin',
        '/usr/bin',
        '/usr/local/sbin',
        '/usr/sbin',
        '/sbin',
    ]

    for location in locations:
        executable_path = os.path.join(location, executable)
        if (os.path.isfile(executable_path) and
                os.access(executable_path, os.X_OK)):
            return executable_path


def _get_command_executable(arguments):
    """
    Return the full path for an executable, raise if the executable is not
    found. If the executable has already a full path do not perform any checks.
    """
    if os.path.isabs(arguments[0]):  # an absolute path
        return arguments
    executable = which(arguments[0])
    if not executable:
        command_msg = 'Could not run command: %s' % ' '.join(arguments)
        executable_msg = '%s not in path.' % arguments[0]
        raise ExecutableNotFound('%s %s' % (executable_msg, command_msg))

    # swap the old executable for the new one
    arguments[0] = executable
    return arguments


def command(arguments, **kwargs):
    """
    Safely execute a ``subprocess.Popen`` call making sure that the
    executable exists and raising a helpful error message
    if it does not.

    .. note:: This should be the prefered way of calling ``subprocess.Popen``
    since it provides the caller with the safety net of making sure that
    executables *will* be found and will error nicely otherwise.

    This returns the output of the command and the return code of the
    process in a tuple: (output, returncode).
    """

    arguments = list(map(_bytes2str, _get_command_executable(arguments)))

    LOG.info('Running command: %s' % ' '.join(arguments))
    process = subprocess.Popen(
        arguments,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs)
    out, err = process.communicate()

    return _bytes2str(out), _bytes2str(err), process.returncode


def command_with_stdin(arguments, stdin):
    LOG.info("Running command with stdin: " + " ".join(arguments))
    process = subprocess.Popen(
        arguments,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    out, err = process.communicate(stdin)
    LOG.debug(out)
    if process.returncode != 0:
        LOG.error(err)
        raise SystemExit(
            "'{cmd}' failed with status code {returncode}".format(
                cmd=arguments,
                returncode=process.returncode,
            )
        )
    return out


def _bytes2str(string):
    return string.decode('utf-8') if isinstance(string, bytes) else string


def command_init(arguments, **kwargs):
    """
    Safely execute a non-blocking ``subprocess.Popen`` call
    making sure that the executable exists and raising a helpful
    error message if it does not.

    .. note:: This should be the preferred way of calling ``subprocess.Popen``
    since it provides the caller with the safety net of making sure that
    executables *will* be found and will error nicely otherwise.

    This returns the process.
    """

    arguments = list(map(_bytes2str, _get_command_executable(arguments)))

    LOG.info('Running command: %s' % ' '.join(arguments))
    process = subprocess.Popen(
        arguments,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs)
    return process


def command_wait(process):
    """
    Wait for the process finish and parse its output.
    """

    out, err = process.communicate()

    return _bytes2str(out), _bytes2str(err), process.returncode


def command_check_call(arguments):
    """
    Safely execute a ``subprocess.check_call`` call making sure that the
    executable exists and raising a helpful error message if it does not.

    .. note:: This should be the prefered way of calling
    ``subprocess.check_call`` since it provides the caller with the safety net
    of making sure that executables *will* be found and will error nicely
    otherwise.
    """
    arguments = _get_command_executable(arguments)
    LOG.info('Running command: %s', ' '.join(arguments))
    return subprocess.check_call(arguments)


def platform_distro():
    """
    Returns a normalized, lower case string without any leading nor trailing
    whitespace that represents the distribution name of the current machine.
    """
    distro = platform_information()[0] or ''
    return distro.strip().lower()


def platform_information():
    distro, release, codename = platform.linux_distribution()
    # this could be an empty string in Debian
    if not codename and 'debian' in distro.lower():
        debian_codenames = {
            '8': 'jessie',
            '7': 'wheezy',
            '6': 'squeeze',
        }
        major_version = release.split('.')[0]
        codename = debian_codenames.get(major_version, '')

        # In order to support newer jessie/sid or wheezy/sid strings we test
        # this if sid is buried in the minor, we should use sid anyway.
        if not codename and '/' in release:
            major, minor = release.split('/')
            if minor == 'sid':
                codename = minor
            else:
                codename = major

    return (
        str(distro).strip(),
        str(release).strip(),
        str(codename).strip()
    )

#
# An alternative block_path implementation would be
#
#   name = basename(dev)
#   return /sys/devices/virtual/block/$name
#
# It is however more fragile because it relies on the fact
# that the basename of the device the user will use always
# matches the one the driver will use. On Ubuntu 14.04, for
# instance, when multipath creates a partition table on
#
#   /dev/mapper/353333330000007d0 -> ../dm-0
#
# it will create partition devices named
#
#   /dev/mapper/353333330000007d0-part1
#
# which is the same device as /dev/dm-1 but not a symbolic
# link to it:
#
#   ubuntu@other:~$ ls -l /dev/mapper /dev/dm-1
#   brw-rw---- 1 root disk 252, 1 Aug 15 17:52 /dev/dm-1
#   lrwxrwxrwx 1 root root        7 Aug 15 17:52 353333330000007d0 -> ../dm-0
#   brw-rw---- 1 root disk 252,   1 Aug 15 17:52 353333330000007d0-part1
#
# Using the basename in this case fails.
#


def block_path(dev):
    path = os.path.realpath(dev)
    rdev = os.stat(path).st_rdev
    (M, m) = (os.major(rdev), os.minor(rdev))
    return "{sysfs}/dev/block/{M}:{m}".format(sysfs=SYSFS, M=M, m=m)


def get_dm_uuid(dev):
    uuid_path = os.path.join(block_path(dev), 'dm', 'uuid')
    LOG.debug("get_dm_uuid " + dev + " uuid path is " + uuid_path)
    if not os.path.exists(uuid_path):
        return False
    uuid = open(uuid_path, 'r').read()
    LOG.debug("get_dm_uuid " + dev + " uuid is " + uuid)
    return uuid


def is_mpath(dev):
    """
    True if the path is managed by multipath
    """
    uuid = get_dm_uuid(dev)
    return (uuid and
            (re.match('part\d+-mpath-', uuid) or
             re.match('mpath-', uuid)))


def get_dev_name(path):
    """
    get device name from path.  e.g.::

        /dev/sda -> sdas, /dev/cciss/c0d1 -> cciss!c0d1

    a device "name" is something like::

        sdb
        cciss!c0d1

    """
    assert path.startswith('/dev/')
    base = path[5:]
    return base.replace('/', '!')


def get_dev_path(name):
    """
    get a path (/dev/...) from a name (cciss!c0d1)
    a device "path" is something like::

        /dev/sdb
        /dev/cciss/c0d1

    """
    return '/dev/' + name.replace('!', '/')


def get_dev_relpath(name):
    """
    get a relative path to /dev from a name (cciss!c0d1)
    """
    return name.replace('!', '/')


def get_dev_size(dev, size='megabytes'):
    """
    Attempt to get the size of a device so that we can prevent errors
    from actions to devices that are smaller, and improve error reporting.

    Because we want to avoid breakage in case this approach is not robust, we
    will issue a warning if we failed to get the size.

    :param size: bytes or megabytes
    :param dev: the device to calculate the size
    """
    fd = os.open(dev, os.O_RDONLY)
    dividers = {'bytes': 1, 'megabytes': 1024 * 1024}
    try:
        device_size = os.lseek(fd, 0, os.SEEK_END)
        divider = dividers.get(size, 1024 * 1024)  # default to megabytes
        return device_size // divider
    except Exception as error:
        LOG.warning('failed to get size of %s: %s' % (dev, str(error)))
    finally:
        os.close(fd)


def get_partition_mpath(dev, pnum):
    part_re = "part{pnum}-mpath-".format(pnum=pnum)
    partitions = list_partitions_mpath(dev, part_re)
    if partitions:
        return partitions[0]
    else:
        return None


def get_partition_dev(dev, pnum):
    """
    get the device name for a partition

    assume that partitions are named like the base dev,
    with a number, and optionally
    some intervening characters (like 'p').  e.g.,

       sda 1 -> sda1
       cciss/c0d1 1 -> cciss!c0d1p1
    """
    max_retry = 10
    for retry in range(0, max_retry + 1):
        partname = None
        error_msg = ""
        if is_mpath(dev):
            partname = get_partition_mpath(dev, pnum)
        else:
            name = get_dev_name(os.path.realpath(dev))
            sys_entry = os.path.join('/sys/block', name)
            error_msg = " in %s" % sys_entry
            for f in os.listdir(sys_entry):
                if f.startswith(name) and f.endswith(str(pnum)):
                    # we want the shortest name that starts with the base name
                    # and ends with the partition number
                    if not partname or len(f) < len(partname):
                        partname = f
        if partname:
            if retry:
                LOG.info('Found partition %d for %s after %d tries' %
                         (pnum, dev, retry))
            return get_dev_path(partname)
        else:
            if retry < max_retry:
                LOG.info('Try %d/%d : partition %d for %s does not exist%s' %
                         (retry + 1, max_retry, pnum, dev, error_msg))
                time.sleep(.2)
                continue
            else:
                raise Error('partition %d for %s does not appear to exist%s' %
                            (pnum, dev, error_msg))


def list_all_partitions():
    """
    Return a list of devices and partitions
    """
    names = os.listdir('/sys/block')
    dev_part_list = {}
    for name in names:
        # /dev/fd0 may hang http://tracker.ceph.com/issues/6827
        if re.match(r'^fd\d$', name):
            continue
        dev_part_list[name] = list_partitions(get_dev_path(name))
    return dev_part_list


def list_partitions(dev):
    dev = os.path.realpath(dev)
    if is_mpath(dev):
        return list_partitions_mpath(dev)
    else:
        return list_partitions_device(dev)


def list_partitions_mpath(dev, part_re="part\d+-mpath-"):
    p = block_path(dev)
    partitions = []
    holders = os.path.join(p, 'holders')
    for holder in os.listdir(holders):
        uuid_path = os.path.join(holders, holder, 'dm', 'uuid')
        uuid = open(uuid_path, 'r').read()
        LOG.debug("list_partitions_mpath: " + uuid_path + " uuid = " + uuid)
        if re.match(part_re, uuid):
            partitions.append(holder)
    return partitions


def list_partitions_device(dev):
    """
    Return a list of partitions on the given device name
    """
    partitions = []
    basename = get_dev_name(dev)
    for name in os.listdir(block_path(dev)):
        if name.startswith(basename):
            partitions.append(name)
    return partitions


def get_partition_base(dev):
    """
    Get the base device for a partition
    """
    dev = os.path.realpath(dev)
    if not stat.S_ISBLK(os.lstat(dev).st_mode):
        raise Error('not a block device', dev)

    name = get_dev_name(dev)
    if os.path.exists(os.path.join('/sys/block', name)):
        raise Error('not a partition', dev)

    # find the base
    for basename in os.listdir('/sys/block'):
        if os.path.exists(os.path.join('/sys/block', basename, name)):
            return get_dev_path(basename)
    raise Error('no parent device for partition', dev)


def is_partition_mpath(dev):
    uuid = get_dm_uuid(dev)
    return bool(re.match('part\d+-mpath-', uuid))


def partnum_mpath(dev):
    uuid = get_dm_uuid(dev)
    return re.findall('part(\d+)-mpath-', uuid)[0]


def get_partition_base_mpath(dev):
    slave_path = os.path.join(block_path(dev), 'slaves')
    slaves = os.listdir(slave_path)
    assert slaves
    name_path = os.path.join(slave_path, slaves[0], 'dm', 'name')
    name = open(name_path, 'r').read().strip()
    return os.path.join('/dev/mapper', name)


def is_partition(dev):
    """
    Check whether a given device path is a partition or a full disk.
    """
    if is_mpath(dev):
        return is_partition_mpath(dev)

    dev = os.path.realpath(dev)
    st = os.lstat(dev)
    if not stat.S_ISBLK(st.st_mode):
        raise Error('not a block device', dev)

    name = get_dev_name(dev)
    if os.path.exists(os.path.join('/sys/block', name)):
        return False

    # make sure it is a partition of something else
    major = os.major(st.st_rdev)
    minor = os.minor(st.st_rdev)
    if os.path.exists('/sys/dev/block/%d:%d/partition' % (major, minor)):
        return True

    raise Error('not a disk or partition', dev)


def is_mounted(dev):
    """
    Check if the given device is mounted.
    """
    dev = os.path.realpath(dev)
    with open('/proc/mounts', 'rb') as proc_mounts:
        for line in proc_mounts:
            fields = line.split()
            if len(fields) < 3:
                continue
            mounts_dev = fields[0]
            path = fields[1]
            if os.path.isabs(mounts_dev) and os.path.exists(mounts_dev):
                mounts_dev = os.path.realpath(mounts_dev)
                if mounts_dev == dev:
                    return _bytes2str(path)
    return None


def is_held(dev):
    """
    Check if a device is held by another device (e.g., a dm-crypt mapping)
    """
    assert os.path.exists(dev)
    if is_mpath(dev):
        return []

    dev = os.path.realpath(dev)
    base = get_dev_name(dev)

    # full disk?
    directory = '/sys/block/{base}/holders'.format(base=base)
    if os.path.exists(directory):
        return os.listdir(directory)

    # partition?
    part = base
    while len(base):
        directory = '/sys/block/{base}/{part}/holders'.format(
            part=part, base=base)
        if os.path.exists(directory):
            return os.listdir(directory)
        base = base[:-1]
    return []


def verify_not_in_use(dev, check_partitions=False):
    """
    Verify if a given device (path) is in use (e.g. mounted or
    in use by device-mapper).

    :raises: Error if device is in use.
    """
    assert os.path.exists(dev)
    if is_mounted(dev):
        raise Error('Device is mounted', dev)
    holders = is_held(dev)
    if holders:
        raise Error('Device %s is in use by a device-mapper '
                    'mapping (dm-crypt?)' % dev, ','.join(holders))

    if check_partitions and not is_partition(dev):
        for partname in list_partitions(dev):
            partition = get_dev_path(partname)
            if is_mounted(partition):
                raise Error('Device is mounted', partition)
            holders = is_held(partition)
            if holders:
                raise Error('Device %s is in use by a device-mapper '
                            'mapping (dm-crypt?)'
                            % partition, ','.join(holders))


def must_be_one_line(line):
    """
    Checks if given line is really one single line.

    :raises: TruncatedLineError or TooManyLinesError
    :return: Content of the line, or None if line isn't valid.
    """
    line = _bytes2str(line)

    if line[-1:] != '\n':
        raise TruncatedLineError(line)
    line = line[:-1]
    if '\n' in line:
        raise TooManyLinesError(line)
    return line


def read_one_line(parent, name):
    """
    Read a file whose sole contents are a single line.

    Strips the newline.

    :return: Contents of the line, or None if file did not exist.
    """
    path = os.path.join(parent, name)
    try:
        line = open(path, 'rb').read()
    except IOError as e:
        if e.errno == errno.ENOENT:
            return None
        else:
            raise

    try:
        line = must_be_one_line(line)
    except (TruncatedLineError, TooManyLinesError) as e:
        raise Error(
            'File is corrupt: {path}: {msg}'.format(
                path=path,
                msg=e,
            )
        )
    return line


def write_one_line(parent, name, text):
    """
    Write a file whose sole contents are a single line.

    Adds a newline.
    """
    path = os.path.join(parent, name)
    tmp = '{path}.{pid}.tmp'.format(path=path, pid=os.getpid())
    with open(tmp, 'wb') as tmp_file:
        tmp_file.write(text.encode('utf-8') + b'\n')
        os.fsync(tmp_file.fileno())
    path_set_context(tmp)
    os.rename(tmp, path)


def init_get():
    """
    Get a init system using 'ceph-detect-init'
    """
    init = _check_output(
        args=[
            'ceph-detect-init',
            '--default', 'sysvinit',
        ],
    )
    init = must_be_one_line(init)
    return init


def check_osd_magic(path):
    """
    Check that this path has the Ceph OSD magic.

    :raises: BadMagicError if this does not look like a Ceph OSD data
    dir.
    """
    magic = read_one_line(path, 'magic')
    if magic is None:
        # probably not mkfs'ed yet
        raise BadMagicError(path)
    if magic != CEPH_OSD_ONDISK_MAGIC:
        raise BadMagicError(path)


def check_osd_id(osd_id):
    """
    Ensures osd id is numeric.
    """
    if not re.match(r'^[0-9]+$', osd_id):
        raise Error('osd id is not numeric', osd_id)


def allocate_osd_id(
    cluster,
    fsid,
    keyring,
):
    """
    Accocates an OSD id on the given cluster.

    :raises: Error if the call to allocate the OSD id fails.
    :return: The allocated OSD id.
    """

    LOG.debug('Allocating OSD id...')
    try:
        osd_id = _check_output(
            args=[
                'ceph',
                '--cluster', cluster,
                '--name', 'client.bootstrap-osd',
                '--keyring', keyring,
                'osd', 'create', '--concise',
                fsid,
            ],
        )
    except subprocess.CalledProcessError as e:
        raise Error('ceph osd create failed', e, e.output)
    osd_id = must_be_one_line(osd_id)
    check_osd_id(osd_id)
    return osd_id


def get_osd_id(path):
    """
    Gets the OSD id of the OSD at the given path.
    """
    osd_id = read_one_line(path, 'whoami')
    if osd_id is not None:
        check_osd_id(osd_id)
    return osd_id


def get_ceph_user():
    global CEPH_PREF_USER

    if CEPH_PREF_USER is not None:
        try:
            pwd.getpwnam(CEPH_PREF_USER)
            return CEPH_PREF_USER
        except KeyError:
            print("No such user:", CEPH_PREF_USER)
            sys.exit(2)
    else:
        try:
            pwd.getpwnam('ceph')
            return 'ceph'
        except KeyError:
            return 'root'


def get_ceph_group():
    global CEPH_PREF_GROUP

    if CEPH_PREF_GROUP is not None:
        try:
            grp.getgrnam(CEPH_PREF_GROUP)
            return CEPH_PREF_GROUP
        except KeyError:
            print("No such group:", CEPH_PREF_GROUP)
            sys.exit(2)
    else:
        try:
            grp.getgrnam('ceph')
            return 'ceph'
        except KeyError:
            return 'root'


def path_set_context(path):
    # restore selinux context to default policy values
    if which('restorecon'):
        command(['restorecon', '-R', path])

    # if ceph user exists, set owner to ceph
    if get_ceph_user() == 'ceph':
        command(['chown', '-R', 'ceph:ceph', path])


def _check_output(args=None, **kwargs):
    out, err, ret = command(args, **kwargs)
    if ret:
        cmd = args[0]
        error = subprocess.CalledProcessError(ret, cmd)
        error.output = out + err
        raise error
    return _bytes2str(out)


def get_conf(cluster, variable):
    """
    Get the value of the given configuration variable from the
    cluster.

    :raises: Error if call to ceph-conf fails.
    :return: The variable value or None.
    """
    try:
        out, err, ret = command(
            [
                'ceph-conf',
                '--cluster={cluster}'.format(
                    cluster=cluster,
                ),
                '--name=osd.',
                '--lookup',
                variable,
            ],
            close_fds=True,
        )
    except OSError as e:
        raise Error('error executing ceph-conf', e, err)
    if ret == 1:
        # config entry not found
        return None
    elif ret != 0:
        raise Error('getting variable from configuration failed')
    value = out.split('\n', 1)[0]
    # don't differentiate between "var=" and no var set
    if not value:
        return None
    return value


def get_conf_with_default(cluster, variable):
    """
    Get a config value that is known to the C++ code.

    This will fail if called on variables that are not defined in
    common config options.
    """
    try:
        out = _check_output(
            args=[
                'ceph-osd',
                '--cluster={cluster}'.format(
                    cluster=cluster,
                ),
                '--show-config-value={variable}'.format(
                    variable=variable,
                ),
            ],
            close_fds=True,
        )
    except subprocess.CalledProcessError as e:
        raise Error(
            'getting variable from configuration failed',
            e,
        )

    value = str(out).split('\n', 1)[0]
    return value


def get_fsid(cluster):
    """
    Get the fsid of the cluster.

    :return: The fsid or raises Error.
    """
    fsid = get_conf_with_default(cluster=cluster, variable='fsid')
    if fsid is None:
        raise Error('getting cluster uuid from configuration failed')
    return fsid.lower()


def get_dmcrypt_key_path(
    _uuid,
    key_dir,
    luks
):
    """
    Get path to dmcrypt key file.

    :return: Path to the dmcrypt key file, callers should check for existence.
    """
    if luks:
        path = os.path.join(key_dir, _uuid + ".luks.key")
    else:
        path = os.path.join(key_dir, _uuid)

    return path


def get_dmcrypt_key(
    _uuid,
    key_dir,
    luks
):
    legacy_path = get_dmcrypt_key_path(_uuid, key_dir, luks)
    if os.path.exists(legacy_path):
        return (legacy_path,)
    path = os.path.join(STATEDIR, 'osd-lockbox', _uuid)
    if os.path.exists(path):
        mode = get_oneliner(path, 'key-management-mode')
        osd_uuid = get_oneliner(path, 'osd-uuid')
        ceph_fsid = read_one_line(path, 'ceph_fsid')
        if ceph_fsid is None:
            LOG.warning("no `ceph_fsid` found falling back to 'ceph' "
                        "for cluster name")
            cluster = 'ceph'
        else:
            cluster = find_cluster_by_uuid(ceph_fsid)
            if cluster is None:
                raise Error('No cluster conf found in ' + SYSCONFDIR +
                            ' with fsid %s' % ceph_fsid)

        if mode == KEY_MANAGEMENT_MODE_V1:
            key, stderr, ret = command(
                [
                    'ceph',
                    '--cluster', cluster,
                    '--name',
                    'client.osd-lockbox.' + osd_uuid,
                    '--keyring',
                    os.path.join(path, 'keyring'),
                    'config-key',
                    'get',
                    'dm-crypt/osd/' + osd_uuid + '/luks',
                ],
            )
            LOG.debug("stderr " + stderr)
            assert ret == 0
            return base64.b64decode(key)
        else:
            raise Error('unknown key-management-mode ' + str(mode))
    raise Error('unable to read dm-crypt key', path, legacy_path)


def _dmcrypt_map(
    rawdev,
    key,
    _uuid,
    cryptsetup_parameters,
    luks,
    format_dev=False,
):
    dev = dmcrypt_is_mapped(_uuid)
    if dev:
        return dev

    if isinstance(key, tuple):
        # legacy, before lockbox
        assert os.path.exists(key[0])
        keypath = key[0]
        key = None
    else:
        keypath = '-'
    dev = '/dev/mapper/' + _uuid
    luksFormat_args = [
        'cryptsetup',
        '--batch-mode',
        '--key-file',
        keypath,
        'luksFormat',
        rawdev,
    ] + cryptsetup_parameters

    luksOpen_args = [
        'cryptsetup',
        '--key-file',
        keypath,
        'luksOpen',
        rawdev,
        _uuid,
    ]

    create_args = [
        'cryptsetup',
        '--key-file',
        keypath,
        'create',
        _uuid,
        rawdev,
    ] + cryptsetup_parameters

    def run(args, stdin):
        LOG.info(" ".join(args))
        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        out, err = process.communicate(stdin)
        LOG.debug(out)
        LOG.error(err)
        assert process.returncode == 0

    try:
        if luks:
            if format_dev:
                run(luksFormat_args, key)
            run(luksOpen_args, key)
        else:
            # Plain mode has no format function, nor any validation
            # that the key is correct.
            run(create_args, key)
        # set proper ownership of mapped device
        command_check_call(['chown', 'ceph:ceph', dev])
        return dev

    except subprocess.CalledProcessError as e:
        raise Error('unable to map device', rawdev, e)


def dmcrypt_unmap(
    _uuid
):
    if not os.path.exists('/dev/mapper/' + _uuid):
        return
    retries = 0
    while True:
        try:
            command_check_call(['cryptsetup', 'remove', _uuid])
            break
        except subprocess.CalledProcessError as e:
            if retries == 10:
                raise Error('unable to unmap device', _uuid, e)
            else:
                time.sleep(0.5 + retries * 1.0)
                retries += 1


def mount(
    dev,
    fstype,
    options,
):
    """
    Mounts a device with given filessystem type and
    mount options to a tempfile path under /var/lib/ceph/tmp.
    """
    # sanity check: none of the arguments are None
    if dev is None:
        raise ValueError('dev may not be None')
    if fstype is None:
        raise ValueError('fstype may not be None')

    # pick best-of-breed mount options based on fs type
    if options is None:
        options = MOUNT_OPTIONS.get(fstype, '')

    myTemp = STATEDIR + '/tmp'
    # mkdtemp expect 'dir' to be existing on the system
    # Let's be sure it's always the case
    if not os.path.exists(myTemp):
        os.makedirs(myTemp)

    # mount
    path = tempfile.mkdtemp(
        prefix='mnt.',
        dir=myTemp,
    )
    try:
        LOG.debug('Mounting %s on %s with options %s', dev, path, options)
        command_check_call(
            [
                'mount',
                '-t', fstype,
                '-o', options,
                '--',
                dev,
                path,
            ],
        )
        if which('restorecon'):
            command(
                [
                    'restorecon',
                    path,
                ],
            )
    except subprocess.CalledProcessError as e:
        try:
            os.rmdir(path)
        except (OSError, IOError):
            pass
        raise MountError(e)

    return path


def unmount(
    path,
):
    """
    Unmount and removes the given mount point.
    """
    retries = 0
    while True:
        try:
            LOG.debug('Unmounting %s', path)
            command_check_call(
                [
                    '/bin/umount',
                    '--',
                    path,
                ],
            )
            break
        except subprocess.CalledProcessError as e:
            # on failure, retry 3 times with incremental backoff
            if retries == 3:
                raise UnmountError(e)
            else:
                time.sleep(0.5 + retries * 1.0)
                retries += 1

    os.rmdir(path)


###########################################

def extract_parted_partition_numbers(partitions):
    numbers_as_strings = re.findall('^\d+', partitions, re.MULTILINE)
    return map(int, numbers_as_strings)


def get_free_partition_index(dev):
    """
    Get the next free partition index on a given device.

    :return: Index number (> 1 if there is already a partition on the device)
    or 1 if there is no partition table.
    """
    try:
        lines = _check_output(
            args=[
                'parted',
                '--machine',
                '--',
                dev,
                'print',
            ],
        )
    except subprocess.CalledProcessError as e:
        LOG.info('cannot read partition index; assume it '
                 'isn\'t present\n (Error: %s)' % e)
        return 1

    if not lines:
        raise Error('parted failed to output anything')
    LOG.debug('get_free_partition_index: analyzing ' + lines)
    if ('CHS;' not in lines and
            'CYL;' not in lines and
            'BYT;' not in lines):
        raise Error('parted output expected to contain one of ' +
                    'CHH; CYL; or BYT; : ' + lines)
    if os.path.realpath(dev) not in lines:
        raise Error('parted output expected to contain ' + dev + ': ' + lines)
    _, partitions = lines.split(os.path.realpath(dev))
    partition_numbers = extract_parted_partition_numbers(partitions)
    if partition_numbers:
        return max(partition_numbers) + 1
    else:
        return 1


def check_journal_reqs(args):
    _, _, allows_journal = command([
        'ceph-osd', '--check-allows-journal',
        '-i', '0',
        '--log-file', '$run_dir/$cluster-osd-check.log',
        '--cluster', args.cluster,
        '--setuser', get_ceph_user(),
        '--setgroup', get_ceph_group(),
    ])
    _, _, wants_journal = command([
        'ceph-osd', '--check-wants-journal',
        '-i', '0',
        '--log-file', '$run_dir/$cluster-osd-check.log',
        '--cluster', args.cluster,
        '--setuser', get_ceph_user(),
        '--setgroup', get_ceph_group(),
    ])
    _, _, needs_journal = command([
        'ceph-osd', '--check-needs-journal',
        '-i', '0',
        '--log-file', '$run_dir/$cluster-osd-check.log',
        '--cluster', args.cluster,
        '--setuser', get_ceph_user(),
        '--setgroup', get_ceph_group(),
    ])
    return (not allows_journal, not wants_journal, not needs_journal)


def update_partition(dev, description):
    """
    Must be called after modifying a partition table so the kernel
    know about the change and fire udev events accordingly. A side
    effect of partprobe is to remove partitions and add them again.
    The first udevadm settle waits for ongoing udev events to
    complete, just in case one of them rely on an existing partition
    on dev.  The second udevadm settle guarantees to the caller that
    all udev events related to the partition table change have been
    processed, i.e. the 95-ceph-osd.rules actions and mode changes,
    group changes etc. are complete.
    """
    LOG.debug('Calling partprobe on %s device %s', description, dev)
    partprobe_ok = False
    error = 'unknown error'
    partprobe = _get_command_executable(['partprobe'])[0]
    for i in range(5):
        command_check_call(['udevadm', 'settle', '--timeout=600'])
        try:
            _check_output(['flock', '-s', dev, partprobe, dev])
            partprobe_ok = True
            break
        except subprocess.CalledProcessError as e:
            error = e.output
            if ('unable to inform the kernel' not in error and
                    'Device or resource busy' not in error):
                raise
            LOG.debug('partprobe %s failed : %s (ignored, waiting 60s)'
                      % (dev, error))
            time.sleep(60)
    if not partprobe_ok:
        raise Error('partprobe %s failed : %s' % (dev, error))
    command_check_call(['udevadm', 'settle', '--timeout=600'])


def zap(dev):
    """
    Destroy the partition table and content of a given disk.
    """
    dev = os.path.realpath(dev)
    dmode = os.stat(dev).st_mode
    if not stat.S_ISBLK(dmode) or is_partition(dev):
        raise Error('not full block device; cannot zap', dev)
    try:
        LOG.debug('Zapping partition table on %s', dev)

        # try to wipe out any GPT partition table backups.  sgdisk
        # isn't too thorough.
        lba_size = 4096
        size = 33 * lba_size
        with open(dev, 'wb') as dev_file:
            dev_file.seek(-size, os.SEEK_END)
            dev_file.write(size * b'\0')

        command_check_call(
            [
                'sgdisk',
                '--zap-all',
                '--',
                dev,
            ],
        )
        command_check_call(
            [
                'sgdisk',
                '--clear',
                '--mbrtogpt',
                '--',
                dev,
            ],
        )

        update_partition(dev, 'zapped')

    except subprocess.CalledProcessError as e:
        raise Error(e)


def adjust_symlink(target, path):
    create = True
    if os.path.lexists(path):
        try:
            mode = os.lstat(path).st_mode
            if stat.S_ISREG(mode):
                LOG.debug('Removing old file %s', path)
                os.unlink(path)
            elif stat.S_ISLNK(mode):
                old = os.readlink(path)
                if old != target:
                    LOG.debug('Removing old symlink %s -> %s', path, old)
                    os.unlink(path)
                else:
                    create = False
        except:
            raise Error('unable to remove (or adjust) old file (symlink)',
                        path)
    if create:
        LOG.debug('Creating symlink %s -> %s', path, target)
        try:
            os.symlink(target, path)
        except:
            raise Error('unable to create symlink %s -> %s' % (path, target))


def get_mount_options(cluster, fs_type):
    mount_options = get_conf(
        cluster,
        variable='osd_mount_options_{fstype}'.format(
            fstype=fs_type,
        ),
    )
    if mount_options is None:
        mount_options = get_conf(
            cluster,
            variable='osd_fs_mount_options_{fstype}'.format(
                fstype=fs_type,
            ),
        )
    else:
        # remove whitespaces
        mount_options = "".join(mount_options.split())
    return mount_options


class Device(object):

    def __init__(self, path, args):
        self.args = args
        self.path = path
        self.dev_size = None
        self.partitions = {}
        self.ptype_map = None
        assert not is_partition(self.path)

    def create_partition(self, uuid, name, size=0, num=0):
        ptype = self.ptype_tobe_for_name(name)
        if num == 0:
            num = get_free_partition_index(dev=self.path)
        if size > 0:
            new = '--new={num}:0:+{size}M'.format(num=num, size=size)
            if size > self.get_dev_size():
                LOG.error('refusing to create %s on %s' % (name, self.path))
                LOG.error('%s size (%sM) is bigger than device (%sM)'
                          % (name, size, self.get_dev_size()))
                raise Error('%s device size (%sM) is not big enough for %s'
                            % (self.path, self.get_dev_size(), name))
        else:
            new = '--largest-new={num}'.format(num=num)

        LOG.debug('Creating %s partition num %d size %d on %s',
                  name, num, size, self.path)
        command_check_call(
            [
                'sgdisk',
                new,
                '--change-name={num}:ceph {name}'.format(num=num, name=name),
                '--partition-guid={num}:{uuid}'.format(num=num, uuid=uuid),
                '--typecode={num}:{uuid}'.format(num=num, uuid=ptype),
                '--mbrtogpt',
                '--',
                self.path,
            ]
        )
        update_partition(self.path, 'created')
        return num

    def ptype_tobe_for_name(self, name):
        LOG.debug("name = " + name)
        if name == 'data':
            name = 'osd'
        if name == 'lockbox':
            if is_mpath(self.path):
                return PTYPE['mpath']['lockbox']['tobe']
            else:
                return PTYPE['regular']['lockbox']['tobe']
        if self.ptype_map is None:
            partition = DevicePartition.factory(
                path=self.path, dev=None, args=self.args)
            self.ptype_map = partition.ptype_map
        return self.ptype_map[name]['tobe']

    def get_partition(self, num):
        if num not in self.partitions:
            dev = get_partition_dev(self.path, num)
            partition = DevicePartition.factory(
                path=self.path, dev=dev, args=self.args)
            partition.set_partition_number(num)
            self.partitions[num] = partition
        return self.partitions[num]

    def get_dev_size(self):
        if self.dev_size is None:
            self.dev_size = get_dev_size(self.path)
        return self.dev_size

    @staticmethod
    def factory(path, args):
        return Device(path, args)


class DevicePartition(object):

    def __init__(self, args):
        self.args = args
        self.num = None
        self.rawdev = None
        self.dev = None
        self.uuid = None
        self.ptype_map = None
        self.ptype = None
        self.set_variables_ptype()

    def get_uuid(self):
        if self.uuid is None:
            self.uuid = get_partition_uuid(self.rawdev)
        return self.uuid

    def get_ptype(self):
        if self.ptype is None:
            self.ptype = get_partition_type(self.rawdev)
        return self.ptype

    def set_partition_number(self, num):
        self.num = num

    def get_partition_number(self):
        return self.num

    def set_dev(self, dev):
        self.dev = dev
        self.rawdev = dev

    def get_dev(self):
        return self.dev

    def get_rawdev(self):
        return self.rawdev

    def set_variables_ptype(self):
        self.ptype_map = PTYPE['regular']

    def ptype_for_name(self, name):
        return self.ptype_map[name]['ready']

    @staticmethod
    def factory(path, dev, args):
        dmcrypt_type = CryptHelpers.get_dmcrypt_type(args)
        if ((path is not None and is_mpath(path)) or
                (dev is not None and is_mpath(dev))):
            partition = DevicePartitionMultipath(args)
        elif dmcrypt_type == 'luks':
            partition = DevicePartitionCryptLuks(args)
        elif dmcrypt_type == 'plain':
            partition = DevicePartitionCryptPlain(args)
        else:
            partition = DevicePartition(args)
        partition.set_dev(dev)
        return partition


class DevicePartitionMultipath(DevicePartition):

    def set_variables_ptype(self):
        self.ptype_map = PTYPE['mpath']


class DevicePartitionCrypt(DevicePartition):

    def __init__(self, args):
        super(DevicePartitionCrypt, self).__init__(args)
        self.osd_dm_key = None
        self.cryptsetup_parameters = CryptHelpers.get_cryptsetup_parameters(
            self.args)
        self.dmcrypt_type = CryptHelpers.get_dmcrypt_type(self.args)
        self.dmcrypt_keysize = CryptHelpers.get_dmcrypt_keysize(self.args)

    def setup_crypt(self):
        pass

    def map(self):
        self.setup_crypt()
        self.dev = _dmcrypt_map(
            rawdev=self.rawdev,
            key=self.osd_dm_key,
            _uuid=self.get_uuid(),
            cryptsetup_parameters=self.cryptsetup_parameters,
            luks=self.luks(),
            format_dev=True,
        )

    def unmap(self):
        self.setup_crypt()
        dmcrypt_unmap(self.get_uuid())
        self.dev = self.rawdev

    def format(self):
        self.setup_crypt()
        self.map()


class DevicePartitionCryptPlain(DevicePartitionCrypt):

    def luks(self):
        return False

    def setup_crypt(self):
        if self.osd_dm_key is not None:
            return

        self.cryptsetup_parameters += ['--key-size', str(self.dmcrypt_keysize)]

        self.osd_dm_key = get_dmcrypt_key(
            self.get_uuid(), self.args.dmcrypt_key_dir,
            False)

    def set_variables_ptype(self):
        self.ptype_map = PTYPE['plain']


class DevicePartitionCryptLuks(DevicePartitionCrypt):

    def luks(self):
        return True

    def setup_crypt(self):
        if self.osd_dm_key is not None:
            return

        if self.dmcrypt_keysize == 1024:
            # We don't force this into the cryptsetup_parameters,
            # as we want the cryptsetup defaults
            # to prevail for the actual LUKS key lengths.
            pass
        else:
            self.cryptsetup_parameters += ['--key-size',
                                           str(self.dmcrypt_keysize)]

        self.osd_dm_key = get_dmcrypt_key(
            self.get_uuid(), self.args.dmcrypt_key_dir,
            True)

    def set_variables_ptype(self):
        self.ptype_map = PTYPE['luks']


class Prepare(object):

    @staticmethod
    def parser():
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '--cluster',
            metavar='NAME',
            default='ceph',
            help='cluster name to assign this disk to',
        )
        parser.add_argument(
            '--cluster-uuid',
            metavar='UUID',
            help='cluster uuid to assign this disk to',
        )
        parser.add_argument(
            '--osd-uuid',
            metavar='UUID',
            help='unique OSD uuid to assign this disk to',
        )
        parser.add_argument(
            '--dmcrypt',
            action='store_true', default=None,
            help='encrypt DATA and/or JOURNAL devices with dm-crypt',
        )
        parser.add_argument(
            '--dmcrypt-key-dir',
            metavar='KEYDIR',
            default='/etc/ceph/dmcrypt-keys',
            help='directory where dm-crypt keys are stored',
        )
        parser.add_argument(
            '--prepare-key',
            metavar='PATH',
            help='bootstrap-osd keyring path template (%(default)s)',
            default='{statedir}/bootstrap-osd/{cluster}.keyring',
            dest='prepare_key_template',
        )
        return parser

    @staticmethod
    def set_subparser(subparsers):
        parents = [
            Prepare.parser(),
            PrepareData.parser(),
            Lockbox.parser(),
        ]
        parents.extend(PrepareFilestore.parent_parsers())
        parents.extend(PrepareBluestore.parent_parsers())
        parser = subparsers.add_parser(
            'prepare',
            parents=parents,
            help='Prepare a directory or disk for a Ceph OSD',
        )
        parser.set_defaults(
            func=Prepare.main,
        )
        return parser

    def prepare(self):
        with prepare_lock:
            self.prepare_locked()

    @staticmethod
    def factory(args):
        if args.bluestore:
            return PrepareBluestore(args)
        else:
            return PrepareFilestore(args)

    @staticmethod
    def main(args):
        Prepare.factory(args).prepare()


class PrepareFilestore(Prepare):

    def __init__(self, args):
        if args.dmcrypt:
            self.lockbox = Lockbox(args)
        self.data = PrepareFilestoreData(args)
        self.journal = PrepareJournal(args)

    @staticmethod
    def parent_parsers():
        return [
            PrepareJournal.parser(),
        ]

    def prepare_locked(self):
        if self.data.args.dmcrypt:
            self.lockbox.prepare()
        self.data.prepare(self.journal)


class PrepareBluestore(Prepare):

    def __init__(self, args):
        if args.dmcrypt:
            self.lockbox = Lockbox(args)
        self.data = PrepareBluestoreData(args)
        self.block = PrepareBluestoreBlock(args)

    @staticmethod
    def parser():
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '--bluestore',
            action='store_true', default=None,
            help='bluestore objectstore',
        )
        return parser

    @staticmethod
    def parent_parsers():
        return [
            PrepareBluestore.parser(),
            PrepareBluestoreBlock.parser(),
        ]

    def prepare_locked(self):
        if self.data.args.dmcrypt:
            self.lockbox.prepare()
        self.data.prepare(self.block)


class Space(object):

    NAMES = ('block', 'journal')


class PrepareSpace(object):

    NONE = 0
    FILE = 1
    DEVICE = 2

    def __init__(self, args):
        self.args = args
        self.set_type()
        self.space_size = self.get_space_size()
        if getattr(self.args, self.name + '_uuid') is None:
            setattr(self.args, self.name + '_uuid', str(uuid.uuid4()))
        self.space_symlink = None
        self.space_dmcrypt = None

    def set_type(self):
        name = self.name
        args = self.args
        dmode = os.stat(args.data).st_mode
        if (self.wants_space() and
                stat.S_ISBLK(dmode) and
                not is_partition(args.data) and
                getattr(args, name) is None and
                getattr(args, name + '_file') is None):
            LOG.info('Will colocate %s with data on %s',
                     name, args.data)
            setattr(args, name, args.data)

        if getattr(args, name) is None:
            if getattr(args, name + '_dev'):
                raise Error('%s is unspecified; not a block device' %
                            name.capitalize(), getattr(args, name))
            self.type = self.NONE
            return

        if not os.path.exists(getattr(args, name)):
            if getattr(args, name + '_dev'):
                raise Error('%s does not exist; not a block device' %
                            name.capitalize(), getattr(args, name))
            self.type = self.FILE
            return

        mode = os.stat(getattr(args, name)).st_mode
        if stat.S_ISBLK(mode):
            if getattr(args, name + '_file'):
                raise Error('%s is not a regular file' % name.capitalize,
                            getattr(args, name))
            self.type = self.DEVICE
            return

        if stat.S_ISREG(mode):
            if getattr(args, name + '_dev'):
                raise Error('%s is not a block device' % name.capitalize,
                            getattr(args, name))
            self.type = self.FILE
            return

        raise Error('%s %s is neither a block device nor regular file' %
                    (name.capitalize, getattr(args, name)))

    def is_none(self):
        return self.type == self.NONE

    def is_file(self):
        return self.type == self.FILE

    def is_device(self):
        return self.type == self.DEVICE

    @staticmethod
    def parser(name):
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '--%s-uuid' % name,
            metavar='UUID',
            help='unique uuid to assign to the %s' % name,
        )
        parser.add_argument(
            '--%s-file' % name,
            action='store_true', default=None,
            help='verify that %s is a file' % name.upper(),
        )
        parser.add_argument(
            '--%s-dev' % name,
            action='store_true', default=None,
            help='verify that %s is a block device' % name.upper(),
        )
        parser.add_argument(
            name,
            metavar=name.upper(),
            nargs='?',
            help=('path to OSD %s disk block device;' % name +
                  ' leave out to store %s in file' % name),
        )
        return parser

    def wants_space(self):
        return True

    def populate_data_path(self, path):
        if self.type == self.DEVICE:
            self.populate_data_path_device(path)
        elif self.type == self.FILE:
            self.populate_data_path_file(path)
        elif self.type == self.NONE:
            pass
        else:
            raise Error('unexpected type ', self.type)

    def populate_data_path_file(self, path):
        space_uuid = self.name + '_uuid'
        if getattr(self.args, space_uuid) is not None:
            write_one_line(path, space_uuid,
                           getattr(self.args, space_uuid))
        if self.space_symlink is not None:
            adjust_symlink(self.space_symlink,
                           os.path.join(path, self.name))

    def populate_data_path_device(self, path):
        self.populate_data_path_file(path)

        if self.space_dmcrypt is not None:
            adjust_symlink(self.space_dmcrypt,
                           os.path.join(path, self.name + '_dmcrypt'))
        else:
            try:
                os.unlink(os.path.join(path, self.name + '_dmcrypt'))
            except OSError:
                pass

    def prepare(self):
        if self.type == self.DEVICE:
            self.prepare_device()
        elif self.type == self.FILE:
            self.prepare_file()
        elif self.type == self.NONE:
            pass
        else:
            raise Error('unexpected type ', self.type)

    def prepare_file(self):
        space_filename = getattr(self.args, self.name)
        if not os.path.exists(space_filename):
            LOG.debug('Creating %s file %s with size 0'
                      ' (ceph-osd will resize and allocate)',
                      self.name,
                      space_filename)
            space_file = open(space_filename, 'wb')
            space_file.close()
            path_set_context(space_filename)

        LOG.debug('%s is file %s',
                  self.name.capitalize(),
                  space_filename)
        LOG.warning('OSD will not be hot-swappable if %s is '
                    'not the same device as the osd data' %
                    self.name)
        self.space_symlink = space_filename

    def prepare_device(self):
        reusing_partition = False

        if is_partition(getattr(self.args, self.name)):
            LOG.debug('%s %s is a partition',
                      self.name.capitalize(), getattr(self.args, self.name))
            partition = DevicePartition.factory(
                path=None, dev=getattr(self.args, self.name), args=self.args)
            if isinstance(partition, DevicePartitionCrypt):
                raise Error(getattr(self.args, self.name) +
                            ' partition already exists'
                            ' and --dmcrypt specified')
            LOG.warning('OSD will not be hot-swappable' +
                        ' if ' + self.name + ' is not' +
                        ' the same device as the osd data')
            if partition.get_ptype() == partition.ptype_for_name(self.name):
                LOG.debug('%s %s was previously prepared with '
                          'ceph-disk. Reusing it.',
                          self.name.capitalize(),
                          getattr(self.args, self.name))
                reusing_partition = True
                # Read and reuse the partition uuid from this journal's
                # previous life. We reuse the uuid instead of changing it
                # because udev does not reliably notice changes to an
                # existing partition's GUID.  See
                # http://tracker.ceph.com/issues/10146
                setattr(self.args, self.name + '_uuid', partition.get_uuid())
                LOG.debug('Reusing %s with uuid %s',
                          self.name,
                          getattr(self.args, self.name + '_uuid'))
            else:
                LOG.warning('%s %s was not prepared with '
                            'ceph-disk. Symlinking directly.',
                            self.name.capitalize(),
                            getattr(self.args, self.name))
                self.space_symlink = getattr(self.args, self.name)
                return

        self.space_symlink = '/dev/disk/by-partuuid/{uuid}'.format(
            uuid=getattr(self.args, self.name + '_uuid'))

        if self.args.dmcrypt:
            self.space_dmcrypt = self.space_symlink
            self.space_symlink = '/dev/mapper/{uuid}'.format(
                uuid=getattr(self.args, self.name + '_uuid'))

        if reusing_partition:
            # confirm that the space_symlink exists. It should since
            # this was an active space
            # in the past. Continuing otherwise would be futile.
            assert os.path.exists(self.space_symlink)
            return

        num = self.desired_partition_number()

        if num == 0:
            LOG.warning('OSD will not be hot-swappable if %s '
                        'is not the same device as the osd data',
                        self.name)

        device = Device.factory(getattr(self.args, self.name), self.args)
        num = device.create_partition(
            uuid=getattr(self.args, self.name + '_uuid'),
            name=self.name,
            size=self.space_size,
            num=num)

        partition = device.get_partition(num)

        LOG.debug('%s is GPT partition %s',
                  self.name.capitalize(),
                  self.space_symlink)

        if isinstance(partition, DevicePartitionCrypt):
            partition.format()
            partition.map()

            command_check_call(
                [
                    'sgdisk',
                    '--typecode={num}:{uuid}'.format(
                        num=num,
                        uuid=partition.ptype_for_name(self.name),
                    ),
                    '--',
                    getattr(self.args, self.name),
                ],
            )

        LOG.debug('%s is GPT partition %s',
                  self.name.capitalize(),
                  self.space_symlink)


class PrepareJournal(PrepareSpace):

    def __init__(self, args):
        self.name = 'journal'
        (self.allows_journal,
         self.wants_journal,
         self.needs_journal) = check_journal_reqs(args)

        if args.journal and not self.allows_journal:
            raise Error('journal specified but not allowed by osd backend')

        super(PrepareJournal, self).__init__(args)

    def wants_space(self):
        return self.wants_journal

    def get_space_size(self):
        return int(get_conf_with_default(
            cluster=self.args.cluster,
            variable='osd_journal_size',
        ))

    def desired_partition_number(self):
        if self.args.journal == self.args.data:
            # we're sharing the disk between osd data and journal;
            # make journal be partition number 2
            num = 2
        else:
            num = 0
        return num

    @staticmethod
    def parser():
        return PrepareSpace.parser('journal')


class PrepareBluestoreBlock(PrepareSpace):

    def __init__(self, args):
        self.name = 'block'
        super(PrepareBluestoreBlock, self).__init__(args)

    def get_space_size(self):
        return 0  # get as much space as possible

    def desired_partition_number(self):
        if self.args.block == self.args.data:
            num = 2
        else:
            num = 0
        return num

    @staticmethod
    def parser():
        return PrepareSpace.parser('block')


class CryptHelpers(object):

    @staticmethod
    def get_cryptsetup_parameters(args):
        cryptsetup_parameters_str = get_conf(
            cluster=args.cluster,
            variable='osd_cryptsetup_parameters',
        )
        if cryptsetup_parameters_str is None:
            return []
        else:
            return shlex.split(cryptsetup_parameters_str)

    @staticmethod
    def get_dmcrypt_keysize(args):
        dmcrypt_keysize_str = get_conf(
            cluster=args.cluster,
            variable='osd_dmcrypt_key_size',
        )
        dmcrypt_type = CryptHelpers.get_dmcrypt_type(args)
        if dmcrypt_type == 'luks':
            if dmcrypt_keysize_str is None:
                # As LUKS will hash the 'passphrase' in .luks.key
                # into a key, set a large default
                # so if not updated for some time, it is still a
                # reasonable value.
                #
                return 1024
            else:
                return int(dmcrypt_keysize_str)
        elif dmcrypt_type == 'plain':
            if dmcrypt_keysize_str is None:
                # This value is hard-coded in the udev script
                return 256
            else:
                LOG.warning('ensure the 95-ceph-osd.rules file has '
                            'been copied to /etc/udev/rules.d '
                            'and modified to call cryptsetup '
                            'with --key-size=%s' % dmcrypt_keysize_str)
                return int(dmcrypt_keysize_str)
        else:
            return 0

    @staticmethod
    def get_dmcrypt_type(args):
        if hasattr(args, 'dmcrypt') and args.dmcrypt:
            dmcrypt_type = get_conf(
                cluster=args.cluster,
                variable='osd_dmcrypt_type',
            )

            if dmcrypt_type is None or dmcrypt_type == 'luks':
                return 'luks'
            elif dmcrypt_type == 'plain':
                return 'plain'
            else:
                raise Error('invalid osd_dmcrypt_type parameter '
                            '(must be luks or plain): ', dmcrypt_type)
        else:
            return None


class Lockbox(object):

    def __init__(self, args):
        self.args = args
        self.partition = None
        self.device = None

        if hasattr(self.args, 'lockbox') and self.args.lockbox is None:
            self.args.lockbox = self.args.data

    def set_partition(self, partition):
        self.partition = partition

    @staticmethod
    def parser():
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '--lockbox',
            help='path to the device to store the lockbox',
        )
        parser.add_argument(
            '--lockbox-uuid',
            metavar='UUID',
            help='unique lockbox uuid',
        )
        return parser

    def create_partition(self):
        self.device = Device.factory(self.args.lockbox, argparse.Namespace())
        partition_number = 3
        self.device.create_partition(uuid=self.args.lockbox_uuid,
                                     name='lockbox',
                                     num=partition_number,
                                     size=10)  # MB
        return self.device.get_partition(partition_number)

    def set_or_create_partition(self):
        if is_partition(self.args.lockbox):
            LOG.debug('OSD lockbox device %s is a partition',
                      self.args.lockbox)
            self.partition = DevicePartition.factory(
                path=None, dev=self.args.lockbox, args=self.args)
            ptype = self.partition.get_ptype()
            ready = Ptype.get_ready_by_name('lockbox')
            if ptype not in ready:
                LOG.warning('incorrect partition UUID: %s, expected %s'
                            % (ptype, str(ready)))
        else:
            LOG.debug('Creating osd partition on %s',
                      self.args.lockbox)
            self.partition = self.create_partition()

    def create_key(self):
        key_size = CryptHelpers.get_dmcrypt_keysize(self.args)
        key = open('/dev/urandom', 'rb').read(key_size / 8)
        base64_key = base64.b64encode(key)
        cluster = self.args.cluster
        bootstrap = self.args.prepare_key_template.format(cluster=cluster,
                                                          statedir=STATEDIR)
        command_with_stdin(
            [
                'ceph',
                '--cluster', cluster,
                '--name', 'client.bootstrap-osd',
                '--keyring', bootstrap,
                '-i', '-',
                'config-key',
                'put',
                'dm-crypt/osd/' + self.args.osd_uuid + '/luks',
            ],
            base64_key
        )
        keyring, stderr, ret = command(
            [
                'ceph',
                '--cluster', cluster,
                '--name', 'client.bootstrap-osd',
                '--keyring', bootstrap,
                'auth',
                'get-or-create',
                'client.osd-lockbox.' + self.args.osd_uuid,
                'mon',
                ('allow command "config-key get" with key="dm-crypt/osd/' +
                 self.args.osd_uuid + '/luks"'),
            ],
        )
        LOG.debug("stderr " + stderr)
        assert ret == 0
        path = self.get_mount_point()
        open(os.path.join(path, 'keyring'), 'w').write(keyring)
        write_one_line(path, 'key-management-mode', KEY_MANAGEMENT_MODE_V1)

    def symlink_spaces(self, path):
        target = self.get_mount_point()
        for name in Space.NAMES:
            if (hasattr(self.args, name + '_uuid') and
                    getattr(self.args, name + '_uuid')):
                uuid = getattr(self.args, name + '_uuid')
                symlink = os.path.join(STATEDIR, 'osd-lockbox', uuid)
                adjust_symlink(target, symlink)
                write_one_line(path, name + '-uuid', uuid)

    def populate(self):
        maybe_mkdir(os.path.join(STATEDIR, 'osd-lockbox'))
        args = ['mkfs', '-t', 'ext4', self.partition.get_dev()]
        LOG.debug('Creating lockbox fs on %s: ' + str(" ".join(args)))
        command_check_call(args)
        path = self.get_mount_point()
        maybe_mkdir(path)
        args = ['mount', '-t', 'ext4', self.partition.get_dev(), path]
        LOG.debug('Mounting lockbox ' + str(" ".join(args)))
        command_check_call(args)
        write_one_line(path, 'osd-uuid', self.args.osd_uuid)
        if self.args.cluster_uuid is None:
            self.args.cluster_uuid = get_fsid(cluster=self.args.cluster)
        write_one_line(path, 'ceph_fsid', self.args.cluster_uuid)
        self.create_key()
        self.symlink_spaces(path)
        write_one_line(path, 'magic', CEPH_LOCKBOX_ONDISK_MAGIC)
        if self.device is not None:
            command_check_call(
                [
                    'sgdisk',
                    '--typecode={num}:{uuid}'.format(
                        num=self.partition.get_partition_number(),
                        uuid=self.partition.ptype_for_name('lockbox'),
                    ),
                    '--',
                    get_partition_base(self.partition.get_dev()),
                ],
            )

    def get_mount_point(self):
        return os.path.join(STATEDIR, 'osd-lockbox', self.args.osd_uuid)

    def get_osd_uuid(self):
        return self.args.osd_uuid

    def activate(self):
        path = is_mounted(self.partition.get_dev())
        if path:
            LOG.info("Lockbox already mounted at " + path)
            return

        path = tempfile.mkdtemp(
            prefix='mnt.',
            dir=STATEDIR + '/tmp',
        )
        args = ['mount', '-t', 'ext4', '-o', 'ro',
                self.partition.get_dev(),
                path]
        LOG.debug('Mounting lockbox temporarily ' + str(" ".join(args)))
        command_check_call(args)
        self.args.osd_uuid = get_oneliner(path, 'osd-uuid')
        command_check_call(['umount', path])
        LOG.debug('Mounting lockbox readonly ' + str(" ".join(args)))
        args = ['mount', '-t', 'ext4', '-o', 'ro',
                self.partition.get_dev(),
                self.get_mount_point()]
        command_check_call(args)
        for name in Space.NAMES + ('osd',):
            uuid_path = os.path.join(self.get_mount_point(), name + '-uuid')
            if os.path.exists(uuid_path):
                uuid = get_oneliner(self.get_mount_point(), name + '-uuid')
                dev = os.path.join('/dev/disk/by-partuuid/', uuid.lower())
                args = ['ceph-disk', 'trigger', dev]
                command_check_call(args)

    def prepare(self):
        verify_not_in_use(self.args.lockbox, check_partitions=True)
        self.set_or_create_partition()
        self.populate()


class PrepareData(object):

    FILE = 1
    DEVICE = 2

    def __init__(self, args):

        self.args = args
        self.partition = None
        self.set_type()
        if self.args.cluster_uuid is None:
            self.args.cluster_uuid = get_fsid(cluster=self.args.cluster)

        if self.args.osd_uuid is None:
            self.args.osd_uuid = str(uuid.uuid4())

    def set_type(self):
        dmode = os.stat(self.args.data).st_mode

        if stat.S_ISDIR(dmode):
            self.type = self.FILE
        elif stat.S_ISBLK(dmode):
            self.type = self.DEVICE
        else:
            raise Error('not a dir or block device', self.args.data)

    def is_file(self):
        return self.type == self.FILE

    def is_device(self):
        return self.type == self.DEVICE

    @staticmethod
    def parser():
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '--fs-type',
            help='file system type to use (e.g. "ext4")',
        )
        parser.add_argument(
            '--zap-disk',
            action='store_true', default=None,
            help='destroy the partition table (and content) of a disk',
        )
        parser.add_argument(
            '--data-dir',
            action='store_true', default=None,
            help='verify that DATA is a dir',
        )
        parser.add_argument(
            '--data-dev',
            action='store_true', default=None,
            help='verify that DATA is a block device',
        )
        parser.add_argument(
            'data',
            metavar='DATA',
            help='path to OSD data (a disk block device or directory)',
        )
        return parser

    def populate_data_path_file(self, path, *to_prepare_list):
        self.populate_data_path(path, *to_prepare_list)

    def populate_data_path(self, path, *to_prepare_list):
        if os.path.exists(os.path.join(path, 'magic')):
            LOG.debug('Data dir %s already exists', path)
            return
        else:
            LOG.debug('Preparing osd data dir %s', path)

        if self.args.osd_uuid is None:
            self.args.osd_uuid = str(uuid.uuid4())

        write_one_line(path, 'ceph_fsid', self.args.cluster_uuid)
        write_one_line(path, 'fsid', self.args.osd_uuid)
        write_one_line(path, 'magic', CEPH_OSD_ONDISK_MAGIC)

        for to_prepare in to_prepare_list:
            to_prepare.populate_data_path(path)

    def prepare(self, *to_prepare_list):
        if self.type == self.DEVICE:
            self.prepare_device(*to_prepare_list)
        elif self.type == self.FILE:
            self.prepare_file(*to_prepare_list)
        else:
            raise Error('unexpected type ', self.type)

    def prepare_file(self, *to_prepare_list):

        if not os.path.exists(self.args.data):
            raise Error('data path for directory does not exist',
                        self.args.data)

        if self.args.data_dev:
            raise Error('data path is not a block device', self.args.data)

        for to_prepare in to_prepare_list:
            to_prepare.prepare()

        self.populate_data_path_file(self.args.data, *to_prepare_list)

    def sanity_checks(self):
        if not os.path.exists(self.args.data):
            raise Error('data path for device does not exist',
                        self.args.data)
        verify_not_in_use(self.args.data,
                          check_partitions=not self.args.dmcrypt)

    def set_variables(self):
        if self.args.fs_type is None:
            self.args.fs_type = get_conf(
                cluster=self.args.cluster,
                variable='osd_mkfs_type',
            )
            if self.args.fs_type is None:
                self.args.fs_type = get_conf(
                    cluster=self.args.cluster,
                    variable='osd_fs_type',
                )
            if self.args.fs_type is None:
                self.args.fs_type = DEFAULT_FS_TYPE

        self.mkfs_args = get_conf(
            cluster=self.args.cluster,
            variable='osd_mkfs_options_{fstype}'.format(
                fstype=self.args.fs_type,
            ),
        )
        if self.mkfs_args is None:
            self.mkfs_args = get_conf(
                cluster=self.args.cluster,
                variable='osd_fs_mkfs_options_{fstype}'.format(
                    fstype=self.args.fs_type,
                ),
            )

        self.mount_options = get_mount_options(cluster=self.args.cluster,
                                               fs_type=self.args.fs_type)

        if self.args.osd_uuid is None:
            self.args.osd_uuid = str(uuid.uuid4())

    def prepare_device(self, *to_prepare_list):
        self.sanity_checks()
        self.set_variables()
        if self.args.zap_disk is not None:
            zap(self.args.data)

    def create_data_partition(self):
        device = Device.factory(self.args.data, self.args)
        partition_number = 1
        device.create_partition(uuid=self.args.osd_uuid,
                                name='data',
                                num=partition_number,
                                size=self.get_space_size())
        return device.get_partition(partition_number)

    def set_data_partition(self):
        if is_partition(self.args.data):
            LOG.debug('OSD data device %s is a partition',
                      self.args.data)
            self.partition = DevicePartition.factory(
                path=None, dev=self.args.data, args=self.args)
            ptype = self.partition.get_ptype()
            ready = Ptype.get_ready_by_name('osd')
            if ptype not in ready:
                LOG.warning('incorrect partition UUID: %s, expected %s'
                            % (ptype, str(ready)))
        else:
            LOG.debug('Creating osd partition on %s',
                      self.args.data)
            self.partition = self.create_data_partition()

    def populate_data_path_device(self, *to_prepare_list):
        partition = self.partition

        if isinstance(partition, DevicePartitionCrypt):
            partition.map()

        try:
            args = [
                'mkfs',
                '-t',
                self.args.fs_type,
            ]
            if self.mkfs_args is not None:
                args.extend(self.mkfs_args.split())
                if self.args.fs_type == 'xfs':
                    args.extend(['-f'])  # always force
            else:
                args.extend(MKFS_ARGS.get(self.args.fs_type, []))
            args.extend([
                '--',
                partition.get_dev(),
            ])
            try:
                LOG.debug('Creating %s fs on %s',
                          self.args.fs_type, partition.get_dev())
                command_check_call(args)
            except subprocess.CalledProcessError as e:
                raise Error(e)

            path = mount(dev=partition.get_dev(),
                         fstype=self.args.fs_type,
                         options=self.mount_options)

            try:
                self.populate_data_path(path, *to_prepare_list)
            finally:
                path_set_context(path)
                unmount(path)
        finally:
            if isinstance(partition, DevicePartitionCrypt):
                partition.unmap()

        if not is_partition(self.args.data):
            try:
                command_check_call(
                    [
                        'sgdisk',
                        '--typecode=%d:%s' % (partition.get_partition_number(),
                                              partition.ptype_for_name('osd')),
                        '--',
                        self.args.data,
                    ],
                )
            except subprocess.CalledProcessError as e:
                raise Error(e)
            update_partition(self.args.data, 'prepared')
            command_check_call(['udevadm', 'trigger',
                                '--action=add',
                                '--sysname-match',
                                os.path.basename(partition.rawdev)])


class PrepareFilestoreData(PrepareData):

    def get_space_size(self):
        return 0  # get as much space as possible

    def prepare_device(self, *to_prepare_list):
        super(PrepareFilestoreData, self).prepare_device(*to_prepare_list)
        for to_prepare in to_prepare_list:
            to_prepare.prepare()
        self.set_data_partition()
        self.populate_data_path_device(*to_prepare_list)


class PrepareBluestoreData(PrepareData):

    def get_space_size(self):
        return 100  # MB

    def prepare_device(self, *to_prepare_list):
        super(PrepareBluestoreData, self).prepare_device(*to_prepare_list)
        self.set_data_partition()
        for to_prepare in to_prepare_list:
            to_prepare.prepare()
        self.populate_data_path_device(*to_prepare_list)

    def populate_data_path(self, path, *to_prepare_list):
        super(PrepareBluestoreData, self).populate_data_path(path,
                                                             *to_prepare_list)
        write_one_line(path, 'type', 'bluestore')


#
# Temporary workaround: if ceph-osd --mkfs does not
# complete within 5 minutes, assume it is blocked
# because of http://tracker.ceph.com/issues/13522
# and retry a few times.
#
# Remove this function calls with command_check_call
# when http://tracker.ceph.com/issues/13522 is fixed
#
def ceph_osd_mkfs(arguments):
    timeout = _get_command_executable(['timeout'])
    mkfs_ok = False
    error = 'unknown error'
    for delay in os.environ.get('CEPH_OSD_MKFS_DELAYS',
                                '300 300 300 300 300').split():
        try:
            _check_output(timeout + [delay] + arguments)
            mkfs_ok = True
            break
        except subprocess.CalledProcessError as e:
            error = e.output
            if e.returncode == 124:  # timeout fired, retry
                LOG.debug('%s timed out : %s (retry)'
                          % (str(arguments), error))
            else:
                break
    if not mkfs_ok:
        raise Error('%s failed : %s' % (str(arguments), error))


def mkfs(
    path,
    cluster,
    osd_id,
    fsid,
    keyring,
):
    monmap = os.path.join(path, 'activate.monmap')
    command_check_call(
        [
            'ceph',
            '--cluster', cluster,
            '--name', 'client.bootstrap-osd',
            '--keyring', keyring,
            'mon', 'getmap', '-o', monmap,
        ],
    )

    osd_type = read_one_line(path, 'type')

    if osd_type == 'bluestore':
        ceph_osd_mkfs(
            [
                'ceph-osd',
                '--cluster', cluster,
                '--mkfs',
                '--mkkey',
                '-i', osd_id,
                '--monmap', monmap,
                '--osd-data', path,
                '--osd-uuid', fsid,
                '--keyring', os.path.join(path, 'keyring'),
                '--setuser', get_ceph_user(),
                '--setgroup', get_ceph_group(),
            ],
        )
    else:
        ceph_osd_mkfs(
            [
                'ceph-osd',
                '--cluster', cluster,
                '--mkfs',
                '--mkkey',
                '-i', osd_id,
                '--monmap', monmap,
                '--osd-data', path,
                '--osd-journal', os.path.join(path, 'journal'),
                '--osd-uuid', fsid,
                '--keyring', os.path.join(path, 'keyring'),
                '--setuser', get_ceph_user(),
                '--setgroup', get_ceph_group(),
            ],
        )


def auth_key(
    path,
    cluster,
    osd_id,
    keyring,
):
    try:
        # try dumpling+ cap scheme
        command_check_call(
            [
                'ceph',
                '--cluster', cluster,
                '--name', 'client.bootstrap-osd',
                '--keyring', keyring,
                'auth', 'add', 'osd.{osd_id}'.format(osd_id=osd_id),
                '-i', os.path.join(path, 'keyring'),
                'osd', 'allow *',
                'mon', 'allow profile osd',
            ],
        )
    except subprocess.CalledProcessError as err:
        if err.returncode == errno.EINVAL:
            # try old cap scheme
            command_check_call(
                [
                    'ceph',
                    '--cluster', cluster,
                    '--name', 'client.bootstrap-osd',
                    '--keyring', keyring,
                    'auth', 'add', 'osd.{osd_id}'.format(osd_id=osd_id),
                    '-i', os.path.join(path, 'keyring'),
                    'osd', 'allow *',
                    'mon', 'allow rwx',
                ],
            )
        else:
            raise


def get_mount_point(cluster, osd_id):
    parent = STATEDIR + '/osd'
    return os.path.join(
        parent,
        '{cluster}-{osd_id}'.format(cluster=cluster, osd_id=osd_id),
    )


def move_mount(
    dev,
    path,
    cluster,
    osd_id,
    fstype,
    mount_options,
):
    LOG.debug('Moving mount to final location...')
    osd_data = get_mount_point(cluster, osd_id)
    maybe_mkdir(osd_data)

    # pick best-of-breed mount options based on fs type
    if mount_options is None:
        mount_options = MOUNT_OPTIONS.get(fstype, '')

    # we really want to mount --move, but that is not supported when
    # the parent mount is shared, as it is by default on RH, Fedora,
    # and probably others.  Also, --bind doesn't properly manipulate
    # /etc/mtab, which *still* isn't a symlink to /proc/mounts despite
    # this being 2013.  Instead, mount the original device at the final
    # location.
    command_check_call(
        [
            '/bin/mount',
            '-o',
            mount_options,
            '--',
            dev,
            osd_data,
        ],
    )
    command_check_call(
        [
            '/bin/umount',
            '-l',   # lazy, in case someone else is peeking at the
                    # wrong moment
            '--',
            path,
        ],
    )


#
# For upgrade purposes, to make sure there are no competing units,
# both --runtime unit and the default should be disabled. There can be
# two units at the same time: one with --runtime and another without
# it. If, for any reason (manual or ceph-disk) the two units co-exist
# they will compete with each other.
#
def systemd_disable(
    path,
    osd_id,
):
    # ensure there is no duplicate ceph-osd@.service
    for style in ([], ['--runtime']):
        command_check_call(
            [
                'systemctl',
                'disable',
                'ceph-osd@{osd_id}'.format(osd_id=osd_id),
            ] + style,
        )


def systemd_start(
    path,
    osd_id,
):
    systemd_disable(path, osd_id)
    if is_mounted(path):
        style = ['--runtime']
    else:
        style = []
    command_check_call(
        [
            'systemctl',
            'enable',
            'ceph-osd@{osd_id}'.format(osd_id=osd_id),
        ] + style,
    )
    command_check_call(
        [
            'systemctl',
            'start',
            'ceph-osd@{osd_id}'.format(osd_id=osd_id),
        ],
    )


def systemd_stop(
    path,
    osd_id,
):
    systemd_disable(path, osd_id)
    command_check_call(
        [
            'systemctl',
            'stop',
            'ceph-osd@{osd_id}'.format(osd_id=osd_id),
        ],
    )


def start_daemon(
    cluster,
    osd_id,
):
    LOG.debug('Starting %s osd.%s...', cluster, osd_id)

    path = (STATEDIR + '/osd/{cluster}-{osd_id}').format(
        cluster=cluster, osd_id=osd_id)

    try:
        if os.path.exists(os.path.join(path, 'upstart')):
            command_check_call(
                [
                    '/sbin/initctl',
                    # use emit, not start, because start would fail if the
                    # instance was already running
                    'emit',
                    # since the daemon starting doesn't guarantee much about
                    # the service being operational anyway, don't bother
                    # waiting for it
                    '--no-wait',
                    '--',
                    'ceph-osd',
                    'cluster={cluster}'.format(cluster=cluster),
                    'id={osd_id}'.format(osd_id=osd_id),
                ],
            )
        elif os.path.exists(os.path.join(path, 'sysvinit')):
            if os.path.exists('/usr/sbin/service'):
                svc = '/usr/sbin/service'
            else:
                svc = '/sbin/service'
            command_check_call(
                [
                    svc,
                    'ceph',
                    '--cluster',
                    '{cluster}'.format(cluster=cluster),
                    'start',
                    'osd.{osd_id}'.format(osd_id=osd_id),
                ],
            )
        elif os.path.exists(os.path.join(path, 'systemd')):
            systemd_start(path, osd_id)
        else:
            raise Error('{cluster} osd.{osd_id} is not tagged '
                        'with an init system'.format(
                            cluster=cluster,
                            osd_id=osd_id,
                        ))
    except subprocess.CalledProcessError as e:
        raise Error('ceph osd start failed', e)


def stop_daemon(
    cluster,
    osd_id,
):
    LOG.debug('Stoping %s osd.%s...', cluster, osd_id)

    path = (STATEDIR + '/osd/{cluster}-{osd_id}').format(
        cluster=cluster, osd_id=osd_id)

    try:
        if os.path.exists(os.path.join(path, 'upstart')):
            command_check_call(
                [
                    '/sbin/initctl',
                    'stop',
                    'ceph-osd',
                    'cluster={cluster}'.format(cluster=cluster),
                    'id={osd_id}'.format(osd_id=osd_id),
                ],
            )
        elif os.path.exists(os.path.join(path, 'sysvinit')):
            svc = which('service')
            command_check_call(
                [
                    svc,
                    'ceph',
                    '--cluster',
                    '{cluster}'.format(cluster=cluster),
                    'stop',
                    'osd.{osd_id}'.format(osd_id=osd_id),
                ],
            )
        elif os.path.exists(os.path.join(path, 'systemd')):
            systemd_stop(path, osd_id)
        else:
            raise Error('{cluster} osd.{osd_id} is not tagged with an init '
                        ' system'.format(cluster=cluster, osd_id=osd_id))
    except subprocess.CalledProcessError as e:
        raise Error('ceph osd stop failed', e)


def detect_fstype(
    dev,
):
    fstype = _check_output(
        args=[
            '/sbin/blkid',
            # we don't want stale cached results
            '-p',
            '-s', 'TYPE',
            '-o', 'value',
            '--',
            dev,
        ],
    )
    fstype = must_be_one_line(fstype)
    return fstype


def dmcrypt_is_mapped(uuid):
    path = os.path.join('/dev/mapper', uuid)
    if os.path.exists(path):
        return path
    else:
        return None


def dmcrypt_map(dev, dmcrypt_key_dir):
    ptype = get_partition_type(dev)
    if ptype in Ptype.get_ready_by_type('plain'):
        luks = False
        cryptsetup_parameters = ['--key-size', '256']
    elif ptype in Ptype.get_ready_by_type('luks'):
        luks = True
        cryptsetup_parameters = []
    else:
        raise Error('--dmcrypt called for dev %s with invalid ptype %s'
                    % (dev, ptype))
    part_uuid = get_partition_uuid(dev)
    dmcrypt_key = get_dmcrypt_key(part_uuid, dmcrypt_key_dir, luks)
    return _dmcrypt_map(
        rawdev=dev,
        key=dmcrypt_key,
        _uuid=part_uuid,
        cryptsetup_parameters=cryptsetup_parameters,
        luks=luks,
        format_dev=False,
    )


def mount_activate(
    dev,
    activate_key_template,
    init,
    dmcrypt,
    dmcrypt_key_dir,
    reactivate=False,
):

    if dmcrypt:
        part_uuid = get_partition_uuid(dev)
        dev = dmcrypt_map(dev, dmcrypt_key_dir)
    try:
        fstype = detect_fstype(dev=dev)
    except (subprocess.CalledProcessError,
            TruncatedLineError,
            TooManyLinesError) as e:
        raise FilesystemTypeError(
            'device {dev}'.format(dev=dev),
            e,
        )

    # TODO always using mount options from cluster=ceph for
    # now; see http://tracker.newdream.net/issues/3253
    mount_options = get_mount_options(cluster='ceph', fs_type=fstype)

    path = mount(dev=dev, fstype=fstype, options=mount_options)

    # check if the disk is deactive, change the journal owner, group
    # mode for correct user and group.
    if os.path.exists(os.path.join(path, 'deactive')):
        # logging to syslog will help us easy to know udev triggered failure
        if not reactivate:
            unmount(path)
            # we need to unmap again because dmcrypt map will create again
            # on bootup stage (due to deactivate)
            if '/dev/mapper/' in dev:
                part_uuid = dev.replace('/dev/mapper/', '')
                dmcrypt_unmap(part_uuid)
            LOG.info('OSD deactivated! reactivate with: --reactivate')
            raise Error('OSD deactivated! reactivate with: --reactivate')
        # flag to activate a deactive osd.
        deactive = True
    else:
        deactive = False

    osd_id = None
    cluster = None
    try:
        (osd_id, cluster) = activate(path, activate_key_template, init)

        # Now active successfully
        # If we got reactivate and deactive, remove the deactive file
        if deactive and reactivate:
            os.remove(os.path.join(path, 'deactive'))
            LOG.info('Remove `deactive` file.')

        # check if the disk is already active, or if something else is already
        # mounted there
        active = False
        other = False
        src_dev = os.stat(path).st_dev
        try:
            dst_dev = os.stat((STATEDIR + '/osd/{cluster}-{osd_id}').format(
                cluster=cluster,
                osd_id=osd_id)).st_dev
            if src_dev == dst_dev:
                active = True
            else:
                parent_dev = os.stat(STATEDIR + '/osd').st_dev
                if dst_dev != parent_dev:
                    other = True
                elif os.listdir(get_mount_point(cluster, osd_id)):
                    LOG.info(get_mount_point(cluster, osd_id) +
                             " is not empty, won't override")
                    other = True

        except OSError:
            pass

        if active:
            LOG.info('%s osd.%s already mounted in position; unmounting ours.'
                     % (cluster, osd_id))
            unmount(path)
        elif other:
            raise Error('another %s osd.%s already mounted in position '
                        '(old/different cluster instance?); unmounting ours.'
                        % (cluster, osd_id))
        else:
            move_mount(
                dev=dev,
                path=path,
                cluster=cluster,
                osd_id=osd_id,
                fstype=fstype,
                mount_options=mount_options,
            )
        return cluster, osd_id

    except:
        LOG.error('Failed to activate')
        unmount(path)
        raise
    finally:
        # remove our temp dir
        if os.path.exists(path):
            os.rmdir(path)


def activate_dir(
    path,
    activate_key_template,
    init,
):

    if not os.path.exists(path):
        raise Error(
            'directory %s does not exist' % path
        )

    (osd_id, cluster) = activate(path, activate_key_template, init)

    if init not in (None, 'none'):
        canonical = (STATEDIR + '/osd/{cluster}-{osd_id}').format(
            cluster=cluster,
            osd_id=osd_id)
        if path != canonical:
            # symlink it from the proper location
            create = True
            if os.path.lexists(canonical):
                old = os.readlink(canonical)
                if old != path:
                    LOG.debug('Removing old symlink %s -> %s', canonical, old)
                    try:
                        os.unlink(canonical)
                    except:
                        raise Error('unable to remove old symlink', canonical)
                else:
                    create = False
            if create:
                LOG.debug('Creating symlink %s -> %s', canonical, path)
                try:
                    os.symlink(path, canonical)
                except:
                    raise Error('unable to create symlink %s -> %s'
                                % (canonical, path))

    return cluster, osd_id


def find_cluster_by_uuid(_uuid):
    """
    Find a cluster name by searching /etc/ceph/*.conf for a conf file
    with the right uuid.
    """
    _uuid = _uuid.lower()
    no_fsid = []
    if not os.path.exists(SYSCONFDIR):
        return None
    for conf_file in os.listdir(SYSCONFDIR):
        if not conf_file.endswith('.conf'):
            continue
        cluster = conf_file[:-5]
        try:
            fsid = get_fsid(cluster)
        except Error as e:
            if 'getting cluster uuid from configuration failed' not in str(e):
                raise e
            no_fsid.append(cluster)
        else:
            if fsid == _uuid:
                return cluster
    # be tolerant of /etc/ceph/ceph.conf without an fsid defined.
    if len(no_fsid) == 1 and no_fsid[0] == 'ceph':
        LOG.warning('No fsid defined in ' + SYSCONFDIR +
                    '/ceph.conf; using anyway')
        return 'ceph'
    return None


def activate(
    path,
    activate_key_template,
    init,
):

    check_osd_magic(path)

    ceph_fsid = read_one_line(path, 'ceph_fsid')
    if ceph_fsid is None:
        raise Error('No cluster uuid assigned.')
    LOG.debug('Cluster uuid is %s', ceph_fsid)

    cluster = find_cluster_by_uuid(ceph_fsid)
    if cluster is None:
        raise Error('No cluster conf found in ' + SYSCONFDIR +
                    ' with fsid %s' % ceph_fsid)
    LOG.debug('Cluster name is %s', cluster)

    fsid = read_one_line(path, 'fsid')
    if fsid is None:
        raise Error('No OSD uuid assigned.')
    LOG.debug('OSD uuid is %s', fsid)

    keyring = activate_key_template.format(cluster=cluster,
                                           statedir=STATEDIR)

    osd_id = get_osd_id(path)
    if osd_id is None:
        osd_id = allocate_osd_id(
            cluster=cluster,
            fsid=fsid,
            keyring=keyring,
        )
        write_one_line(path, 'whoami', osd_id)
    LOG.debug('OSD id is %s', osd_id)

    if not os.path.exists(os.path.join(path, 'ready')):
        LOG.debug('Initializing OSD...')
        # re-running mkfs is safe, so just run until it completes
        mkfs(
            path=path,
            cluster=cluster,
            osd_id=osd_id,
            fsid=fsid,
            keyring=keyring,
        )

    if init not in (None, 'none'):
        if init == 'auto':
            conf_val = get_conf(
                cluster=cluster,
                variable='init'
            )
            if conf_val is not None:
                init = conf_val
            else:
                init = init_get()

        LOG.debug('Marking with init system %s', init)
        init_path = os.path.join(path, init)
        with open(init_path, 'w'):
            path_set_context(init_path)

    # remove markers for others, just in case.
    for other in INIT_SYSTEMS:
        if other != init:
            try:
                os.unlink(os.path.join(path, other))
            except OSError:
                pass

    if not os.path.exists(os.path.join(path, 'active')):
        LOG.debug('Authorizing OSD key...')
        auth_key(
            path=path,
            cluster=cluster,
            osd_id=osd_id,
            keyring=keyring,
        )
        write_one_line(path, 'active', 'ok')
    LOG.debug('%s osd.%s data dir is ready at %s', cluster, osd_id, path)
    return (osd_id, cluster)


def main_activate(args):
    cluster = None
    osd_id = None

    LOG.info('path = ' + str(args.path))
    if not os.path.exists(args.path):
        raise Error('%s does not exist' % args.path)

    if is_suppressed(args.path):
        LOG.info('suppressed activate request on %s', args.path)
        return

    with activate_lock:
        mode = os.stat(args.path).st_mode
        if stat.S_ISBLK(mode):
            if (is_partition(args.path) and
                    (get_partition_type(args.path) ==
                     PTYPE['mpath']['osd']['ready']) and
                    not is_mpath(args.path)):
                raise Error('%s is not a multipath block device' %
                            args.path)
            (cluster, osd_id) = mount_activate(
                dev=args.path,
                activate_key_template=args.activate_key_template,
                init=args.mark_init,
                dmcrypt=args.dmcrypt,
                dmcrypt_key_dir=args.dmcrypt_key_dir,
                reactivate=args.reactivate,
            )
            osd_data = get_mount_point(cluster, osd_id)

        elif stat.S_ISDIR(mode):
            (cluster, osd_id) = activate_dir(
                path=args.path,
                activate_key_template=args.activate_key_template,
                init=args.mark_init,
            )
            osd_data = args.path

        else:
            raise Error('%s is not a directory or block device' % args.path)

        # exit with 0 if the journal device is not up, yet
        # journal device will do the activation
        osd_journal = '{path}/journal'.format(path=osd_data)
        if os.path.islink(osd_journal) and not os.access(osd_journal, os.F_OK):
            LOG.info("activate: Journal not present, not starting, yet")
            return

        if (not args.no_start_daemon and args.mark_init == 'none'):
            command_check_call(
                [
                    'ceph-osd',
                    '--cluster={cluster}'.format(cluster=cluster),
                    '--id={osd_id}'.format(osd_id=osd_id),
                    '--osd-data={path}'.format(path=osd_data),
                    '--osd-journal={journal}'.format(journal=osd_journal),
                ],
            )

        if (not args.no_start_daemon and
                args.mark_init not in (None, 'none')):

            start_daemon(
                cluster=cluster,
                osd_id=osd_id,
            )


def main_activate_lockbox(args):
    with activate_lock:
        main_activate_lockbox_protected(args)


def main_activate_lockbox_protected(args):
    partition = DevicePartition.factory(
        path=None, dev=args.path, args=args)

    lockbox = Lockbox(args)
    lockbox.set_partition(partition)
    lockbox.activate()


###########################

def _mark_osd_out(cluster, osd_id):
    LOG.info('Prepare to mark osd.%d out...', osd_id)
    command([
        'ceph',
        'osd',
        'out',
        'osd.%d' % osd_id,
    ])


def _check_osd_status(cluster, osd_id):
    """
    report the osd status:
    00(0) : means OSD OUT AND DOWN
    01(1) : means OSD OUT AND UP
    10(2) : means OSD IN AND DOWN
    11(3) : means OSD IN AND UP
    """
    LOG.info("Checking osd id: %s ..." % osd_id)
    found = False
    status_code = 0
    out, err, ret = command([
        'ceph',
        'osd',
        'dump',
        '--cluster={cluster}'.format(
            cluster=cluster,
        ),
        '--format',
        'json',
    ])
    out_json = json.loads(out)
    for item in out_json[u'osds']:
        if item.get(u'osd') == int(osd_id):
            found = True
            if item.get(u'in') is 1:
                status_code += 2
            if item.get(u'up') is 1:
                status_code += 1
    if not found:
        raise Error('Could not osd.%s in osd tree!' % osd_id)
    return status_code


def _remove_osd_directory_files(mounted_path, cluster):
    """
    To remove the 'ready', 'active', INIT-specific files.
    """
    if os.path.exists(os.path.join(mounted_path, 'ready')):
        os.remove(os.path.join(mounted_path, 'ready'))
        LOG.info('Remove `ready` file.')
    else:
        LOG.info('`ready` file is already removed.')

    if os.path.exists(os.path.join(mounted_path, 'active')):
        os.remove(os.path.join(mounted_path, 'active'))
        LOG.info('Remove `active` file.')
    else:
        LOG.info('`active` file is already removed.')

    # Just check `upstart` and `sysvinit` directly if filename is init-spec.
    conf_val = get_conf(
        cluster=cluster,
        variable='init'
    )
    if conf_val is not None:
        init = conf_val
    else:
        init = init_get()
    os.remove(os.path.join(mounted_path, init))
    LOG.info('Remove `%s` file.', init)
    return


def main_deactivate(args):
    with activate_lock:
        main_deactivate_locked(args)


def main_deactivate_locked(args):
    osd_id = args.deactivate_by_id
    path = args.path
    target_dev = None
    dmcrypt = False
    devices = list_devices()

    # list all devices and found we need
    for device in devices:
        if 'partitions' in device:
            for dev_part in device.get('partitions'):
                if (osd_id and
                        'whoami' in dev_part and
                        dev_part['whoami'] == osd_id):
                    target_dev = dev_part
                elif (path and
                        'path' in dev_part and
                        dev_part['path'] == path):
                    target_dev = dev_part
    if not target_dev:
        raise Error('Cannot find any match device!!')

    # set up all we need variable
    osd_id = target_dev['whoami']
    part_type = target_dev['ptype']
    mounted_path = target_dev['mount']
    if Ptype.is_dmcrypt(part_type, 'osd'):
        dmcrypt = True

    # Do not do anything if osd is already down.
    status_code = _check_osd_status(args.cluster, osd_id)
    if status_code == OSD_STATUS_IN_UP:
        if args.mark_out is True:
            _mark_osd_out(args.cluster, int(osd_id))
        stop_daemon(args.cluster, osd_id)
    elif status_code == OSD_STATUS_IN_DOWN:
        if args.mark_out is True:
            _mark_osd_out(args.cluster, int(osd_id))
        LOG.info("OSD already out/down. Do not do anything now.")
        return
    elif status_code == OSD_STATUS_OUT_UP:
        stop_daemon(args.cluster, osd_id)
    elif status_code == OSD_STATUS_OUT_DOWN:
        LOG.info("OSD already out/down. Do not do anything now.")
        return

    if not args.once:
        # remove 'ready', 'active', and INIT-specific files.
        _remove_osd_directory_files(mounted_path, args.cluster)

        # Write deactivate to osd directory!
        with open(os.path.join(mounted_path, 'deactive'), 'w'):
            path_set_context(os.path.join(mounted_path, 'deactive'))

    unmount(mounted_path)
    LOG.info("Umount `%s` successfully.", mounted_path)

    if dmcrypt:
        lockbox = os.path.join(STATEDIR, 'osd-lockbox')
        command(['umount', os.path.join(lockbox, target_dev['uuid'])])

        dmcrypt_unmap(target_dev['uuid'])
        for name in Space.NAMES:
            if name + '_uuid' in target_dev:
                dmcrypt_unmap(target_dev[name + '_uuid'])

###########################


def _remove_from_crush_map(cluster, osd_id):
    LOG.info("Prepare to remove osd.%s from crush map..." % osd_id)
    command([
        'ceph',
        'osd',
        'crush',
        'remove',
        'osd.%s' % osd_id,
    ])


def _delete_osd_auth_key(cluster, osd_id):
    LOG.info("Prepare to delete osd.%s cephx key..." % osd_id)
    command([
        'ceph',
        'auth',
        'del',
        'osd.%s' % osd_id,
    ])


def _deallocate_osd_id(cluster, osd_id):
    LOG.info("Prepare to deallocate the osd-id: %s..." % osd_id)
    command([
        'ceph',
        'osd',
        'rm',
        '%s' % osd_id,
    ])


def _remove_lockbox(uuid, cluster):
    command([
        'ceph',
        '--cluster', cluster,
        'auth',
        'del',
        'client.osd-lockbox.' + uuid,
    ])
    command([
        'ceph',
        '--cluster', cluster,
        'config-key',
        'del',
        'dm-crypt/osd/' + uuid + '/luks',
    ])
    lockbox = os.path.join(STATEDIR, 'osd-lockbox')
    if not os.path.exists(lockbox):
        return
    canonical = os.path.join(lockbox, uuid)
    command(['umount', canonical])
    for name in os.listdir(lockbox):
        path = os.path.join(lockbox, name)
        if os.path.islink(path) and os.readlink(path) == canonical:
            os.unlink(path)


def destroy_lookup_device(args, predicate, description):
    devices = list_devices()
    for device in devices:
        for partition in device.get('partitions', []):
            if partition['type'] == 'lockbox':
                if not is_mounted(partition['path']):
                    main_activate_lockbox_protected(
                        argparse.Namespace(verbose=args.verbose,
                                           path=partition['path']))
    for device in devices:
        for partition in device.get('partitions', []):
            if partition['dmcrypt']:
                dmcrypt_path = dmcrypt_is_mapped(partition['uuid'])
                if dmcrypt_path:
                    unmap = False
                else:
                    dmcrypt_path = dmcrypt_map(partition['path'],
                                               args.dmcrypt_key_dir)
                    unmap = True
                list_dev_osd(dmcrypt_path, {}, partition)
                if unmap:
                    dmcrypt_unmap(partition['uuid'])
                dmcrypt = True
            else:
                dmcrypt = False
            if predicate(partition):
                return dmcrypt, partition
    raise Error('found no device matching ', description)


def main_destroy(args):
    with activate_lock:
        main_destroy_locked(args)


def main_destroy_locked(args):
    osd_id = args.destroy_by_id
    path = args.path
    target_dev = None

    if path:
        if not is_partition(path):
            raise Error(path + " must be a partition device")
        path = os.path.realpath(path)

    if path:
        (dmcrypt, target_dev) = destroy_lookup_device(
            args, lambda x: x.get('path') == path,
            path)
    elif osd_id:
        (dmcrypt, target_dev) = destroy_lookup_device(
            args, lambda x: x.get('whoami') == osd_id,
            'osd id ' + str(osd_id))

    osd_id = target_dev['whoami']
    dev_path = target_dev['path']
    if target_dev['ptype'] == PTYPE['mpath']['osd']['ready']:
        base_dev = get_partition_base_mpath(dev_path)
    else:
        base_dev = get_partition_base(dev_path)

    # Before osd deactivate, we cannot destroy it
    status_code = _check_osd_status(args.cluster, osd_id)
    if status_code != OSD_STATUS_OUT_DOWN and \
       status_code != OSD_STATUS_IN_DOWN:
        raise Error("Could not destroy the active osd. (osd-id: %s)" %
                    osd_id)

    # Remove OSD from crush map
    _remove_from_crush_map(args.cluster, osd_id)

    # Remove OSD cephx key
    _delete_osd_auth_key(args.cluster, osd_id)

    # Deallocate OSD ID
    _deallocate_osd_id(args.cluster, osd_id)

    # we remove the crypt map and device mapper (if dmcrypt is True)
    if dmcrypt:
        for name in Space.NAMES:
            if target_dev.get(name + '_uuid'):
                dmcrypt_unmap(target_dev[name + '_uuid'])
        _remove_lockbox(target_dev['uuid'], args.cluster)

    # Check zap flag. If we found zap flag, we need to find device for
    # destroy this osd data.
    if args.zap is True:
        # erase the osd data
        LOG.info("Prepare to zap the device %s" % base_dev)
        zap(base_dev)


def get_space_osd_uuid(name, path):
    if not os.path.exists(path):
        raise Error('%s does not exist' % path)

    mode = os.stat(path).st_mode
    if not stat.S_ISBLK(mode):
        raise Error('%s is not a block device' % path)

    if (is_partition(path) and
            get_partition_type(path) in (PTYPE['mpath']['journal']['ready'],
                                         PTYPE['mpath']['block']['ready']) and
            not is_mpath(path)):
        raise Error('%s is not a multipath block device' %
                    path)

    try:
        out = _check_output(
            args=[
                'ceph-osd',
                '--get-device-fsid',
                path,
            ],
            close_fds=True,
        )
    except subprocess.CalledProcessError as e:
        raise Error(
            'failed to get osd uuid/fsid from %s' % name,
            e,
        )
    value = str(out).split('\n', 1)[0]
    LOG.debug('%s %s has OSD UUID %s', name.capitalize(), path, value)
    return value


def main_activate_space(name, args):
    if not os.path.exists(args.dev):
        raise Error('%s does not exist' % args.dev)

    if is_suppressed(args.dev):
        LOG.info('suppressed activate request on space %s', args.dev)
        return

    cluster = None
    osd_id = None
    osd_uuid = None
    dev = None
    with activate_lock:
        if args.dmcrypt:
            dev = dmcrypt_map(args.dev, args.dmcrypt_key_dir)
        else:
            dev = args.dev
        # FIXME: For an encrypted journal dev, does this return the
        # cyphertext or plaintext dev uuid!? Also, if the journal is
        # encrypted, is the data partition also always encrypted, or
        # are mixed pairs supported!?
        osd_uuid = get_space_osd_uuid(name, dev)
        path = os.path.join('/dev/disk/by-partuuid/', osd_uuid.lower())

        if is_suppressed(path):
            LOG.info('suppressed activate request on %s', path)
            return

        # warn and exit with 0 if the data device is not up, yet
        # data device will do the activation
        if not os.access(path, os.F_OK):
            LOG.info("activate: OSD device not present, not starting, yet")
            return

        (cluster, osd_id) = mount_activate(
            dev=path,
            activate_key_template=args.activate_key_template,
            init=args.mark_init,
            dmcrypt=args.dmcrypt,
            dmcrypt_key_dir=args.dmcrypt_key_dir,
            reactivate=args.reactivate,
        )

        start_daemon(
            cluster=cluster,
            osd_id=osd_id,
        )


###########################


def main_activate_all(args):
    dir = '/dev/disk/by-parttypeuuid'
    LOG.debug('Scanning %s', dir)
    if not os.path.exists(dir):
        return
    err = False
    for name in os.listdir(dir):
        if name.find('.') < 0:
            continue
        (tag, uuid) = name.split('.')

        if tag in Ptype.get_ready_by_name('osd'):

            if Ptype.is_dmcrypt(tag, 'osd'):
                path = os.path.join('/dev/mapper', uuid)
            else:
                path = os.path.join(dir, name)

            if is_suppressed(path):
                LOG.info('suppressed activate request on %s', path)
                continue

            LOG.info('Activating %s', path)
            with activate_lock:
                try:
                    # never map dmcrypt cyphertext devices
                    (cluster, osd_id) = mount_activate(
                        dev=path,
                        activate_key_template=args.activate_key_template,
                        init=args.mark_init,
                        dmcrypt=False,
                        dmcrypt_key_dir='',
                    )
                    start_daemon(
                        cluster=cluster,
                        osd_id=osd_id,
                    )

                except Exception as e:
                    print(
                        '{prog}: {msg}'.format(prog=args.prog, msg=e),
                        file=sys.stderr
                    )

                    err = True

    if err:
        raise Error('One or more partitions failed to activate')


###########################

def is_swap(dev):
    dev = os.path.realpath(dev)
    with open('/proc/swaps', 'rb') as proc_swaps:
        for line in proc_swaps.readlines()[1:]:
            fields = line.split()
            if len(fields) < 3:
                continue
            swaps_dev = fields[0]
            if os.path.isabs(swaps_dev) and os.path.exists(swaps_dev):
                swaps_dev = os.path.realpath(swaps_dev)
                if swaps_dev == dev:
                    return True
    return False


def get_oneliner(base, name):
    path = os.path.join(base, name)
    if os.path.isfile(path):
        with open(path, 'rb') as _file:
            return _bytes2str(_file.readline().rstrip())
    return None


def get_dev_fs(dev):
    fscheck, _, _ = command(
        [
            'blkid',
            '-s',
            'TYPE',
            dev,
        ],
    )
    if 'TYPE' in fscheck:
        fstype = fscheck.split()[1].split('"')[1]
        return fstype
    else:
        return None


def split_dev_base_partnum(dev):
    if is_mpath(dev):
        partnum = partnum_mpath(dev)
        base = get_partition_base_mpath(dev)
    else:
        b = block_path(dev)
        partnum = open(os.path.join(b, 'partition')).read().strip()
        base = get_partition_base(dev)
    return base, partnum


def get_partition_type(part):
    return get_blkid_partition_info(part, 'ID_PART_ENTRY_TYPE')


def get_partition_uuid(part):
    return get_blkid_partition_info(part, 'ID_PART_ENTRY_UUID')


def get_blkid_partition_info(dev, what=None):
    out, _, _ = command(
        [
            'blkid',
            '-o',
            'udev',
            '-p',
            dev,
        ]
    )
    p = {}
    for line in out.splitlines():
        (key, value) = line.split('=')
        p[key] = value
    if what:
        return p.get(what)
    else:
        return p


def more_osd_info(path, uuid_map, desc):
    desc['ceph_fsid'] = get_oneliner(path, 'ceph_fsid')
    if desc['ceph_fsid']:
        desc['cluster'] = find_cluster_by_uuid(desc['ceph_fsid'])
    desc['whoami'] = get_oneliner(path, 'whoami')
    for name in Space.NAMES:
        uuid = get_oneliner(path, name + '_uuid')
        if uuid:
            desc[name + '_uuid'] = uuid.lower()
            if desc[name + '_uuid'] in uuid_map:
                desc[name + '_dev'] = uuid_map[desc[name + '_uuid']]


def list_dev_osd(dev, uuid_map, desc):
    desc['mount'] = is_mounted(dev)
    desc['fs_type'] = get_dev_fs(dev)
    desc['state'] = 'unprepared'
    if desc['mount']:
        desc['state'] = 'active'
        more_osd_info(desc['mount'], uuid_map, desc)
    elif desc['fs_type']:
        try:
            tpath = mount(dev=dev, fstype=desc['fs_type'], options='')
            if tpath:
                try:
                    magic = get_oneliner(tpath, 'magic')
                    if magic is not None:
                        desc['magic'] = magic
                        desc['state'] = 'prepared'
                        more_osd_info(tpath, uuid_map, desc)
                finally:
                    unmount(tpath)
        except MountError:
            pass


def list_dev_lockbox(dev, uuid_map, desc):
    desc['mount'] = is_mounted(dev)
    desc['fs_type'] = get_dev_fs(dev)
    desc['state'] = 'unprepared'
    if desc['mount']:
        desc['state'] = 'active'
        desc['osd_uuid'] = get_oneliner(desc['mount'], 'osd-uuid')
    elif desc['fs_type']:
        try:
            tpath = tempfile.mkdtemp(prefix='mnt.', dir=STATEDIR + '/tmp')
            args = ['mount', '-t', 'ext4', dev, tpath]
            LOG.debug('Mounting lockbox ' + str(" ".join(args)))
            command_check_call(args)
            magic = get_oneliner(tpath, 'magic')
            if magic is not None:
                desc['magic'] = magic
                desc['state'] = 'prepared'
                desc['osd_uuid'] = get_oneliner(tpath, 'osd-uuid')
            unmount(tpath)
        except subprocess.CalledProcessError:
            pass
    if desc.get('osd_uuid') in uuid_map:
        desc['lockbox_for'] = uuid_map[desc['osd_uuid']]


def list_format_lockbox_plain(dev):
    desc = []
    if dev.get('lockbox_for'):
        desc.append('for ' + dev['lockbox_for'])
    elif dev.get('osd_uuid'):
        desc.append('for osd ' + dev['osd_uuid'])
    return desc


def list_format_more_osd_info_plain(dev):
    desc = []
    if dev.get('ceph_fsid'):
        if dev.get('cluster'):
            desc.append('cluster ' + dev['cluster'])
        else:
            desc.append('unknown cluster ' + dev['ceph_fsid'])
    if dev.get('whoami'):
        desc.append('osd.%s' % dev['whoami'])
    for name in Space.NAMES:
        if dev.get(name + '_dev'):
            desc.append(name + ' %s' % dev[name + '_dev'])
    return desc


def list_format_dev_plain(dev, prefix=''):
    desc = []
    if dev['ptype'] == PTYPE['regular']['osd']['ready']:
        desc = (['ceph data', dev['state']] +
                list_format_more_osd_info_plain(dev))
    elif dev['ptype'] in (PTYPE['regular']['lockbox']['ready'],
                          PTYPE['mpath']['lockbox']['ready']):
        desc = (['ceph lockbox', dev['state']] +
                list_format_lockbox_plain(dev))
    elif Ptype.is_dmcrypt(dev['ptype'], 'osd'):
        dmcrypt = dev['dmcrypt']
        if not dmcrypt['holders']:
            desc = ['ceph data (dmcrypt %s)' % dmcrypt['type'],
                    'not currently mapped']
        elif len(dmcrypt['holders']) == 1:
            holder = get_dev_path(dmcrypt['holders'][0])
            desc = ['ceph data (dmcrypt %s %s)' %
                    (dmcrypt['type'], holder)]
            desc += list_format_more_osd_info_plain(dev)
        else:
            desc = ['ceph data (dmcrypt %s)' % dmcrypt['type'],
                    'holders: ' + ','.join(dmcrypt['holders'])]
    elif Ptype.is_regular_space(dev['ptype']):
        name = Ptype.space_ptype_to_name(dev['ptype'])
        desc.append('ceph ' + name)
        if dev.get(name + '_for'):
            desc.append('for %s' % dev[name + '_for'])
    elif Ptype.is_dmcrypt_space(dev['ptype']):
        name = Ptype.space_ptype_to_name(dev['ptype'])
        dmcrypt = dev['dmcrypt']
        if dmcrypt['holders'] and len(dmcrypt['holders']) == 1:
            holder = get_dev_path(dmcrypt['holders'][0])
            desc = ['ceph ' + name + ' (dmcrypt %s %s)' %
                    (dmcrypt['type'], holder)]
        else:
            desc = ['ceph ' + name + ' (dmcrypt %s)' % dmcrypt['type']]
        if dev.get(name + '_for'):
            desc.append('for %s' % dev[name + '_for'])
    else:
        desc.append(dev['type'])
        if dev.get('fs_type'):
            desc.append(dev['fs_type'])
        elif dev.get('ptype'):
            desc.append(dev['ptype'])
        if dev.get('mount'):
            desc.append('mounted on %s' % dev['mount'])
    return '%s%s %s' % (prefix, dev['path'], ', '.join(desc))


def list_format_plain(devices):
    lines = []
    for device in devices:
        if device.get('partitions'):
            lines.append('%s :' % device['path'])
            for p in sorted(device['partitions']):
                lines.append(list_format_dev_plain(dev=p,
                                                   prefix=' '))
        else:
            lines.append(list_format_dev_plain(dev=device,
                                               prefix=''))
    return "\n".join(lines)


def list_dev(dev, uuid_map, space_map):
    info = {
        'path': dev,
        'dmcrypt': {},
    }

    info['is_partition'] = is_partition(dev)
    if info['is_partition']:
        ptype = get_partition_type(dev)
        info['uuid'] = get_partition_uuid(dev)
    else:
        ptype = 'unknown'
    info['ptype'] = ptype
    LOG.info("list_dev(dev = " + dev + ", ptype = " + str(ptype) + ")")
    if ptype in (PTYPE['regular']['osd']['ready'],
                 PTYPE['mpath']['osd']['ready']):
        info['type'] = 'data'
        if ptype == PTYPE['mpath']['osd']['ready']:
            info['multipath'] = True
        list_dev_osd(dev, uuid_map, info)
    elif ptype in (PTYPE['regular']['lockbox']['ready'],
                   PTYPE['mpath']['lockbox']['ready']):
        info['type'] = 'lockbox'
        if ptype == PTYPE['mpath']['osd']['ready']:
            info['multipath'] = True
        list_dev_lockbox(dev, uuid_map, info)
    elif ptype == PTYPE['plain']['osd']['ready']:
        holders = is_held(dev)
        info['type'] = 'data'
        info['dmcrypt']['holders'] = holders
        info['dmcrypt']['type'] = 'plain'
        if len(holders) == 1:
            list_dev_osd(get_dev_path(holders[0]), uuid_map, info)
    elif ptype == PTYPE['luks']['osd']['ready']:
        holders = is_held(dev)
        info['type'] = 'data'
        info['dmcrypt']['holders'] = holders
        info['dmcrypt']['type'] = 'LUKS'
        if len(holders) == 1:
            list_dev_osd(get_dev_path(holders[0]), uuid_map, info)
    elif Ptype.is_regular_space(ptype) or Ptype.is_mpath_space(ptype):
        name = Ptype.space_ptype_to_name(ptype)
        info['type'] = name
        if ptype == PTYPE['mpath'][name]['ready']:
            info['multipath'] = True
        if info.get('uuid') in space_map:
            info[name + '_for'] = space_map[info['uuid']]
    elif Ptype.is_plain_space(ptype):
        name = Ptype.space_ptype_to_name(ptype)
        holders = is_held(dev)
        info['type'] = name
        info['dmcrypt']['type'] = 'plain'
        info['dmcrypt']['holders'] = holders
        if info.get('uuid') in space_map:
            info[name + '_for'] = space_map[info['uuid']]
    elif Ptype.is_luks_space(ptype):
        name = Ptype.space_ptype_to_name(ptype)
        holders = is_held(dev)
        info['type'] = name
        info['dmcrypt']['type'] = 'LUKS'
        info['dmcrypt']['holders'] = holders
        if info.get('uuid') in space_map:
            info[name + '_for'] = space_map[info['uuid']]
    else:
        path = is_mounted(dev)
        fs_type = get_dev_fs(dev)
        if is_swap(dev):
            info['type'] = 'swap'
        else:
            info['type'] = 'other'
        if fs_type:
            info['fs_type'] = fs_type
        if path:
            info['mount'] = path

    return info


def list_devices():
    partmap = list_all_partitions()

    uuid_map = {}
    space_map = {}
    for base, parts in sorted(partmap.items()):
        for p in parts:
            dev = get_dev_path(p)
            part_uuid = get_partition_uuid(dev)
            if part_uuid:
                uuid_map[part_uuid] = dev
            ptype = get_partition_type(dev)
            LOG.debug("main_list: " + dev +
                      " ptype = " + str(ptype) +
                      " uuid = " + str(part_uuid))
            if ptype in Ptype.get_ready_by_name('osd'):
                if Ptype.is_dmcrypt(ptype, 'osd'):
                    holders = is_held(dev)
                    if len(holders) != 1:
                        continue
                    dev_to_mount = get_dev_path(holders[0])
                else:
                    dev_to_mount = dev

                fs_type = get_dev_fs(dev_to_mount)
                if fs_type is not None:
                    mount_options = get_mount_options(cluster='ceph',
                                                      fs_type=fs_type)
                    try:
                        tpath = mount(dev=dev_to_mount,
                                      fstype=fs_type, options=mount_options)
                        try:
                            for name in Space.NAMES:
                                space_uuid = get_oneliner(tpath,
                                                          name + '_uuid')
                                if space_uuid:
                                    space_map[space_uuid.lower()] = dev
                        finally:
                            unmount(tpath)
                    except MountError:
                        pass

    LOG.debug("main_list: " + str(partmap) + ", uuid_map = " +
              str(uuid_map) + ", space_map = " + str(space_map))

    devices = []
    for base, parts in sorted(partmap.items()):
        if parts:
            disk = {'path': get_dev_path(base)}
            partitions = []
            for p in sorted(parts):
                partitions.append(list_dev(get_dev_path(p),
                                           uuid_map,
                                           space_map))
            disk['partitions'] = partitions
            devices.append(disk)
        else:
            device = list_dev(get_dev_path(base), uuid_map, space_map)
            device['path'] = get_dev_path(base)
            devices.append(device)
    LOG.debug("list_devices: " + str(devices))
    return devices


def main_list(args):
    with activate_lock:
        main_list_protected(args)


def main_list_protected(args):
    devices = list_devices()
    if args.path:
        paths = []
        for path in args.path:
            if os.path.exists(path):
                paths.append(os.path.realpath(path))
            else:
                paths.append(path)
        selected_devices = []
        for device in devices:
            for path in paths:
                if re.search(path + '$', device['path']):
                    selected_devices.append(device)
    else:
        selected_devices = devices
    if args.format == 'json':
        print(json.dumps(selected_devices))
    else:
        output = list_format_plain(selected_devices)
        if output:
            print(output)


###########################
#
# Mark devices that we want to suppress activates on with a
# file like
#
#  /var/lib/ceph/tmp/suppress-activate.sdb
#
# where the last bit is the sanitized device name (/dev/X without the
# /dev/ prefix) and the is_suppress() check matches a prefix.  That
# means suppressing sdb will stop activate on sdb1, sdb2, etc.
#

def is_suppressed(path):
    disk = os.path.realpath(path)
    try:
        if (not disk.startswith('/dev/') or
                not stat.S_ISBLK(os.lstat(disk).st_mode)):
            return False
        base = get_dev_name(disk)
        while len(base):
            if os.path.exists(SUPPRESS_PREFIX + base):  # noqa
                return True
            base = base[:-1]
    except:
        return False


def set_suppress(path):
    disk = os.path.realpath(path)
    if not os.path.exists(disk):
        raise Error('does not exist', path)
    if not stat.S_ISBLK(os.lstat(path).st_mode):
        raise Error('not a block device', path)
    base = get_dev_name(disk)

    with open(SUPPRESS_PREFIX + base, 'w') as f:  # noqa
        pass
    LOG.info('set suppress flag on %s', base)


def unset_suppress(path):
    disk = os.path.realpath(path)
    if not os.path.exists(disk):
        raise Error('does not exist', path)
    if not stat.S_ISBLK(os.lstat(path).st_mode):
        raise Error('not a block device', path)
    assert disk.startswith('/dev/')
    base = get_dev_name(disk)

    fn = SUPPRESS_PREFIX + base  # noqa
    if not os.path.exists(fn):
        raise Error('not marked as suppressed', path)

    try:
        os.unlink(fn)
        LOG.info('unset suppress flag on %s', base)
    except OSError as e:
        raise Error('failed to unsuppress', e)


def main_suppress(args):
    set_suppress(args.path)


def main_unsuppress(args):
    unset_suppress(args.path)


def main_zap(args):
    for dev in args.dev:
        zap(dev)


def main_trigger(args):
    LOG.debug("main_trigger: " + str(args))
    if is_systemd() and not args.sync:
        # http://www.freedesktop.org/software/systemd/man/systemd-escape.html
        escaped_dev = args.dev[1:].replace('-', '\\x2d')
        service = 'ceph-disk@{dev}.service'.format(dev=escaped_dev)
        LOG.info('systemd detected, triggering %s' % service)
        command(
            [
                'systemctl',
                '--no-block',
                'restart',
                service,
            ]
        )
        return
    if is_upstart() and not args.sync:
        LOG.info('upstart detected, triggering ceph-disk task')
        command(
            [
                'initctl',
                'emit',
                'ceph-disk',
                'dev={dev}'.format(dev=args.dev),
                'pid={pid}'.format(pid=os.getpid()),
            ]
        )
        return

    if get_ceph_user() == 'ceph':
        command_check_call(['chown', 'ceph:ceph', args.dev])
    parttype = get_partition_type(args.dev)
    partid = get_partition_uuid(args.dev)

    LOG.info('trigger {dev} parttype {parttype} uuid {partid}'.format(
        dev=args.dev,
        parttype=parttype,
        partid=partid,
    ))

    ceph_disk = ['ceph-disk']
    if args.verbose:
        ceph_disk.append('--verbose')

    if parttype in (PTYPE['regular']['osd']['ready'],
                    PTYPE['mpath']['osd']['ready']):
        out, err, ret = command(
            ceph_disk +
            [
                'activate',
                args.dev,
            ]
        )

    elif parttype in (PTYPE['plain']['osd']['ready'],
                      PTYPE['luks']['osd']['ready']):
        out, err, ret = command(
            ceph_disk +
            [
                'activate',
                '--dmcrypt',
                args.dev,
            ]
        )

    elif parttype in (PTYPE['regular']['journal']['ready'],
                      PTYPE['mpath']['journal']['ready']):
        out, err, ret = command(
            ceph_disk +
            [
                'activate-journal',
                args.dev,
            ]
        )

    elif parttype in (PTYPE['plain']['journal']['ready'],
                      PTYPE['luks']['journal']['ready']):
        out, err, ret = command(
            ceph_disk +
            [
                'activate-journal',
                '--dmcrypt',
                args.dev,
            ]
        )

    elif parttype in (PTYPE['regular']['block']['ready'],
                      PTYPE['mpath']['block']['ready']):
        out, err, ret = command(
            ceph_disk +
            [
                'activate-block',
                args.dev,
            ]
        )

    elif parttype in (PTYPE['plain']['block']['ready'],
                      PTYPE['luks']['block']['ready']):
        out, err, ret = command(
            ceph_disk +
            [
                'activate-block',
                '--dmcrypt',
                args.dev,
            ]
        )

    elif parttype in (PTYPE['regular']['lockbox']['ready'],
                      PTYPE['mpath']['lockbox']['ready']):
        out, err, ret = command(
            ceph_disk +
            [
                'activate-lockbox',
                args.dev,
            ]
        )

    else:
        raise Error('unrecognized partition type %s' % parttype)

    if ret != 0:
        LOG.info(out)
        LOG.error(err)
        raise Error('return code ' + str(ret))
    else:
        LOG.debug(out)
        LOG.debug(err)


def main_fix(args):
    # A hash table containing 'path': ('uid', 'gid', blocking, recursive)
    fix_table = [
        ('/usr/bin/ceph-mon', 'root', 'root', True, False),
        ('/usr/bin/ceph-mds', 'root', 'root', True, False),
        ('/usr/bin/ceph-osd', 'root', 'root', True, False),
        ('/usr/bin/radosgw', 'root', 'root', True, False),
        ('/etc/ceph', 'root', 'root', True, True),
        ('/var/run/ceph', 'ceph', 'ceph', True, True),
        ('/var/log/ceph', 'ceph', 'ceph', True, True),
        ('/var/log/radosgw', 'ceph', 'ceph', True, True),
        ('/var/lib/ceph', 'ceph', 'ceph', True, False),
    ]

    # Relabel/chown all files under /var/lib/ceph/ recursively (except for osd)
    for directory in glob.glob('/var/lib/ceph/*'):
        if directory == '/var/lib/ceph/osd':
            fix_table.append((directory, 'ceph', 'ceph', True, False))
        else:
            fix_table.append((directory, 'ceph', 'ceph', True, True))

    # Relabel/chown the osds recursively and in parallel
    for directory in glob.glob('/var/lib/ceph/osd/*'):
        fix_table.append((directory, 'ceph', 'ceph', False, True))

    LOG.debug("fix_table: " + str(fix_table))

    # The lists of background processes
    all_processes = []
    permissions_processes = []
    selinux_processes = []

    # Preliminary checks
    if args.selinux or args.all:
        out, err, ret = command(['selinuxenabled'])
        if ret:
            LOG.error('SELinux is not enabled, please enable it, first.')
            raise Error('no SELinux')

    for daemon in ['ceph-mon', 'ceph-osd', 'ceph-mds', 'radosgw', 'ceph-mgr']:
        out, err, ret = command(['pgrep', daemon])
        if ret == 0:
            LOG.error(daemon + ' is running, please stop it, first')
            raise Error(daemon + ' running')

    # Relabel the basic system data without the ceph files
    if args.system or args.all:
        c = ['restorecon', '-R', '/']
        for directory, _, _, _, _ in fix_table:
            # Skip /var/lib/ceph subdirectories
            if directory.startswith('/var/lib/ceph/'):
                continue
            c.append('-e')
            c.append(directory)

        out, err, ret = command(c)

        if ret:
            LOG.error("Failed to restore labels of the underlying system")
            LOG.error(err)
            raise Error("basic restore failed")

    # Use find to relabel + chown ~simultaenously
    if args.all:
        for directory, uid, gid, blocking, recursive in fix_table:
            # Skip directories/files that are not installed
            if not os.access(directory, os.F_OK):
                continue

            c = [
                'find',
                directory,
                '-exec',
                'chown',
                ':'.join((uid, gid)),
                '{}',
                '+',
                '-exec',
                'restorecon',
                '{}',
                '+',
            ]

            # Just pass -maxdepth 0 for non-recursive calls
            if not recursive:
                c += ['-maxdepth', '0']

            if blocking:
                out, err, ret = command(c)

                if ret:
                    LOG.error("Failed to fix " + directory)
                    LOG.error(err)
                    raise Error(directory + " fix failed")
            else:
                all_processes.append(command_init(c))

    LOG.debug("all_processes: " + str(all_processes))
    for process in all_processes:
        out, err, ret = command_wait(process)
        if ret:
            LOG.error("A background find process failed")
            LOG.error(err)
            raise Error("background failed")

    # Fix permissions
    if args.permissions:
        for directory, uid, gid, blocking, recursive in fix_table:
            # Skip directories/files that are not installed
            if not os.access(directory, os.F_OK):
                continue

            if recursive:
                c = [
                    'chown',
                    '-R',
                    ':'.join((uid, gid)),
                    directory
                ]
            else:
                c = [
                    'chown',
                    ':'.join((uid, gid)),
                    directory
                ]

            if blocking:
                out, err, ret = command(c)

                if ret:
                    LOG.error("Failed to chown " + directory)
                    LOG.error(err)
                    raise Error(directory + " chown failed")
            else:
                permissions_processes.append(command_init(c))

    LOG.debug("permissions_processes: " + str(permissions_processes))
    for process in permissions_processes:
        out, err, ret = command_wait(process)
        if ret:
            LOG.error("A background permissions process failed")
            LOG.error(err)
            raise Error("background failed")

    # Fix SELinux labels
    if args.selinux:
        for directory, uid, gid, blocking, recursive in fix_table:
            # Skip directories/files that are not installed
            if not os.access(directory, os.F_OK):
                continue

            if recursive:
                c = [
                    'restorecon',
                    '-R',
                    directory
                ]
            else:
                c = [
                    'restorecon',
                    directory
                ]

            if blocking:
                out, err, ret = command(c)

                if ret:
                    LOG.error("Failed to restore labels for " + directory)
                    LOG.error(err)
                    raise Error(directory + " relabel failed")
            else:
                selinux_processes.append(command_init(c))

    LOG.debug("selinux_processes: " + str(selinux_processes))
    for process in selinux_processes:
        out, err, ret = command_wait(process)
        if ret:
            LOG.error("A background selinux process failed")
            LOG.error(err)
            raise Error("background failed")

    LOG.info(
        "The ceph files has been fixed, please reboot "
        "the system for the changes to take effect."
    )


def setup_statedir(dir):
    # XXX The following use of globals makes linting
    # really hard. Global state in Python is iffy and
    # should be avoided.
    global STATEDIR
    STATEDIR = dir

    if not os.path.exists(STATEDIR):
        os.mkdir(STATEDIR)
    if not os.path.exists(STATEDIR + "/tmp"):
        os.mkdir(STATEDIR + "/tmp")

    global prepare_lock
    prepare_lock = FileLock(STATEDIR + '/tmp/ceph-disk.prepare.lock')

    global activate_lock
    activate_lock = FileLock(STATEDIR + '/tmp/ceph-disk.activate.lock')

    global SUPPRESS_PREFIX
    SUPPRESS_PREFIX = STATEDIR + '/tmp/suppress-activate.'


def setup_sysconfdir(dir):
    global SYSCONFDIR
    SYSCONFDIR = dir


def parse_args(argv):
    parser = argparse.ArgumentParser(
        'ceph-disk',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
    )
    parser.add_argument(
        '--log-stdout',
        action='store_true', default=None,
        help='log to stdout',
    )
    parser.add_argument(
        '--prepend-to-path',
        metavar='PATH',
        default='/usr/bin',
        help=('prepend PATH to $PATH for backward compatibility '
              '(default /usr/bin)'),
    )
    parser.add_argument(
        '--statedir',
        metavar='PATH',
        default='/var/lib/ceph',
        help=('directory in which ceph state is preserved '
              '(default /var/lib/ceph)'),
    )
    parser.add_argument(
        '--sysconfdir',
        metavar='PATH',
        default='/etc/ceph',
        help=('directory in which ceph configuration files are found '
              '(default /etc/ceph)'),
    )
    parser.add_argument(
        '--setuser',
        metavar='USER',
        default=None,
        help='use the given user for subprocesses, rather than ceph or root'
    )
    parser.add_argument(
        '--setgroup',
        metavar='GROUP',
        default=None,
        help='use the given group for subprocesses, rather than ceph or root'
    )
    parser.set_defaults(
        # we want to hold on to this, for later
        prog=parser.prog,
    )

    subparsers = parser.add_subparsers(
        title='subcommands',
        description='valid subcommands',
        help='sub-command help',
    )

    Prepare.set_subparser(subparsers)
    make_activate_parser(subparsers)
    make_activate_lockbox_parser(subparsers)
    make_activate_block_parser(subparsers)
    make_activate_journal_parser(subparsers)
    make_activate_all_parser(subparsers)
    make_list_parser(subparsers)
    make_suppress_parser(subparsers)
    make_deactivate_parser(subparsers)
    make_destroy_parser(subparsers)
    make_zap_parser(subparsers)
    make_trigger_parser(subparsers)
    make_fix_parser(subparsers)

    args = parser.parse_args(argv)
    return args


def make_fix_parser(subparsers):
    fix_parser = subparsers.add_parser(
        'fix',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        """)),
        help='fix SELinux labels and/or file permissions')

    fix_parser.add_argument(
        '--system',
        action='store_true',
        default=False,
        help='fix SELinux labels for the non-ceph system data'
    )
    fix_parser.add_argument(
        '--selinux',
        action='store_true',
        default=False,
        help='fix SELinux labels for ceph data'
    )
    fix_parser.add_argument(
        '--permissions',
        action='store_true',
        default=False,
        help='fix file permissions for ceph data'
    )
    fix_parser.add_argument(
        '--all',
        action='store_true',
        default=False,
        help='perform all the fix-related operations'
    )
    fix_parser.set_defaults(
        func=main_fix,
    )
    return fix_parser


def make_trigger_parser(subparsers):
    trigger_parser = subparsers.add_parser(
        'trigger',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        The partition given in argument is activated. The type of the
        partition (data, lockbox, journal etc.) is detected by its
        type. If the init system is upstart or systemd, the activation is
        delegated to it and runs asynchronously, which
        helps reduce the execution time of udev actions.
        """)),
        help='activate any device (called by udev)')
    trigger_parser.add_argument(
        'dev',
        help=('device'),
    )
    trigger_parser.add_argument(
        '--cluster',
        metavar='NAME',
        default='ceph',
        help='cluster name to assign this disk to',
    )
    trigger_parser.add_argument(
        '--dmcrypt',
        action='store_true', default=None,
        help='map devices with dm-crypt',
    )
    trigger_parser.add_argument(
        '--dmcrypt-key-dir',
        metavar='KEYDIR',
        default='/etc/ceph/dmcrypt-keys',
        help='directory where dm-crypt keys are stored',
    )
    trigger_parser.add_argument(
        '--sync',
        action='store_true', default=None,
        help='do operation synchronously; do not trigger systemd',
    )
    trigger_parser.set_defaults(
        func=main_trigger,
    )
    return trigger_parser


def make_activate_parser(subparsers):
    activate_parser = subparsers.add_parser(
        'activate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Activate the OSD found at PATH (can be a directory
        or a device partition, possibly encrypted). When
        activated for the first time, a unique OSD id is obtained
        from the cluster. If PATH is a directory, a symbolic
        link is added in {statedir}/osd/ceph-$id. If PATH is
        a partition, it is mounted on {statedir}/osd/ceph-$id.
        Finally, the OSD daemon is run.

        If the OSD depends on auxiliary partitions (journal, block, ...)
        they need to be available otherwise activation will fail. It
        may happen if a journal is encrypted and cryptsetup was not
        run yet.
        """.format(statedir=STATEDIR))),
        help='Activate a Ceph OSD')
    activate_parser.add_argument(
        '--mount',
        action='store_true', default=None,
        help='mount a block device [deprecated, ignored]',
    )
    activate_parser.add_argument(
        '--activate-key',
        metavar='PATH',
        help='bootstrap-osd keyring path template (%(default)s)',
        dest='activate_key_template',
    )
    activate_parser.add_argument(
        '--mark-init',
        metavar='INITSYSTEM',
        help='init system to manage this dir',
        default='auto',
        choices=INIT_SYSTEMS,
    )
    activate_parser.add_argument(
        '--no-start-daemon',
        action='store_true', default=None,
        help='do not start the daemon',
    )
    activate_parser.add_argument(
        'path',
        metavar='PATH',
        help='path to block device or directory',
    )
    activate_parser.add_argument(
        '--dmcrypt',
        action='store_true', default=None,
        help='map DATA and/or JOURNAL devices with dm-crypt',
    )
    activate_parser.add_argument(
        '--dmcrypt-key-dir',
        metavar='KEYDIR',
        default='/etc/ceph/dmcrypt-keys',
        help='directory where dm-crypt keys are stored',
    )
    activate_parser.add_argument(
        '--reactivate',
        action='store_true', default=False,
        help='activate the deactived OSD',
    )
    activate_parser.set_defaults(
        activate_key_template='{statedir}/bootstrap-osd/{cluster}.keyring',
        func=main_activate,
    )
    return activate_parser


def make_activate_lockbox_parser(subparsers):
    parser = subparsers.add_parser(
        'activate-lockbox',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Mount the partition found at PATH on {statedir}/osd-lockbox/$uuid
        where $uuid uniquely identifies the OSD that needs this lockbox
        to retrieve keys from the monitor and unlock its partitions.

        If the OSD has one or more auxiliary devices (journal, block, ...)
        symbolic links are created at {statedir}/osd-lockbox/$other_uuid
        and point to {statedir}/osd-lockbox/$uuid. This will, for instance,
        allow a journal encrypted in a partition identified by $other_uuid to
        fetch the keys it needs from the monitor.

        Finally the OSD is activated, as it would be with ceph-disk activate.
        """.format(statedir=STATEDIR))),
        help='Activate a Ceph lockbox')
    parser.add_argument(
        '--activate-key',
        help='bootstrap-osd keyring path template (%(default)s)',
        dest='activate_key_template',
    )
    parser.add_argument(
        '--dmcrypt-key-dir',
        metavar='KEYDIR',
        default='/etc/ceph/dmcrypt-keys',
        help='directory where dm-crypt keys are stored',
    )
    parser.add_argument(
        'path',
        metavar='PATH',
        help='path to block device',
    )
    parser.set_defaults(
        activate_key_template='{statedir}/bootstrap-osd/{cluster}.keyring',
        func=main_activate_lockbox,
    )
    return parser


def make_activate_block_parser(subparsers):
    return make_activate_space_parser('block', subparsers)


def make_activate_journal_parser(subparsers):
    return make_activate_space_parser('journal', subparsers)


def make_activate_space_parser(name, subparsers):
    activate_space_parser = subparsers.add_parser(
        'activate-%s' % name,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Activating a {name} partition is only meaningfull
        if it is encrypted and it will map it using
        cryptsetup.

        Finally the corresponding OSD is activated,
        as it would be with ceph-disk activate.
        """.format(name=name))),
        help='Activate an OSD via its %s device' % name)
    activate_space_parser.add_argument(
        'dev',
        metavar='DEV',
        help='path to %s block device' % name,
    )
    activate_space_parser.add_argument(
        '--activate-key',
        metavar='PATH',
        help='bootstrap-osd keyring path template (%(default)s)',
        dest='activate_key_template',
    )
    activate_space_parser.add_argument(
        '--mark-init',
        metavar='INITSYSTEM',
        help='init system to manage this dir',
        default='auto',
        choices=INIT_SYSTEMS,
    )
    activate_space_parser.add_argument(
        '--dmcrypt',
        action='store_true', default=None,
        help=('map data and/or auxiliariy (journal, etc.) '
              'devices with dm-crypt'),
    )
    activate_space_parser.add_argument(
        '--dmcrypt-key-dir',
        metavar='KEYDIR',
        default='/etc/ceph/dmcrypt-keys',
        help='directory where dm-crypt keys are stored',
    )
    activate_space_parser.add_argument(
        '--reactivate',
        action='store_true', default=False,
        help='activate the deactived OSD',
    )
    activate_space_parser.set_defaults(
        activate_key_template='{statedir}/bootstrap-osd/{cluster}.keyring',
        func=lambda args: main_activate_space(name, args),
    )
    return activate_space_parser


def make_activate_all_parser(subparsers):
    activate_all_parser = subparsers.add_parser(
        'activate-all',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Activate all OSD partitions found in /dev/disk/by-parttypeuuid.
        The partitions containing auxiliary devices (journal, block, ...)
        are not activated.
        """)),
        help='Activate all tagged OSD partitions')
    activate_all_parser.add_argument(
        '--activate-key',
        metavar='PATH',
        help='bootstrap-osd keyring path template (%(default)s)',
        dest='activate_key_template',
    )
    activate_all_parser.add_argument(
        '--mark-init',
        metavar='INITSYSTEM',
        help='init system to manage this dir',
        default='auto',
        choices=INIT_SYSTEMS,
    )
    activate_all_parser.set_defaults(
        activate_key_template='{statedir}/bootstrap-osd/{cluster}.keyring',
        func=main_activate_all,
    )
    return activate_all_parser


def make_list_parser(subparsers):
    list_parser = subparsers.add_parser(
        'list',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Display all partitions on the system and their
        associated Ceph information, if any.
        """)),
        help='List disks, partitions, and Ceph OSDs')
    list_parser.add_argument(
        '--format',
        help='output format',
        default='plain',
        choices=['json', 'plain'],
    )
    list_parser.add_argument(
        'path',
        metavar='PATH',
        nargs='*',
        help='path to block devices, relative to /sys/block',
    )
    list_parser.set_defaults(
        func=main_list,
    )
    return list_parser


def make_suppress_parser(subparsers):
    suppress_parser = subparsers.add_parser(
        'suppress-activate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Add a prefix to the list of suppressed device names
        so that they are ignored by all activate* subcommands.
        """)),
        help='Suppress activate on a device (prefix)')
    suppress_parser.add_argument(
        'path',
        metavar='PATH',
        help='path to block device or directory',
    )
    suppress_parser.set_defaults(
        func=main_suppress,
    )

    unsuppress_parser = subparsers.add_parser(
        'unsuppress-activate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Remove a prefix from the list of suppressed device names
        so that they are no longer ignored by all
        activate* subcommands.
        """)),
        help='Stop suppressing activate on a device (prefix)')
    unsuppress_parser.add_argument(
        'path',
        metavar='PATH',
        help='path to block device or directory',
    )
    unsuppress_parser.set_defaults(
        func=main_unsuppress,
    )
    return suppress_parser


def make_deactivate_parser(subparsers):
    deactivate_parser = subparsers.add_parser(
        'deactivate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Deactivate the OSD located at PATH. It stops the OSD daemon
        and optionally marks it out (with --mark-out). The content of
        the OSD is left untouched.

        By default, the, ready, active, INIT-specific files are
        removed (so that it is not automatically re-activated by the
        udev rules or ceph-disk trigger) and the file deactive is
        created to remember the OSD is deactivated.

        If the --once option is given, the ready, active, INIT-specific
        files are not removed and the OSD will reactivate whenever
        ceph-disk trigger is run on one of the devices (journal, data,
        block, lockbox, ...).

        If the OSD is dmcrypt, remove the data dmcrypt map. When
        deactivate finishes, the OSD is down.
        """)),
        help='Deactivate a Ceph OSD')
    deactivate_parser.add_argument(
        '--cluster',
        metavar='NAME',
        default='ceph',
        help='cluster name to assign this disk to',
    )
    deactivate_parser.add_argument(
        'path',
        metavar='PATH',
        nargs='?',
        help='path to block device or directory',
    )
    deactivate_parser.add_argument(
        '--deactivate-by-id',
        metavar='<id>',
        help='ID of OSD to deactive'
    )
    deactivate_parser.add_argument(
        '--mark-out',
        action='store_true', default=False,
        help='option to mark the osd out',
    )
    deactivate_parser.add_argument(
        '--once',
        action='store_true', default=False,
        help='does not need --reactivate to activate again',
    )
    deactivate_parser.set_defaults(
        func=main_deactivate,
    )


def make_destroy_parser(subparsers):
    destroy_parser = subparsers.add_parser(
        'destroy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Destroy the OSD located at PATH.
        It removes the OSD from the cluster, the crushmap and
        deallocates the OSD id. An OSD must be down before it
        can be destroyed.
        """)),
        help='Destroy a Ceph OSD')
    destroy_parser.add_argument(
        '--cluster',
        metavar='NAME',
        default='ceph',
        help='cluster name to assign this disk to',
    )
    destroy_parser.add_argument(
        'path',
        metavar='PATH',
        nargs='?',
        help='path to block device or directory',
    )
    destroy_parser.add_argument(
        '--destroy-by-id',
        metavar='<id>',
        help='ID of OSD to destroy'
    )
    destroy_parser.add_argument(
        '--dmcrypt-key-dir',
        metavar='KEYDIR',
        default='/etc/ceph/dmcrypt-keys',
        help=('directory where dm-crypt keys are stored '
              '(If you don\'t know how it work, '
              'dont use it. we have default value)'),
    )
    destroy_parser.add_argument(
        '--zap',
        action='store_true', default=False,
        help='option to erase data and partition',
    )
    destroy_parser.set_defaults(
        func=main_destroy,
    )


def make_zap_parser(subparsers):
    zap_parser = subparsers.add_parser(
        'zap',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.fill(textwrap.dedent("""\
        Zap/erase/destroy a device's partition table and contents. It
        actually uses sgdisk and it's option --zap-all to
        destroy both GPT and MBR data structures so that the disk
        becomes suitable for repartitioning.
        """)),
        help='Zap/erase/destroy a device\'s partition table (and contents)')
    zap_parser.add_argument(
        'dev',
        metavar='DEV',
        nargs='+',
        help='path to block device',
    )
    zap_parser.set_defaults(
        func=main_zap,
    )
    return zap_parser


def main(argv):
    args = parse_args(argv)

    setup_logging(args.verbose, args.log_stdout)

    if args.prepend_to_path != '':
        path = os.environ.get('PATH', os.defpath)
        os.environ['PATH'] = args.prepend_to_path + ":" + path

    if args.func.__name__ != 'main_trigger':
        # trigger may run when statedir is unavailable and does not use it
        setup_statedir(args.statedir)
    setup_sysconfdir(args.sysconfdir)

    global CEPH_PREF_USER
    CEPH_PREF_USER = args.setuser
    global CEPH_PREF_GROUP
    CEPH_PREF_GROUP = args.setgroup

    if args.verbose:
        args.func(args)
    else:
        main_catch(args.func, args)


def setup_logging(verbose, log_stdout):
    loglevel = logging.WARNING
    if verbose:
        loglevel = logging.DEBUG

    if log_stdout:
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(loglevel)
        formatter = logging.Formatter('%(funcName)s: %(message)s')
        ch.setFormatter(formatter)
        LOG.addHandler(ch)
        LOG.setLevel(loglevel)
    else:
        logging.basicConfig(
            level=loglevel,
            format='%(funcName)s: %(message)s',
        )


def main_catch(func, args):

    try:
        func(args)

    except Error as e:
        raise SystemExit(
            '{prog}: {msg}'.format(
                prog=args.prog,
                msg=e,
            )
        )

    except CephDiskException as error:
        exc_name = error.__class__.__name__
        raise SystemExit(
            '{prog} {exc_name}: {msg}'.format(
                prog=args.prog,
                exc_name=exc_name,
                msg=error,
            )
        )


def run():
    main(sys.argv[1:])


if __name__ == '__main__':
    main(sys.argv[1:])
    warned_about = {}
