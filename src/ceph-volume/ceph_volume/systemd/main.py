"""
This file is used only by systemd units that are passing their instance suffix
as arguments to this script so that it can parse the suffix into arguments that
``ceph-volume <sub command>`` can consume
"""

import sys
import logging
from ceph_volume import log, process
from ceph_volume.exceptions import SuffixParsingError


def parse_subcommand(string):
    subcommand = string.rsplit('-', 1)[-1]
    if not subcommand:
        raise SuffixParsingError('subcommand', string)
    return subcommand


def parse_osd_id(string):
    osd_id = string.split('-', 1)[0]
    if not osd_id:
        raise SuffixParsingError('OSD id', string)
    if osd_id.isdigit():
        return osd_id
    raise SuffixParsingError('OSD id', string)


def parse_osd_uuid(string):
    osd_id = '%s-' % parse_osd_id(string)
    osd_subcommand = '-%s' % parse_subcommand(string)
    # remove the id first
    trimmed_suffix = string.split(osd_id)[-1]
    # now remove the sub command
    osd_uuid = trimmed_suffix.split(osd_subcommand)[0]
    if not osd_uuid:
        raise SuffixParsingError('OSD uuid', string)
    return osd_uuid


def main(args=None):
    """
    Main entry point for the ``ceph-volume-systemd`` executable. ``args`` are
    optional for easier testing of arguments.

    Expected input is similar to::

        ['/path/to/ceph-volume-systemd', '<osd id>-<osd uuid>-<device type>']

    For example::

        [
            '/usr/bin/ceph-volume-systemd',
            '0-8715BEB4-15C5-49DE-BA6F-401086EC7B41-lvm'
        ]

    The last part of the argument is the only interesting bit, which contains
    the metadata needed to proxy the call to ``ceph-volume`` itself.

    Reusing the example, the proxy call to ``ceph-volume`` would look like::

        ceph-volume lvm 0 8715BEB4-15C5-49DE-BA6F-401086EC7B41

    """
    log.setup(name='ceph-volume-systemd.log')
    logger = logging.getLogger('systemd')

    args = args or sys.argv
    suffix = args[-1]
    sub_command = parse_subcommand(suffix)
    osd_id = parse_osd_id(suffix)
    osd_uuid = parse_osd_uuid(suffix)
    logger.info('raw systemd input received: %s', suffix)
    logger.info('osd id: %s, osd uuid: %s, sub-command: %s', osd_id, osd_uuid, sub_command)
    command = ['ceph-volume', sub_command, 'activate', osd_id, osd_uuid]

    # don't log any output to the terminal, just rely on stderr/stdout going to logging
    process.run(command, terminal_logging=False)
