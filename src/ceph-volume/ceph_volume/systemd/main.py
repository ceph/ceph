"""
This file is used only by systemd units that are passing their instance suffix
as arguments to this script so that it can parse the suffix into arguments that
``ceph-volume <sub command>`` can consume
"""
import os
import sys
import time
import logging
from ceph_volume import log, process
from ceph_volume.exceptions import SuffixParsingError


def parse_subcommand(string):
    subcommand = string.split('-', 1)[0]
    if not subcommand:
        raise SuffixParsingError('subcommand', string)
    return subcommand


def parse_extra_data(string):
    # get the subcommand to split on that
    sub_command = parse_subcommand(string)

    # the split will leave data with a dash, so remove that
    data = string.split(sub_command)[-1]
    if not data:
        raise SuffixParsingError('data', string)
    return data.lstrip('-')


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

        ['/path/to/ceph-volume-systemd', '<type>-<extra metadata>']

    For example::

        [
            '/usr/bin/ceph-volume-systemd',
            'lvm-0-8715BEB4-15C5-49DE-BA6F-401086EC7B41'
        ]

    The first part of the argument is the only interesting bit, which contains
    the metadata needed to proxy the call to ``ceph-volume`` itself.

    Reusing the example, the proxy call to ``ceph-volume`` would look like::

        ceph-volume lvm trigger 0-8715BEB4-15C5-49DE-BA6F-401086EC7B41

    That means that ``lvm`` is used as the subcommand and it is **expected**
    that a ``trigger`` sub-commmand will be present to make sense of the extra
    piece of the string.

    """
    log.setup(name='ceph-volume-systemd.log', log_path='/var/log/ceph/ceph-volume-systemd.log')
    logger = logging.getLogger('systemd')

    args = args if args is not None else sys.argv
    try:
        suffix = args[-1]
    except IndexError:
        raise RuntimeError('no arguments supplied')
    sub_command = parse_subcommand(suffix)
    extra_data = parse_extra_data(suffix)
    logger.info('raw systemd input received: %s', suffix)
    logger.info('parsed sub-command: %s, extra data: %s', sub_command, extra_data)
    command = ['ceph-volume', sub_command, 'trigger', extra_data]

    tries = os.environ.get('CEPH_VOLUME_SYSTEMD_TRIES', 30)
    interval = os.environ.get('CEPH_VOLUME_SYSTEMD_INTERVAL', 5)
    while tries > 0:
        try:
            # don't log any output to the terminal, just rely on stderr/stdout
            # going to logging
            process.run(command, terminal_logging=False)
            logger.info('successfully trggered activation for: %s', extra_data)
            break
        except RuntimeError as error:
            logger.warning(error)
            logger.warning('failed activating OSD, retries left: %s', tries)
            tries -= 1
            time.sleep(interval)
