"""
These utilities for prepare provide all the pieces needed to prepare a device
but also a compounded ("single call") helper to do them in order. Some plugins
may want to change some part of the process, while others might want to consume
the single-call helper
"""
import os
import logging
from ceph_volume import process, conf, terminal
from ceph_volume.util import system, constants

logger = logging.getLogger(__name__)


def create_id(fsid):
    stdout, stderr, returncode = process.call(
        [
            'ceph',
            '--cluster', conf.cluster,
            '--name', 'client.bootstrap-osd',
            '--keyring', '/var/lib/ceph/bootstrap-osd/ceph.keyring',
            'osd', 'create', fsid
        ],
        terminal_logging=True
    )
    if returncode != 0:
        for line in stdout:
            terminal.write(line)
        for line in stderr:
            terminal.write(line)
        raise RuntimeError()
    return ' '.join(stdout).strip()


def create_path(osd_id):
    system.mkdir_p('/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id))


def format_device(device):
    # only supports xfs
    command = ['sudo', 'mkfs', '-t', 'xfs']

    # get the mkfs options if any for xfs,
    # fallback to the default options defined in constants.mkfs
    flags = conf.ceph.get_list(
        'osd',
        'osd_mkfs_options_xfs',
        default=constants.mkfs.get('xfs'),
        split=' ',
    )

    # always force
    if '-f' not in flags:
        flags.insert(0, '-f')

    command.extend(flags)
    command.append(device)
    process.run(command)


def mount_osd(device, osd_id):
    destination = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    command = ['sudo', 'mount', '-t', 'xfs', '-o']
    flags = conf.ceph.get_list(
        'osd',
        'osd_mount_options_xfs',
        default=constants.mount.get('xfs'),
        split=' ',
    )
    command.append(flags)
    command.append(device)
    command.append(destination)
    process.run(command)


def link_journal(journal_device, osd_id):
    journal_path = '/var/lib/ceph/osd/%s-%s/journal' % (
        conf.cluster,
        osd_id
    )
    command = ['sudo', 'ln', '-s', journal_device, journal_path]
    process.run(command)


def get_monmap(osd_id):
    """
    Before creating the OSD files, a monmap needs to be retrieved so that it
    can be used to tell the monitor(s) about the new OSD. A call will look like::

        ceph --cluster ceph --name client.bootstrap-osd \
             --keyring /var/lib/ceph/bootstrap-osd/ceph.keyring \
             mon getmap -o /var/lib/ceph/osd/ceph-0/activate.monmap
    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/ceph.keyring'
    monmap_destination = os.path.join(path, 'activate.monmap')

    process.run([
        'sudo',
        'ceph',
        '--cluster', conf.cluster,
        '--name', 'client.bootstrap-osd',
        '--keyring', bootstrap_keyring,
        'mon', 'getmap', '-o', monmap_destination
    ])


def osd_mkfs(osd_id, fsid):
    """
    Create the files for the OSD to function. A normal call will look like:

          ceph-osd --cluster ceph --mkfs --mkkey -i 0 \
                   --monmap /var/lib/ceph/osd/ceph-0/activate.monmap \
                   --osd-data /var/lib/ceph/osd/ceph-0 \
                   --osd-journal /var/lib/ceph/osd/ceph-0/journal \
                   --osd-uuid 8d208665-89ae-4733-8888-5d3bfbeeec6c \
                   --keyring /var/lib/ceph/osd/ceph-0/keyring \
                   --setuser ceph --setgroup ceph

    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    monmap = os.path.join(path, 'activate.monmap')
    journal = os.path.join(path, 'journal')
    keyring = os.path.join(path, 'keyring')

    system.chown(journal)
    system.chown(path)

    process.run([
        'sudo',
        'ceph-osd',
        '--cluster', conf.cluster,
        '--mkfs',
        '--mkkey',
        '-i', osd_id,
        '--monmap', monmap,
        '--osd-data', path,
        '--osd-journal', journal,
        '--osd-uuid', fsid,
        '--keyring', keyring,
        '--setuser', 'ceph',
        '--setgroup', 'ceph'
    ])
