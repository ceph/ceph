"""
These utilities for prepare provide all the pieces needed to prepare a device
but also a compounded ("single call") helper to do them in order. Some plugins
may want to change some part of the process, while others might want to consume
the single-call helper
"""
import os
import logging
from ceph_volume import process, conf
from ceph_volume.util import system, constants

logger = logging.getLogger(__name__)


def create_key():
    stdout, stderr, returncode = process.call(['ceph-authtool', '--gen-print-key'])
    if returncode != 0:
        raise RuntimeError('Unable to generate a new auth key')
    return ' '.join(stdout).strip()


def write_keyring(osd_id, secret):
    # FIXME this only works for cephx, but there will be other types of secrets
    # later
    osd_keyring = '/var/lib/ceph/osd/%s-%s/keyring' % (conf.cluster, osd_id)
    process.run(
        [
            'ceph-authtool', osd_keyring,
            '--create-keyring',
            '--name', 'osd.%s' % str(osd_id),
            '--add-key', secret
        ])
    system.chown(osd_keyring)
    # TODO: do the restorecon dance on the osd_keyring path


def create_id(fsid, json_secrets):
    """
    :param fsid: The osd fsid to create, always required
    :param json_secrets: a json-ready object with whatever secrets are wanted
                         to be passed to the monitor
    """
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    stdout, stderr, returncode = process.call(
        [
            'ceph',
            '--cluster', conf.cluster,
            '--name', 'client.bootstrap-osd',
            '--keyring', bootstrap_keyring,
            '-i', '-',
            'osd', 'new', fsid
        ],
        stdin=json_secrets
    )
    if returncode != 0:
        raise RuntimeError('Unable to create a new OSD id')
    return ' '.join(stdout).strip()


def mount_tmpfs(path):
    process.run([
        'mount',
        '-t',
        'tmpfs', 'tmpfs',
        path
    ])


def create_osd_path(osd_id, tmpfs=False):
    path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    system.mkdir_p('/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id))
    if tmpfs:
        mount_tmpfs(path)


def format_device(device):
    # only supports xfs
    command = ['mkfs', '-t', 'xfs']

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
    command = ['mount', '-t', 'xfs', '-o']
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


def _link_device(device, device_type, osd_id):
    """
    Allow linking any device type in an OSD directory. ``device`` must the be
    source, with an absolute path and ``device_type`` will be the destination
    name, like 'journal', or 'block'
    """
    device_path = '/var/lib/ceph/osd/%s-%s/%s' % (
        conf.cluster,
        osd_id,
        device_type
    )
    command = ['ln', '-s', device, device_path]
    system.chown(device)

    process.run(command)


def link_journal(journal_device, osd_id):
    _link_device(journal_device, 'journal', osd_id)


def link_block(block_device, osd_id):
    _link_device(block_device, 'block', osd_id)


def link_wal(wal_device, osd_id):
    _link_device(wal_device, 'block.wal', osd_id)


def link_db(db_device, osd_id):
    _link_device(db_device, 'block.db', osd_id)


def get_monmap(osd_id):
    """
    Before creating the OSD files, a monmap needs to be retrieved so that it
    can be used to tell the monitor(s) about the new OSD. A call will look like::

        ceph --cluster ceph --name client.bootstrap-osd \
             --keyring /var/lib/ceph/bootstrap-osd/ceph.keyring \
             mon getmap -o /var/lib/ceph/osd/ceph-0/activate.monmap
    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    monmap_destination = os.path.join(path, 'activate.monmap')

    process.run([
        'ceph',
        '--cluster', conf.cluster,
        '--name', 'client.bootstrap-osd',
        '--keyring', bootstrap_keyring,
        'mon', 'getmap', '-o', monmap_destination
    ])


def osd_mkfs_bluestore(osd_id, fsid, keyring=None, wal=False, db=False):
    """
    Create the files for the OSD to function. A normal call will look like:

          ceph-osd --cluster ceph --mkfs --mkkey -i 0 \
                   --monmap /var/lib/ceph/osd/ceph-0/activate.monmap \
                   --osd-data /var/lib/ceph/osd/ceph-0 \
                   --osd-uuid 8d208665-89ae-4733-8888-5d3bfbeeec6c \
                   --keyring /var/lib/ceph/osd/ceph-0/keyring \
                   --setuser ceph --setgroup ceph

    In some cases it is required to use the keyring, when it is passed in as
    a keywork argument it is used as part of the ceph-osd command
    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    monmap = os.path.join(path, 'activate.monmap')

    system.chown(path)

    base_command = [
        'ceph-osd',
        '--cluster', conf.cluster,
        # undocumented flag, sets the `type` file to contain 'bluestore'
        '--osd-objectstore', 'bluestore',
        '--mkfs',
        '-i', osd_id,
        '--monmap', monmap,
    ]

    supplementary_command = [
        '--osd-data', path,
        '--osd-uuid', fsid,
        '--setuser', 'ceph',
        '--setgroup', 'ceph'
    ]

    if keyring is not None:
        base_command.extend(['--key', keyring])

    if wal:
        base_command.extend(
            ['--bluestore-block-wal-path', wal]
        )
        system.chown(wal)

    if db:
        base_command.extend(
            ['--bluestore-block-db-path', db]
        )
        system.chown(db)

    command = base_command + supplementary_command

    process.run(command, obfuscate='--key')


def osd_mkfs_filestore(osd_id, fsid):
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

    system.chown(journal)
    system.chown(path)

    process.run([
        'ceph-osd',
        '--cluster', conf.cluster,
        # undocumented flag, sets the `type` file to contain 'filestore'
        '--osd-objectstore', 'filestore',
        '--mkfs',
        '-i', osd_id,
        '--monmap', monmap,
        '--osd-data', path,
        '--osd-journal', journal,
        '--osd-uuid', fsid,
        '--setuser', 'ceph',
        '--setgroup', 'ceph'
    ])
