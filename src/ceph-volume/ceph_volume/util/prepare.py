"""
These utilities for prepare provide all the pieces needed to prepare a device
but also a compounded ("single call") helper to do them in order. Some plugins
may want to change some part of the process, while others might want to consume
the single-call helper
"""
import os
import logging
import json
from ceph_volume import process, conf
from ceph_volume.util import system, constants

logger = logging.getLogger(__name__)


def create_key():
    stdout, stderr, returncode = process.call(
        ['ceph-authtool', '--gen-print-key'],
        show_command=True)
    if returncode != 0:
        raise RuntimeError('Unable to generate a new auth key')
    return ' '.join(stdout).strip()


def write_keyring(osd_id, secret, keyring_name='keyring', name=None):
    """
    Create a keyring file with the ``ceph-authtool`` utility. Constructs the
    path over well-known conventions for the OSD, and allows any other custom
    ``name`` to be set.

    :param osd_id: The ID for the OSD to be used
    :param secret: The key to be added as (as a string)
    :param name: Defaults to 'osd.{ID}' but can be used to add other client
                 names, specifically for 'lockbox' type of keys
    :param keyring_name: Alternative keyring name, for supporting other
                         types of keys like for lockbox
    """
    osd_keyring = '/var/lib/ceph/osd/%s-%s/%s' % (conf.cluster, osd_id, keyring_name)
    name = name or 'osd.%s' % str(osd_id)
    process.run(
        [
            'ceph-authtool', osd_keyring,
            '--create-keyring',
            '--name', name,
            '--add-key', secret
        ])
    system.chown(osd_keyring)


def create_id(fsid, json_secrets, osd_id=None):
    """
    :param fsid: The osd fsid to create, always required
    :param json_secrets: a json-ready object with whatever secrets are wanted
                         to be passed to the monitor
    :param osd_id: Reuse an existing ID from an OSD that's been destroyed, if the
                   id does not exist in the cluster a new ID will be created
    """
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    cmd = [
        'ceph',
        '--cluster', conf.cluster,
        '--name', 'client.bootstrap-osd',
        '--keyring', bootstrap_keyring,
        '-i', '-',
        'osd', 'new', fsid
    ]
    if check_id(osd_id):
        cmd.append(osd_id)
    stdout, stderr, returncode = process.call(
        cmd,
        stdin=json_secrets,
        show_command=True
    )
    if returncode != 0:
        raise RuntimeError('Unable to create a new OSD id')
    return ' '.join(stdout).strip()


def check_id(osd_id):
    """
    Checks to see if an osd ID exists or not. Returns True
    if it does exist, False if it doesn't.

    :param osd_id: The osd ID to check
    """
    if osd_id is None:
        return False
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    stdout, stderr, returncode = process.call(
        [
            'ceph',
            '--cluster', conf.cluster,
            '--name', 'client.bootstrap-osd',
            '--keyring', bootstrap_keyring,
            'osd',
            'tree',
            '-f', 'json',
        ],
        show_command=True
    )
    if returncode != 0:
        raise RuntimeError('Unable check if OSD id exists: %s' % osd_id)

    output = json.loads(''.join(stdout).strip())
    osds = output['nodes']
    return any([str(osd['id']) == str(osd_id) for osd in osds])


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


def _normalize_mount_flags(flags):
    """
    Mount flag options have to be a single string, separated by a comma. If the
    flags are separated by spaces, or with commas and spaces in ceph.conf, the
    mount options will be passed incorrectly.

    This will help when parsing ceph.conf values return something like::

        ["rw,", "exec,"]

    Or::

        [" rw ,", "exec"]

    :param flags: A list of flags, or a single string of mount flags
    """
    if isinstance(flags, list):
        # ensure that spaces and commas are removed so that they can join
        # correctly
        return ','.join([f.strip().strip(',') for f in flags if f])

    # split them, clean them, and join them back again
    flags = flags.strip().split(' ')
    return ','.join(
        [f.strip().strip(',') for f in flags if f]
    )


def mount_osd(device, osd_id):
    destination = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    command = ['mount', '-t', 'xfs', '-o']
    flags = conf.ceph.get_list(
        'osd',
        'osd_mount_options_xfs',
        default=constants.mount.get('xfs'),
        split=' ',
    )
    command.append(_normalize_mount_flags(flags))
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
        base_command.extend(['--keyfile', '-'])

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

    process.call(command, stdin=keyring, show_command=True)


def osd_mkfs_filestore(osd_id, fsid, keyring):
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

    command = [
        'ceph-osd',
        '--cluster', conf.cluster,
        # undocumented flag, sets the `type` file to contain 'filestore'
        '--osd-objectstore', 'filestore',
        '--mkfs',
        '-i', osd_id,
        '--monmap', monmap,
        '--keyfile', '-', # goes through stdin
        '--osd-data', path,
        '--osd-journal', journal,
        '--osd-uuid', fsid,
        '--setuser', 'ceph',
        '--setgroup', 'ceph'
    ]
    process.call(command, stdin=keyring, terminal_verbose=True, show_command=True)
