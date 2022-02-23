from ceph_volume.util import arg_validators, disk
from ceph_volume import process, conf
from ceph_volume import terminal
import argparse

def valid_osd_id(val):
    return str(int(val))

def rollback_osd(args, osd_id=None):
    """
    When the process of creating or preparing fails, the OSD needs to be
    destroyed so that the ID cane be reused.  This is prevents leaving the ID
    around as "used" on the monitor, which can cause confusion if expecting
    sequential OSD IDs.

    The usage of `destroy-new` allows this to be done without requiring the
    admin keyring (otherwise needed for destroy and purge commands)
    """
    if not osd_id:
        # it means that it wasn't generated, so there is nothing to rollback here
        return

    # once here, this is an error condition that needs to be rolled back
    terminal.error('Was unable to complete a new OSD, will rollback changes')
    osd_name = 'osd.%s'
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    cmd = [
        'ceph',
        '--cluster', conf.cluster,
        '--name', 'client.bootstrap-osd',
        '--keyring', bootstrap_keyring,
        'osd', 'purge-new', osd_name % osd_id,
        '--yes-i-really-mean-it',
    ]

    process.run(cmd)


common_args = {
    '--data': {
        'help': 'OSD data path. A physical device or logical volume',
        'required': True,
        'type': arg_validators.ValidDevice(as_string=True),
        #'default':,
        #'type':,
    },
    '--data-size': {
        'help': 'Size of data LV in case a device was passed in --data',
        'default': '0',
        'type': disk.Size.parse
    },
    '--data-slots': {
        'help': ('Intended number of slots on data device. The new OSD gets one'
              'of those slots or 1/nth of the available capacity'),
        'type': int,
        'default': 1,
    },
    '--osd-id': {
        'help': 'Reuse an existing OSD id',
        'default': None,
        'type': valid_osd_id,
    },
    '--osd-fsid': {
        'help': 'Reuse an existing OSD fsid',
        'default': None,
    },
    '--cluster-fsid': {
        'help': 'Specify the cluster fsid, useful when no ceph.conf is available',
        'default': None,
    },
    '--crush-device-class': {
        'dest': 'crush_device_class',
        'help': 'Crush device class to assign this OSD to',
        'default': None,
    },
    '--dmcrypt': {
        'action': 'store_true',
        'help': 'Enable device encryption via dm-crypt',
    },
    '--no-systemd': {
        'dest': 'no_systemd',
        'action': 'store_true',
        'help': 'Skip creating and enabling systemd units and starting OSD services when activating',
    },
}

bluestore_args = {
    '--bluestore': {
        'action': 'store_true',
        'help': 'Use the bluestore objectstore',
    },
    '--block.db': {
        'dest': 'block_db',
        'help': 'Path to bluestore block.db logical volume or device',
        'type': arg_validators.ValidDevice(as_string=True),
    },
    '--block.db-size': {
        'dest': 'block_db_size',
        'help': 'Size of block.db LV in case device was passed in --block.db',
        'default': '0',
        'type': disk.Size.parse
    },
    '--block.db-slots': {
        'dest': 'block_db_slots',
        'help': ('Intended number of slots on db device. The new OSD gets one'
              'of those slots or 1/nth of the available capacity'),
        'type': int,
        'default': 1,
    },
    '--block.wal': {
        'dest': 'block_wal',
        'help': 'Path to bluestore block.wal logical volume or device',
        'type': arg_validators.ValidDevice(as_string=True),
    },
    '--block.wal-size': {
        'dest': 'block_wal_size',
        'help': 'Size of block.wal LV in case device was passed in --block.wal',
        'default': '0',
        'type': disk.Size.parse
    },
    '--block.wal-slots': {
        'dest': 'block_wal_slots',
        'help': ('Intended number of slots on wal device. The new OSD gets one'
              'of those slots or 1/nth of the available capacity'),
        'type': int,
        'default': 1,
    },
}

filestore_args = {
    '--filestore': {
        'action': 'store_true',
        'help': 'Use the filestore objectstore',
    },
    '--journal': {
        'help': 'A logical volume (vg_name/lv_name), or path to a device',
        'type': arg_validators.ValidDevice(as_string=True),
    },
    '--journal-size': {
        'help': 'Size of journal LV in case a raw block device was passed in --journal',
        'default': '0',
        'type': disk.Size.parse
    },
    '--journal-slots': {
        'help': ('Intended number of slots on journal device. The new OSD gets one'
              'of those slots or 1/nth of the available capacity'),
        'type': int,
        'default': 1,
    },
}

def get_default_args():
    defaults = {}
    def format_name(name):
        return name.strip('-').replace('-', '_').replace('.', '_')
    for argset in (common_args, filestore_args, bluestore_args):
        defaults.update({format_name(name): val.get('default', None) for name, val in argset.items()})
    return defaults


def common_parser(prog, description):
    """
    Both prepare and create share the same parser, those are defined here to
    avoid duplication
    """
    parser = argparse.ArgumentParser(
        prog=prog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description,
    )

    filestore_group = parser.add_argument_group('filestore')
    bluestore_group = parser.add_argument_group('bluestore')

    for name, kwargs in common_args.items():
        parser.add_argument(name, **kwargs)

    for name, kwargs in bluestore_args.items():
        bluestore_group.add_argument(name, **kwargs)

    for name, kwargs in filestore_args.items():
        filestore_group.add_argument(name, **kwargs)

    # Do not parse args, so that consumers can do something before the args get
    # parsed triggering argparse behavior
    return parser


create_parser = common_parser  # noqa
prepare_parser = common_parser  # noqa
