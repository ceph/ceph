from ceph_volume.util import arg_validators
from ceph_volume import process, conf
from ceph_volume import terminal
import argparse


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

    required_group = parser.add_argument_group('required arguments')
    filestore_group = parser.add_argument_group('filestore')
    bluestore_group = parser.add_argument_group('bluestore')

    required_group.add_argument(
        '--data',
        required=True,
        type=arg_validators.ValidDevice(as_string=True),
        help='OSD data path. A physical device or logical volume',
    )

    required_group.add_argument(
        '--data-size',
        help='Size of data LV in case a device was passed in --data',
        default=0,
    )

    required_group.add_argument(
        '--data-slots',
        help=('Intended number of slots on data device. The new OSD gets one'
              'of those slots or 1/nth of the available capacity'),
        type=int,
        default=1,
    )

    filestore_group.add_argument(
        '--filestore',
        action='store_true',
        help='Use the filestore objectstore',
    )

    filestore_group.add_argument(
        '--journal',
        help='(REQUIRED) A logical volume (vg_name/lv_name), or path to a device',
    )

    filestore_group.add_argument(
        '--journal-size',
        help='Size of journal LV in case a raw block device was passed in --journal',
        default=0,
    )

    bluestore_group.add_argument(
        '--bluestore',
        action='store_true',
        help='Use the bluestore objectstore',
    )

    bluestore_group.add_argument(
        '--block.db',
        dest='block_db',
        help='Path to bluestore block.db logical volume or device',
    )

    bluestore_group.add_argument(
        '--block.db-size',
        dest='block_db_size',
        help='Size of block.db LV in case device was passed in --block.db',
        default=0,
    )

    required_group.add_argument(
        '--block.db-slots',
        dest='block_db_slots',
        help=('Intended number of slots on db device. The new OSD gets one'
              'of those slots or 1/nth of the available capacity'),
        type=int,
        default=1,
    )

    bluestore_group.add_argument(
        '--block.wal',
        dest='block_wal',
        help='Path to bluestore block.wal logical volume or device',
    )

    bluestore_group.add_argument(
        '--block.wal-size',
        dest='block_wal_size',
        help='Size of block.wal LV in case device was passed in --block.wal',
        default=0,
    )

    required_group.add_argument(
        '--block.wal-slots',
        dest='block_wal_slots',
        help=('Intended number of slots on wal device. The new OSD gets one'
              'of those slots or 1/nth of the available capacity'),
        type=int,
        default=1,
    )

    parser.add_argument(
        '--osd-id',
        help='Reuse an existing OSD id',
    )

    parser.add_argument(
        '--osd-fsid',
        help='Reuse an existing OSD fsid',
    )

    parser.add_argument(
        '--cluster-fsid',
        help='Specify the cluster fsid, useful when no ceph.conf is available',
    )

    parser.add_argument(
        '--crush-device-class',
        dest='crush_device_class',
        help='Crush device class to assign this OSD to',
    )

    parser.add_argument(
        '--dmcrypt',
        action='store_true',
        help='Enable device encryption via dm-crypt',
    )

    parser.add_argument(
        '--no-systemd',
        dest='no_systemd',
        action='store_true',
        help='Skip creating and enabling systemd units and starting OSD services when activating',
    )

    # Do not parse args, so that consumers can do something before the args get
    # parsed triggering argparse behavior
    return parser


create_parser = common_parser  # noqa
prepare_parser = common_parser  # noqa
