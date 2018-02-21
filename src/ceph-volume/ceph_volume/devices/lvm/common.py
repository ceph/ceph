from ceph_volume.util import arg_validators
from ceph_volume import process
from ceph_volume import terminal
import argparse


def rollback_osd(args, osd_id=None):
    """
    When the process of creating or preparing fails, the OSD needs to be either
    purged (ID fully removed) or destroyed (ID persists). This is important
    because otherwise it would leave the ID around, which can cause confusion
    if relying on the automatic (OSD.N + 1) behavior.

    When the OSD id is specified in the command line explicitly (with
    ``--osd-id``) , the the ID is then preserved with a soft removal (``ceph
    osd destroy``), otherwise it is fully removed with ``purge``.
    """
    if not osd_id:
        # it means that it wasn't generated, so there is nothing to rollback here
        return

    # once here, this is an error condition that needs to be rolled back
    terminal.error('Was unable to complete a new OSD, will rollback changes')
    osd_name = 'osd.%s'
    if args.osd_id is None:
        terminal.error('OSD will be fully purged from the cluster, because the ID was generated')
        # the ID wasn't passed in explicitly, so make sure it is fully removed
        process.run([
            'ceph', 'osd', 'purge',
            osd_name % osd_id,
            '--yes-i-really-mean-it'])
    else:
        terminal.error('OSD will be destroyed, keeping the ID because it was provided with --osd-id')
        # the ID was passed explicitly, so allow to re-use by using `destroy`
        process.run([
            'ceph', 'osd', 'destroy',
            osd_name % args.osd_id,
            '--yes-i-really-mean-it'])


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
        type=arg_validators.LVPath(),
        help='OSD data path. A physical device or logical volume',
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
        '--block.wal',
        dest='block_wal',
        help='Path to bluestore block.wal logical volume or device',
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
        '--crush-device-class',
        dest='crush_device_class',
        help='Crush device class to assign this OSD to',
    )

    parser.add_argument(
        '--dmcrypt',
        action='store_true',
        help='Enable device encryption via dm-crypt',
    )

    # Do not parse args, so that consumers can do something before the args get
    # parsed triggering argparse behavior
    return parser


create_parser = common_parser  # noqa
prepare_parser = common_parser  # noqa
