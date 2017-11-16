from ceph_volume.util import arg_validators
import argparse


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
    required_args = parser.add_argument_group('required arguments')
    parser.add_argument(
        '--journal',
        help='(filestore) A logical volume (vg_name/lv_name), or path to a device',
    )
    required_args.add_argument(
        '--data',
        required=True,
        type=arg_validators.LVPath(),
        help='OSD data path. A physical device or logical volume',
    )
    parser.add_argument(
        '--journal-size',
        default=5,
        metavar='GB',
        type=int,
        help='(filestore) Size (in GB) for the journal',
    )
    parser.add_argument(
        '--bluestore',
        action='store_true',
        help='Use the bluestore objectstore',
    )
    parser.add_argument(
        '--filestore',
        action='store_true',
        help='Use the filestore objectstore',
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
        '--block.db',
        dest='block_db',
        help='(bluestore) Path to bluestore block.db logical volume or device',
    )
    parser.add_argument(
        '--block.wal',
        dest='block_wal',
        help='(bluestore) Path to bluestore block.wal logical volume or device',
    )
    # Do not parse args, so that consumers can do something before the args get
    # parsed triggering argparse behavior
    return parser


create_parser = common_parser  # noqa
prepare_parser = common_parser  # noqa
