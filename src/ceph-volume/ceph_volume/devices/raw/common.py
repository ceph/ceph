import argparse
from ceph_volume.util import arg_validators

def create_parser(prog, description):
    """
    Both prepare and create share the same parser, those are defined here to
    avoid duplication
    """
    parser = argparse.ArgumentParser(
        prog=prog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description,
    )
    parser.add_argument(
        '--data',
        required=True,
    type=arg_validators.ValidDevice(as_string=True),
        help='a raw device to use for the OSD',
    )
    parser.add_argument(
        '--bluestore',
        action='store_true',
        help='Use BlueStore backend')
    parser.add_argument(
        '--crush-device-class',
        dest='crush_device_class',
        help='Crush device class to assign this OSD to',
    )
    parser.add_argument(
        '--no-tmpfs',
        action='store_true',
        help='Do not use a tmpfs mount for OSD data dir'
    )
    parser.add_argument(
        '--block.db',
        dest='block_db',
        help='Path to bluestore block.db block device'
    )
    parser.add_argument(
        '--block.wal',
        dest='block_wal',
        help='Path to bluestore block.wal block device'
    )
    parser.add_argument(
        '--dmcrypt',
        action='store_true',
        help='Enable device encryption via dm-crypt',
    )
    return parser
