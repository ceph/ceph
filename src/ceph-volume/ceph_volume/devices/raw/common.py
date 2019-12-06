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
        '--osd-id',
        help='Reuse an existing OSD id',
    )
    parser.add_argument(
        '--osd-uuid',
        help='Reuse an existing OSD UUID',
    )
    parser.add_argument(
        '--cluster-fsid',
        help='Specify the cluster fsid, useful when no ceph.conf is available',
    )
    parser.add_argument(
        '--no-tmpfs',
        action='store_true',
        help='Do not use a tmpfs mount for OSD data dir')
    return parser
