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
        help='A logical group name, path to a logical volume, or path to a device',
    )
    required_args.add_argument(
        '--data',
        required=True,
        help='A logical group name or a path to a logical volume',
    )
    parser.add_argument(
        '--journal-size',
        default=5,
        metavar='GB',
        type=int,
        help='Size (in GB) A logical group name or a path to a logical volume',
    )
    parser.add_argument(
        '--bluestore',
        action='store_true', default=False,
        help='Use the bluestore objectstore (not currently supported)',
    )
    parser.add_argument(
        '--filestore',
        action='store_true', default=True,
        help='Use the filestore objectstore (currently the only supported object store)',
    )
    parser.add_argument(
        '--osd-id',
        help='Reuse an existing OSD id',
    )
    parser.add_argument(
        '--osd-fsid',
        help='Reuse an existing OSD fsid',
    )
    # Do not parse args, so that consumers can do something before the args get
    # parsed triggering argparse behavior
    return parser


create_parser = common_parser  # noqa
prepare_parser = common_parser  # noqa
