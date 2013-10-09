import argparse

import teuthology.run


def main():
    teuthology.run.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(description='Run ceph integration tests')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
    )
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='+',
        type=teuthology.run.config_file,
        action=teuthology.run.MergeConfig,
        default={},
        help='config file to read',
    )
    parser.add_argument(
        '-a', '--archive',
        metavar='DIR',
        help='path to archive results in',
    )
    parser.add_argument(
        '--description',
        help='job description',
    )
    parser.add_argument(
        '--owner',
        help='job owner',
    )
    parser.add_argument(
        '--lock',
        action='store_true',
        default=False,
        help='lock machines for the duration of the run',
    )
    parser.add_argument(
        '--machine-type',
        default=None,
        help='Type of machine to lock/run tests on.',
    )
    parser.add_argument(
        '--os-type',
        default='ubuntu',
        help='Distro/OS of machine to run test on.',
    )
    parser.add_argument(
        '--block',
        action='store_true',
        default=False,
        help='block until locking machines succeeds (use with --lock)',
    )
    parser.add_argument(
        '--name',
        metavar='NAME',
        help='name for this teuthology run',
    )

    return parser.parse_args()
