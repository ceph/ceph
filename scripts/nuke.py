import argparse
from argparse import RawTextHelpFormatter
import textwrap

import teuthology.misc
import teuthology.nuke


def main():
    teuthology.nuke.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(
        description='Reset test machines',
        epilog=textwrap.dedent('''
        Examples:
        teuthology-nuke -t target.yaml --unlock --owner user@host
        teuthology-nuke -t target.yaml --pid 1234 --unlock --owner user@host \n
        '''),
        formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose'
    )
    parser.add_argument(
        '-t', '--targets',
        nargs='+',
        type=teuthology.misc.config_file,
        action=teuthology.misc.MergeConfig,
        default={},
        dest='config',
        help='yaml config containing machines to nuke',
    )
    parser.add_argument(
        '-a', '--archive',
        metavar='DIR',
        help='archive path for a job to kill and nuke',
    )
    parser.add_argument(
        '--owner',
        help='job owner',
    )
    parser.add_argument(
        '-p',
        '--pid',
        type=int,
        default=False,
        help='pid of the process to be killed',
    )
    parser.add_argument(
        '-r', '--reboot-all',
        action='store_true',
        default=False,
        help='reboot all machines',
    )
    parser.add_argument(
        '-s', '--synch-clocks',
        action='store_true',
        default=False,
        help='synchronize clocks on all machines',
    )
    parser.add_argument(
        '-u', '--unlock',
        action='store_true',
        default=False,
        help='Unlock each successfully nuked machine, and output targets that'
        'could not be nuked.'
    )
    parser.add_argument(
        '-n', '--name',
        metavar='NAME',
        help='Name of run to cleanup'
    )
    parser.add_argument(
        '-i', '--noipmi',
        action='store_true', default=False,
        help='Skip ipmi checking'
    )
    return parser.parse_args()
