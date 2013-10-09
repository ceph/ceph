import argparse
import sys

import teuthology.lock


def main():
    teuthology.lock.updatekeys(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(description="""
Update any hostkeys that have changed. You can list specific machines
to run on, or use -a to check all of them automatically.
""")
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-t', '--targets',
        default=None,
        help='input yaml containing targets to check',
    )
    group.add_argument(
        '-a', '--all',
        action='store_true',
        default=False,
        help='update hostkeys of all machines in the db',
    )
    group.add_argument(
        'machines',
        metavar='MACHINES',
        default=[],
        nargs='*',
        help='hosts to check for updated keys',
    )

    args = parser.parse_args()

    if not (args.all or args.targets or args.machines):
        parser.print_usage()
        print "{name}: error: You must specify machines to update".format(
            name='teuthology-updatekeys')
        sys.exit(2)

    return args
