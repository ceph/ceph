import argparse

import teuthology.lock


def main():
    teuthology.lock.updatekeys(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(description="""
Update any hostkeys that have changed. You can list specific machines
to run on, or use -a to check all of them automatically.
""")
    parser.add_argument(
        '-t', '--targets',
        default=None,
        help='input yaml containing targets to check',
    )
    parser.add_argument(
        'machines',
        metavar='MACHINES',
        default=[],
        nargs='*',
        help='hosts to check for updated keys',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
    )
    parser.add_argument(
        '-a', '--all',
        action='store_true',
        default=False,
        help='update hostkeys of all machines in the db',
    )

    return parser.parse_args()
