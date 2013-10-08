import argparse
from teuthology.suite import ls


def main():
    args = parse_args()
    ls(args.archive_dir, args.verbose)


def parse_args():
    parser = argparse.ArgumentParser(description='List teuthology job results')
    parser.add_argument(
        '--archive-dir',
        metavar='DIR',
        help='path under which to archive results',
        required=True,
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=False,
        help='show reasons tests failed',
    )
    return parser.parse_args()
