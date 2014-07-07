import argparse
import teuthology.ls


def main():
    teuthology.ls.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(description='List teuthology job results')
    parser.add_argument(
        '--archive-dir',
        metavar='DIR',
        default='.',
        help='path under which to archive results',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=False,
        help='show reasons tests failed',
    )
    return parser.parse_args()
