import argparse

import teuthology.results


def main():
    teuthology.results.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(
        description='Email teuthology suite results')
    parser.add_argument(
        '--email',
        help='address to email test failures to',
    )
    parser.add_argument(
        '--timeout',
        help='how many seconds to wait for all tests to finish (default no ' +
        'wait)',
        type=int,
        default=0,
    )
    parser.add_argument(
        '--archive-dir',
        metavar='DIR',
        help='path under which results for the suite are stored',
        required=True,
    )
    parser.add_argument(
        '--name',
        help='name of the suite',
        required=True,
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=False,
        help='be more verbose',
    )
    return parser.parse_args()
