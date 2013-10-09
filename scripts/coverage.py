import argparse

import teuthology.coverage


def main():
    teuthology.coverage.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(description="""
Analyze the coverage of a suite of test runs, generating html output with lcov.
""")
    parser.add_argument(
        '-o', '--lcov-output',
        help='the directory in which to store results',
        required=True,
    )
    parser.add_argument(
        '--html-output',
        help='the directory in which to store html output',
    )
    parser.add_argument(
        '--cov-tools-dir',
        help='the location of coverage scripts (cov-init and cov-analyze)',
        default='../../coverage',
    )
    parser.add_argument(
        '--skip-init',
        help='skip initialization (useful if a run stopped partway through)',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '-v', '--verbose',
        help='be more verbose',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        'test_dir',
        help='the location of the test results',
    )
    return parser.parse_args()
