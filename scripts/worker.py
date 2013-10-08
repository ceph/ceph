import argparse

import teuthology.queue


def main():
    teuthology.queue.worker(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(description="""
Grab jobs from a beanstalk queue and run the teuthology tests they
describe. One job is run at a time.
""")
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
    )
    parser.add_argument(
        '--archive-dir',
        metavar='DIR',
        help='path under which to archive results',
        required=True,
    )
    parser.add_argument(
        '-l', '--log-dir',
        help='path in which to store logs',
        required=True,
    )
    parser.add_argument(
        '-t', '--tube',
        help='which beanstalk tube to read jobs from',
        required=True,
    )

    return parser.parse_args()
