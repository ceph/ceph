import argparse
from textwrap import dedent

import teuthology.report


def main():
    teuthology.report.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit test results to a web service")
    parser.add_argument('-a', '--archive', required=True,
                        help="The base archive directory")
    parser.add_argument('-r', '--run', nargs='*',
                        help="A run (or list of runs) to submit")
    parser.add_argument('--all-runs', action='store_true',
                        help="Submit all runs in the archive")
    parser.add_argument('-R', '--refresh', action='store_true', default=False,
                        help=dedent("""Re-push any runs already stored on the
                                    server. Note that this may be slow."""))
    parser.add_argument('-s', '--server',
                        help=dedent(""""The server to post results to, e.g.
                                    http://localhost:8080/ . May also be
                                    specified in ~/.teuthology.yaml as
                                    'results_server'"""))
    parser.add_argument('-n', '--no-save', dest='save',
                        action='store_false', default=True,
                        help=dedent("""By default, when submitting all runs, we
                        remember the last successful submission in a file
                        called 'last_successful_run'. Pass this flag to disable
                        that behavior."""))
    return parser.parse_args()
