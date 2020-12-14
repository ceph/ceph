import docopt
import sys

import logging

import teuthology
import teuthology.suite
from teuthology.config import config

doc = """
usage: teuthology-wait --help
       teuthology-wait [-v] --run <name>

Wait until run is finished. Returns exit code 0 on success, otherwise 1.

Miscellaneous arguments:
  -h, --help                  Show this help message and exit
  -v, --verbose               Be more verbose

Standard arguments:
  -r, --run <name>          Run name to watch.
"""


def main(argv=sys.argv[1:]):
    args = docopt.docopt(doc, argv=argv)
    if args.get('--verbose'):
        teuthology.log.setLevel(logging.DEBUG)
    name = args.get('--run')
    return teuthology.suite.wait(name, config.max_job_time, None)

