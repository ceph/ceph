"""
usage: local_runner.py --help
       local_runner.py --version
       local_runner.py [options] [--] <config>...

Run ceph integration tests against a local, vstart-deployed Ceph cluster

positional arguments:
  <config> one or more config files to read

optional arguments:
  -h, --help                     show this help message and exit
  -v, --verbose                  be more verbose
  --version                      the current installed version of teuthology
  -a DIR, --archive DIR          path to archive results in
  --suite-path SUITE_PATH        Location of ceph-qa-suite on disk. If not specified,
                                 it will be fetched
"""

import docopt
import logging
import yaml

from teuthology.run import set_up_logging
from teuthology.run import setup_config
from teuthology.run import validate_tasks
from teuthology.run import fetch_tasks_if_needed

from teuthology.config import FakeNamespace

from teuthology.run_tasks import run_tasks

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

def main(args):
    verbose = args["--verbose"]
    archive = args["--archive"]
    # someone could provide a path to already fetched tasks
    suite_path = args["--suite-path"]

    set_up_logging(verbose, archive)

    # print the command being ran
    log.debug("Starting local_runner of Teuthology")

    config = setup_config(args["<config>"])
    config["tasks"] = validate_tasks(config)

    if suite_path is not None:
        config['suite_path'] = suite_path

    log.debug(
        '\n  '.join(['Config:', ] + yaml.safe_dump(
            config, default_flow_style=False).splitlines()))

    # fetches the tasks and returns a new suite_path if needed
    config["suite_path"] = fetch_tasks_if_needed(config)

    # create a FakeNamespace instance that mimics the old argparse way of doing
    # things we do this so we can pass it to run_tasks without porting those
    # tasks to the new way of doing things right now
    args["<config>"] = config
    fake_ctx = FakeNamespace(args)

    # store on global config if interactive-on-error, for contextutil.nested()
    # FIXME this should become more generic, and the keys should use
    # '_' uniformly
    if fake_ctx.config.get('interactive-on-error'):
        teuthology.config.config.ctx = fake_ctx

    try:
        run_tasks(tasks=config['tasks'], ctx=fake_ctx)
    finally:
        pass


if __name__ == "__main__":
    args = docopt.docopt(__doc__, version="0.0")
    main(args)
