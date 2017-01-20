import docopt
import sys

import teuthology.lock
import teuthology.lock.cli

doc = """
usage: teuthology-updatekeys -h
       teuthology-updatekeys [-v] -t <targets>
       teuthology-updatekeys [-v] <machine> ...
       teuthology-updatekeys [-v] -a

Update any hostkeys that have changed. You can list specific machines to run
on, or use -a to check all of them automatically.

positional arguments:
  MACHINES              hosts to check for updated keys

optional arguments:
  -h, --help            Show this help message and exit
  -v, --verbose         Be more verbose
  -t <targets>, --targets <targets>
                        Input yaml containing targets to check
  -a, --all             Update hostkeys of all machines in the db
"""


def main():
    args = docopt.docopt(doc)
    status = teuthology.lock.cli.updatekeys(args)
    sys.exit(status)
