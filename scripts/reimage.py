import docopt
import sys

import teuthology.reimage

doc = """
usage: teuthology-reimage --help
       teuthology-reimage --os-type distro --os-version version [options] <nodes>...

Reimage nodes without locking using specified distro type and version.
The nodes must be locked by the current user, otherwise an error occurs.
Custom owner can be specified in order to provision someone else nodes.
Reimaging unlocked nodes cannot be provided.

Standard arguments:
  -h, --help                        Show this help message and exit
  -v, --verbose                     Be more verbose
  --os-type <os-type>               Distro type like: rhel, ubuntu, etc.
  --os-version <os-version>         Distro version like: 7.6, 16.04, etc.
  --owner user@host                 Owner of the locked machines
"""

def main(argv=sys.argv[1:]):
    args = docopt.docopt(doc, argv=argv)
    return teuthology.reimage.main(args)
