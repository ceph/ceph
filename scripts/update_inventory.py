import docopt

import teuthology
import teuthology.lock
import teuthology.lock.ops
import teuthology.misc
import teuthology.orchestra.remote

import logging

doc = """
usage: teuthology-update-inventory -h
       teuthology-update-inventory [-v] REMOTE [REMOTE ...]

Update the given nodes' inventory information on the lock server


  -h, --help            show this help message and exit
  -v, --verbose         be more verbose
  REMOTE                hostnames of machines whose information to update

"""


def main():
    args = docopt.docopt(doc)
    if args['--verbose']:
        teuthology.log.setLevel(logging.DEBUG)

    remotes = args.get('REMOTE')
    for rem_name in remotes:
        rem_name = teuthology.misc.canonicalize_hostname(rem_name)
        remote = teuthology.orchestra.remote.Remote(rem_name)
        inventory_info = remote.inventory_info
        teuthology.lock.ops.update_inventory(inventory_info)
