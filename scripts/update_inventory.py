import docopt

import teuthology
import teuthology.lock
import teuthology.lock.ops
import teuthology.misc
import teuthology.orchestra.remote

import logging

doc = """
usage: teuthology-update-inventory -h
       teuthology-update-inventory [-v] [-m type] REMOTE [REMOTE ...]

Update the given nodes' inventory information on the lock server


  -h, --help                        show this help message and exit
  -v, --verbose                     be more verbose
  -m <type>, --machine-type <type>  optionally specify a machine type when 
                                    submitting nodes for the first time
  REMOTE                            hostnames of machines whose information to update

"""


def main():
    args = docopt.docopt(doc)
    if args['--verbose']:
        teuthology.log.setLevel(logging.DEBUG)

    machine_type = args.get('--machine-type')
    remotes = args.get('REMOTE')
    for rem_name in remotes:
        rem_name = teuthology.misc.canonicalize_hostname(rem_name)
        remote = teuthology.orchestra.remote.Remote(rem_name)
        remote.connect()
        inventory_info = remote.inventory_info
        if machine_type:
          inventory_info['machine_type'] = machine_type
        teuthology.lock.ops.update_inventory(inventory_info)
