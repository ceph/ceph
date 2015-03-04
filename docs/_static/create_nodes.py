#!/usr/bin/env python
# A sample script that can be used while setting up a new teuthology lab
# This script will connect to the machines in your lab, and populate a
# paddles instance with their information.
#
# You WILL need to modify it.

import logging
import sys
from teuthology.orchestra.remote import Remote
from teuthology.lock import update_inventory
paddles_url = 'http://paddles.example.com/nodes/'

machine_type = 'typica'
lab_domain = 'example.com'
# Don't change the user. It won't work at this time.
user = 'ubuntu'
# We are populating 'typica003' -> 'typica192'
machine_index_range = range(3, 192)

log = logging.getLogger(sys.argv[0])
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
    logging.WARNING)


def get_shortname(machine_type, index):
    """
    Given a number, return a hostname. Example:
        get_shortname('magna', 3) = 'magna003'

    Modify to suit your needs.
    """
    return machine_type + str(index).rjust(3, '0')


def get_info(user, fqdn):
    remote = Remote('@'.join((user, fqdn)))
    return remote.inventory_info


def main():
    shortnames = [get_shortname(machine_type, i) for i in machine_index_range]
    fqdns = ['.'.join((name, lab_domain)) for name in shortnames]
    for fqdn in fqdns:
        log.info("Creating %s", fqdn)
        base_info = dict(
            name=fqdn,
            locked=True,
            locked_by='initial@setup',
            machine_type=machine_type,
            description="Initial node creation",
        )
        try:
            info = get_info(user, fqdn)
            base_info.update(info)
            base_info['up'] = True
        except Exception as exc:
            log.error("{fqdn} is down".format(fqdn=fqdn))
            base_info['up'] = False
            base_info['description'] = repr(exc)
        update_inventory(base_info)

if __name__ == '__main__':
    main()
