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
paddles_url = 'http://pulpito.example.com:8080/nodes/'

machine_type = 'magna'
lab_domain = 'example.com'
user = 'ubuntu'
# We are populating 'magna003' -> 'magna122'
machine_index_range = range(3, 123)

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
    shortnames = [get_shortname(machine_type, i) for i in range(3, 123)]
    fqdns = ['.'.join((name, lab_domain)) for name in shortnames]
    for fqdn in fqdns:
        log.info("Creating %s", fqdn)
        try:
            info = get_info(user, fqdn)
        except Exception:
            info = dict(
                name=fqdn,
                up=False,
            )
        info.update(dict(
            locked=True,
            locked_by='initial@setup',
            machine_type=machine_type,
            description="Initial node creation",
        ))
        update_inventory(info)

if __name__ == '__main__':
    main()
