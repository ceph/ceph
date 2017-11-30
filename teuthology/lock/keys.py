import logging

from teuthology import misc

from . import ops, query

log = logging.getLogger(__name__)


def do_update_keys(machines, all_=False, _raise=True):
    reference = query.list_locks(keyed_by_name=True)
    if all_:
        machines = reference.keys()
    keys_dict = misc.ssh_keyscan(machines, _raise=_raise)
    return push_new_keys(keys_dict, reference), keys_dict


def push_new_keys(keys_dict, reference):
    ret = 0
    for hostname, pubkey in keys_dict.iteritems():
        log.info('Checking %s', hostname)
        if reference[hostname]['ssh_pub_key'] != pubkey:
            log.info('New key found. Updating...')
            if not ops.update_lock(hostname, ssh_pub_key=pubkey):
                log.error('failed to update %s!', hostname)
                ret = 1
    return ret
