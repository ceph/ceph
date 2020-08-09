import logging
import json
import os

import requests

import teuthology.orchestra.remote
import teuthology.parallel
import teuthology.provision
from teuthology import misc
from teuthology.config import config
from teuthology.contextutil import safe_while
from teuthology.task import console_log
from teuthology.misc import canonicalize_hostname

from teuthology.lock import util, query

log = logging.getLogger(__name__)


def update_nodes(nodes, reset_os=False):
    for node in nodes:
        remote = teuthology.orchestra.remote.Remote(
            canonicalize_hostname(node))
        if reset_os:
            log.info("Updating [%s]: reset os type and version on server", node)
            inventory_info = dict()
            inventory_info['os_type'] = ''
            inventory_info['os_version'] = ''
            inventory_info['name'] = remote.hostname
        else:
            log.info("Updating [%s]: set os type and version on server", node)
            inventory_info = remote.inventory_info
        update_inventory(inventory_info)


def lock_many_openstack(ctx, num, machine_type, user=None, description=None,
                        arch=None):
    os_type = teuthology.provision.get_distro(ctx)
    os_version = teuthology.provision.get_distro_version(ctx)
    if hasattr(ctx, 'config'):
        resources_hint = ctx.config.get('openstack')
    else:
        resources_hint = None
    machines =  teuthology.provision.openstack.ProvisionOpenStack().create(
        num, os_type, os_version, arch, resources_hint)
    result = {}
    for machine in machines:
        lock_one(machine, user, description)
        result[machine] = None # we do not collect ssh host keys yet
    return result


def lock_many(ctx, num, machine_type, user=None, description=None,
              os_type=None, os_version=None, arch=None, reimage=True):
    if user is None:
        user = misc.get_user()

    if not util.vps_version_or_type_valid(
            ctx.machine_type,
            os_type,
            os_version
    ):
        log.error('Invalid os-type or version detected -- lock failed')
        return

    # In the for loop below we can safely query for all bare-metal machine_type
    # values at once. So, if we're being asked for 'plana,mira,burnupi', do it
    # all in one shot. If we are passed 'plana,mira,burnupi,vps', do one query
    # for 'plana,mira,burnupi' and one for 'vps'
    machine_types_list = misc.get_multi_machine_types(machine_type)
    if machine_types_list == ['vps']:
        machine_types = machine_types_list
    elif machine_types_list == ['openstack']:
        return lock_many_openstack(ctx, num, machine_type,
                                   user=user,
                                   description=description,
                                   arch=arch)
    elif 'vps' in machine_types_list:
        machine_types_non_vps = list(machine_types_list)
        machine_types_non_vps.remove('vps')
        machine_types_non_vps = '|'.join(machine_types_non_vps)
        machine_types = [machine_types_non_vps, 'vps']
    else:
        machine_types_str = '|'.join(machine_types_list)
        machine_types = [machine_types_str, ]

    for machine_type in machine_types:
        uri = os.path.join(config.lock_server, 'nodes', 'lock_many', '')
        data = dict(
            locked_by=user,
            count=num,
            machine_type=machine_type,
            description=description,
        )
        # Only query for os_type/os_version if non-vps and non-libcloud, since
        # in that case we just create them.
        vm_types = ['vps'] + teuthology.provision.cloud.get_types()
        reimage_types = teuthology.provision.get_reimage_types()
        if machine_type not in vm_types + reimage_types:
            if os_type:
                data['os_type'] = os_type
            if os_version:
                data['os_version'] = os_version
        if arch:
            data['arch'] = arch
        log.debug("lock_many request: %s", repr(data))
        response = requests.post(
            uri,
            data=json.dumps(data),
            headers={'content-type': 'application/json'},
        )
        if response.ok:
            machines = {misc.canonicalize_hostname(machine['name']):
                        machine['ssh_pub_key'] for machine in response.json()}
            log.debug('locked {machines}'.format(
                machines=', '.join(machines.keys())))
            if machine_type in vm_types:
                ok_machs = {}
                update_nodes(machines, True)
                for machine in machines:
                    if teuthology.provision.create_if_vm(ctx, machine):
                        ok_machs[machine] = machines[machine]
                    else:
                        log.error('Unable to create virtual machine: %s',
                                  machine)
                        unlock_one(ctx, machine, user)
                    ok_machs = do_update_keys(list(ok_machs.keys()))[1]
                update_nodes(ok_machs)
                return ok_machs
            elif reimage and machine_type in reimage_types:
                return reimage_many(ctx, machines, machine_type)
            return machines
        elif response.status_code == 503:
            log.error('Insufficient nodes available to lock %d %s nodes.',
                      num, machine_type)
            log.error(response.text)
        else:
            log.error('Could not lock %d %s nodes, reason: unknown.',
                      num, machine_type)
    return []


def lock_one(name, user=None, description=None):
    name = misc.canonicalize_hostname(name, user=None)
    if user is None:
        user = misc.get_user()
    request = dict(name=name, locked=True, locked_by=user,
                   description=description)
    uri = os.path.join(config.lock_server, 'nodes', name, 'lock', '')
    response = requests.put(uri, json.dumps(request))
    success = response.ok
    if success:
        log.debug('locked %s as %s', name, user)
    else:
        try:
            reason = response.json().get('message')
        except ValueError:
            reason = str(response.status_code)
        log.error('failed to lock {node}. reason: {reason}'.format(
            node=name, reason=reason))
    return response


def unlock_many(names, user):
    fixed_names = [misc.canonicalize_hostname(name, user=None) for name in
                   names]
    names = fixed_names
    uri = os.path.join(config.lock_server, 'nodes', 'unlock_many', '')
    data = dict(
        locked_by=user,
        names=names,
    )
    response = requests.post(
        uri,
        data=json.dumps(data),
        headers={'content-type': 'application/json'},
    )
    if response.ok:
        log.debug("Unlocked: %s", ', '.join(names))
    else:
        log.error("Failed to unlock: %s", ', '.join(names))
    return response.ok


def unlock_one(ctx, name, user, description=None):
    name = misc.canonicalize_hostname(name, user=None)
    if not teuthology.provision.destroy_if_vm(ctx, name, user, description):
        log.error('destroy failed for %s', name)
        return False
    request = dict(name=name, locked=False, locked_by=user,
                   description=description)
    uri = os.path.join(config.lock_server, 'nodes', name, 'lock', '')
    with safe_while(
            sleep=1, increment=0.5, action="unlock %s" % name) as proceed:
        while proceed():
            try:
                response = requests.put(uri, json.dumps(request))
                break
            # Work around https://github.com/kennethreitz/requests/issues/2364
            except requests.ConnectionError as e:
                log.warn("Saw %s while unlocking; retrying...", str(e))
    success = response.ok
    if success:
        log.info('unlocked %s', name)
    else:
        try:
            reason = response.json().get('message')
        except ValueError:
            reason = str(response.status_code)
        log.error('failed to unlock {node}. reason: {reason}'.format(
            node=name, reason=reason))
    return success


def update_lock(name, description=None, status=None, ssh_pub_key=None):
    name = misc.canonicalize_hostname(name, user=None)
    updated = {}
    if description is not None:
        updated['description'] = description
    if status is not None:
        updated['up'] = (status == 'up')
    if ssh_pub_key is not None:
        updated['ssh_pub_key'] = ssh_pub_key

    if updated:
        uri = os.path.join(config.lock_server, 'nodes', name, '')
        response = requests.put(
            uri,
            json.dumps(updated))
        return response.ok
    return True


def update_inventory(node_dict):
    """
    Like update_lock(), but takes a dict and doesn't try to do anything smart
    by itself
    """
    name = node_dict.get('name')
    if not name:
        raise ValueError("must specify name")
    if not config.lock_server:
        return
    uri = os.path.join(config.lock_server, 'nodes', name, '')
    log.info("Updating %s on lock server", name)
    response = requests.put(
        uri,
        json.dumps(node_dict),
        headers={'content-type': 'application/json'},
        )
    if response.status_code == 404:
        log.info("Creating new node %s on lock server", name)
        uri = os.path.join(config.lock_server, 'nodes', '')
        response = requests.post(
            uri,
            json.dumps(node_dict),
            headers={'content-type': 'application/json'},
        )
    if not response.ok:
        log.error("Node update/creation failed for %s: %s",
                  name, response.text)
    return response.ok


def do_update_keys(machines, all_=False, _raise=True):
    reference = query.list_locks(keyed_by_name=True)
    if all_:
        machines = reference.keys()
    keys_dict = misc.ssh_keyscan(machines, _raise=_raise)
    return push_new_keys(keys_dict, reference), keys_dict


def push_new_keys(keys_dict, reference):
    ret = 0
    for hostname, pubkey in keys_dict.items():
        log.info('Checking %s', hostname)
        if reference[hostname]['ssh_pub_key'] != pubkey:
            log.info('New key found. Updating...')
            if not update_lock(hostname, ssh_pub_key=pubkey):
                log.error('failed to update %s!', hostname)
                ret = 1
    return ret


def reimage(ctx, machines, machine_type):
    reimaged = dict()
    with teuthology.parallel.parallel() as p:
        for machine in machines:
            log.info("Start node '%s' reimaging", machine)
            update_nodes([machine], True)
            p.spawn(teuthology.provision.reimage, ctx,
                    machine, machine_type)
            reimaged[machine] = machines[machine]
            log.info("Node '%s' reimaging is complete", machine)
    return reimaged


def reimage_many(ctx, machines, machine_type):
    # Setup log file, reimage machines and update their keys
    reimaged = dict()
    console_log_conf = dict(
        logfile_name='{shortname}_reimage.log',
        remotes=[teuthology.orchestra.remote.Remote(machine)
                 for machine in machines],
    )
    with console_log.task(ctx, console_log_conf):
        reimaged = reimage(ctx, machines, machine_type)
    reimaged = do_update_keys(list(reimaged.keys()))[1]
    update_nodes(reimaged)
    return reimaged
