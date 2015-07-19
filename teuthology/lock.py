import argparse
import json
import logging
import subprocess
import yaml
import re
import collections
import os
import requests
import urllib

import teuthology
from . import misc
from . import provision
from .config import config
from .lockstatus import get_status

log = logging.getLogger(__name__)


def is_vm(name):
    return get_status(name)['is_vm']


def get_distro_from_downburst():
    """
    Return a table of valid distros.

    If downburst is in path use it.  If either downburst is unavailable,
    or if downburst is unable to produce a json list, then use a default
    table.
    """
    default_table = {u'rhel_minimal': [u'6.4', u'6.5'],
                     u'fedora': [u'17', u'18', u'19', u'20', u'22'],
                     u'centos': [u'6.3', u'6.4', u'6.5', u'7.0'],
                     u'opensuse': [u'12.2'],
                     u'rhel': [u'6.3', u'6.4', u'6.5', u'7.0', u'7beta'],
                     u'centos_minimal': [u'6.4', u'6.5'],
                     u'ubuntu': [u'8.04(hardy)', u'9.10(karmic)',
                                 u'10.04(lucid)', u'10.10(maverick)',
                                 u'11.04(natty)', u'11.10(oneiric)',
                                 u'12.04(precise)', u'12.10(quantal)',
                                 u'13.04(raring)', u'13.10(saucy)',
                                 u'14.04(trusty)', u'utopic(utopic)'],
                     u'sles': [u'11-sp2'],
                     u'debian': [u'6.0', u'7.0', u'8.0']}
    executable_cmd = provision.downburst_executable()
    if not executable_cmd:
        log.warn("Downburst not found!")
        log.info('Using default values for supported os_type/os_version')
        return default_table
    try:
        output = subprocess.check_output([executable_cmd, 'list-json'])
        downburst_data = json.loads(output)
        return downburst_data
    except (subprocess.CalledProcessError, OSError):
        log.exception("Error calling downburst!")
        log.info('Using default values for supported os_type/os_version')
        return default_table


def vps_version_or_type_valid(machine_type, os_type, os_version):
    """
    Check os-type and os-version parameters when locking a vps.
    Os-type will always be set (defaults to ubuntu).

    In the case where downburst does not handle list-json (an older version
    of downburst, for instance), a message is printed and this checking
    is skipped (so that this code should behave as it did before this
    check was added).
    """
    if not machine_type == 'vps':
        return True
    if os_type is None or os_version is None:
        # we'll use the defaults provided by provision.create_if_vm
        # later on during provisioning
        return True
    valid_os_and_version = get_distro_from_downburst()
    if os_type not in valid_os_and_version:
        log.error("os-type '%s' is invalid. Try one of: %s",
                  os_type,
                  ', '.join(valid_os_and_version.keys()))
        return False
    if not validate_distro_version(os_version,
                                   valid_os_and_version[os_type]):
        log.error(
            "os-version '%s' is invalid for os-type '%s'. Try one of: %s",
            os_version,
            os_type,
            ', '.join(valid_os_and_version[os_type]))
        return False
    return True


def validate_distro_version(version, supported_versions):
    """
    Return True if the version is valid.  For Ubuntu, possible
    supported version values are of the form '12.04 (precise)' where
    either the number of the version name is acceptable.
    """
    if version in supported_versions:
        return True
    for parts in supported_versions:
        part = parts.split('(')
        if len(part) == 2:
            if version == part[0]:
                return True
            if version == part[1][0:len(part[1])-1]:
                return True


def get_statuses(machines):
    if machines:
        statuses = []
        for machine in machines:
            machine = misc.canonicalize_hostname(machine)
            status = get_status(machine)
            if status:
                statuses.append(status)
            else:
                log.error("Lockserver doesn't know about machine: %s" %
                          machine)
    else:
        statuses = list_locks()
    return statuses


def json_matching_statuses(json_file_or_str, statuses):
    """
    Filter statuses by json dict in file or fragment; return list of
    matching statuses.  json_file_or_str must be a file containing
    json or json in a string.
    """
    try:
        open(json_file_or_str, 'r')
    except IOError:
        query = json.loads(json_file_or_str)
    else:
        query = json.load(json_file_or_str)

    if not isinstance(query, dict):
        raise RuntimeError('--json-query must be a dict')

    return_statuses = list()
    for status in statuses:
        for k, v in query.iteritems():
            if not misc.is_in_dict(k, v, status):
                break
        else:
            return_statuses.append(status)

    return return_statuses


def winnow(statuses, arg, status_key, func=None):
    """
    Call with a list of statuses, and the ctx.<key>
    'arg' that you may want to filter by.
    If arg is not None, filter statuses by either:

    1) func=None: filter by status[status_key] == arg
    remove any status that fails

    2) func=<filter function that takes status>: remove any
    status for which func returns False

    Return the possibly-smaller set of statuses.
    """

    if arg is not None:
        if func:
            statuses = [_status for _status in statuses
                        if func(_status)]
        else:
            statuses = [_status for _status in statuses
                       if _status[status_key] == arg]

    return statuses


def main(ctx):
    if ctx.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    misc.read_config(ctx)

    ret = 0
    user = ctx.owner
    machines = [misc.canonicalize_hostname(m, user=False)
                for m in ctx.machines]
    machines_to_update = []

    if ctx.targets:
        try:
            with file(ctx.targets) as f:
                g = yaml.safe_load_all(f)
                for new in g:
                    if 'targets' in new:
                        for t in new['targets'].iterkeys():
                            machines.append(t)
        except IOError as e:
            raise argparse.ArgumentTypeError(str(e))

    if ctx.f:
        assert ctx.lock or ctx.unlock, \
            '-f is only supported by --lock and --unlock'
    if machines:
        assert ctx.lock or ctx.unlock or ctx.list or ctx.list_targets \
            or ctx.update or ctx.brief, \
            'machines cannot be specified with that operation'
    else:
        if ctx.lock:
            log.error("--lock requires specific machines passed as arguments")
        else:
            # This condition might never be hit, but it's not clear.
            assert ctx.num_to_lock or ctx.list or ctx.list_targets or \
                ctx.summary or ctx.brief, \
                'machines must be specified for that operation'
    if ctx.all:
        assert ctx.list or ctx.list_targets or ctx.brief, \
            '--all can only be used with --list, --list-targets, and --brief'
        assert ctx.owner is None, \
            '--all and --owner are mutually exclusive'
        assert not machines, \
            '--all and listing specific machines are incompatible'
    if ctx.num_to_lock:
        assert ctx.machine_type, \
            'must specify machine type to lock'

    if ctx.brief or ctx.list or ctx.list_targets:
        assert ctx.desc is None, '--desc does nothing with --list/--brief'

        # we may need to update host keys for vms.  Don't do it for
        # every vm; however, update any vms included in the list given
        # to the CLI (machines), or any owned by the specified owner or
        # invoking user if no machines are specified.
        vmachines = []
        statuses = get_statuses(machines)
        owner = ctx.owner or misc.get_user()
        for machine in statuses:
            if machine['is_vm'] and machine['locked'] and \
               (machines or machine['locked_by'] == owner):
                vmachines.append(machine['name'])
        if vmachines:
            log.info("updating host keys for %s", ' '.join(sorted(vmachines)))
            do_update_keys(vmachines)
            # get statuses again to refresh any updated keys
            statuses = get_statuses(machines)
        if statuses:
            statuses = winnow(statuses, ctx.machine_type, 'machine_type')
            if not machines and ctx.owner is None and not ctx.all:
                ctx.owner = misc.get_user()
            statuses = winnow(statuses, ctx.owner, 'locked_by')
            statuses = winnow(statuses, ctx.status, 'up',
                                lambda s: s['up'] == (ctx.status == 'up'))
            statuses = winnow(statuses, ctx.locked, 'locked',
                                lambda s: s['locked'] == (ctx.locked == 'true'))
            statuses = winnow(statuses, ctx.desc, 'description')
            statuses = winnow(statuses, ctx.desc_pattern, 'description',
                              lambda s: s['description'] and \
                                        ctx.desc_pattern in s['description'])
            if ctx.json_query:
                statuses = json_matching_statuses(ctx.json_query, statuses)
            statuses = winnow(statuses, ctx.os_type, 'os_type')
            statuses = winnow(statuses, ctx.os_version, 'os_version')

            # When listing, only show the vm_host's name, not every detail
            for s in statuses:
                if not s.get('is_vm', False):
                    continue
                # with an OpenStack API, there is no host for a VM
                if s['vm_host'] is None:
                    continue
                vm_host_name = s.get('vm_host', dict())['name']
                if vm_host_name:
                    s['vm_host'] = vm_host_name
            if ctx.list:
                    print json.dumps(statuses, indent=4)

            elif ctx.brief:
                for s in sorted(statuses, key=lambda s: s.get('name')):
                    locked = "un" if s['locked'] == 0 else "  "
                    mo = re.match('\w+@(\w+?)\..*', s['name'])
                    host = mo.group(1) if mo else s['name']
                    print '{host} {locked}locked {owner} "{desc}"'.format(
                        locked=locked, host=host,
                        owner=s['locked_by'], desc=s['description'])

            else:
                frag = {'targets': {}}
                for f in statuses:
                    frag['targets'][f['name']] = f['ssh_pub_key']
                print yaml.safe_dump(frag, default_flow_style=False)
        else:
            log.error('error retrieving lock statuses')
            ret = 1

    elif ctx.summary:
        do_summary(ctx)
        return 0

    elif ctx.lock:
        if not vps_version_or_type_valid(ctx.machine_type, ctx.os_type,
                                         ctx.os_version):
            log.error('Invalid os-type or version detected -- lock failed')
            return 1
        for machine in machines:
            if not lock_one(machine, user, ctx.desc):
                ret = 1
                if not ctx.f:
                    return ret
            else:
                machines_to_update.append(machine)
                provision.create_if_vm(ctx, machine)
    elif ctx.unlock:
        if ctx.owner is None and user is None:
            user = misc.get_user()
        # If none of them are vpm, do them all in one shot
        if not filter(is_vm, machines):
            res = unlock_many(machines, user)
            return 0 if res else 1
        for machine in machines:
            if not unlock_one(ctx, machine, user):
                ret = 1
                if not ctx.f:
                    return ret
            else:
                machines_to_update.append(machine)
    elif ctx.num_to_lock:
        result = lock_many(ctx, ctx.num_to_lock, ctx.machine_type, user,
                           ctx.desc, ctx.os_type, ctx.os_version)
        if not result:
            ret = 1
        else:
            machines_to_update = result.keys()
            if ctx.machine_type == 'vps':
                shortnames = ' '.join(
                    [misc.decanonicalize_hostname(name) for name in
                     result.keys()]
                )
                if len(result) < ctx.num_to_lock:
                    log.error("Locking failed.")
                    for machine in result:
                        unlock_one(ctx, machine, user)
                    ret = 1
                else:
                    log.info("Successfully Locked:\n%s\n" % shortnames)
                    log.info(
                        "Unable to display keys at this time (virtual " +
                        "machines are booting).")
                    log.info(
                        "Please run teuthology-lock --list-targets %s once " +
                        "these machines come up.",
                        shortnames)
            else:
                print yaml.safe_dump(
                    dict(targets=result),
                    default_flow_style=False)
    elif ctx.update:
        assert ctx.desc is not None or ctx.status is not None, \
            'you must specify description or status to update'
        assert ctx.owner is None, 'only description and status may be updated'
        machines_to_update = machines

        if ctx.desc is not None or ctx.status is not None:
            for machine in machines_to_update:
                update_lock(machine, ctx.desc, ctx.status)

    return ret


def lock_many_openstack(ctx, num, machine_type, user=None, description=None,
                        arch=None):
    os_type = provision.get_distro(ctx)
    os_version = provision.get_distro_version(ctx)
    if hasattr(ctx, 'config'):
        resources_hint = ctx.config.get('openstack')
    else:
        resources_hint = None
    machines =  provision.ProvisionOpenStack().create(
        num, os_type, os_version, arch, resources_hint)
    result = {}
    for machine in machines:
        lock_one(machine, user, description)
        result[machine] = None # we do not collect ssh host keys yet
    return result

def lock_many(ctx, num, machine_type, user=None, description=None,
              os_type=None, os_version=None, arch=None):
    if user is None:
        user = misc.get_user()

    if not vps_version_or_type_valid(ctx.machine_type, os_type, os_version):
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
        # Only query for os_type/os_version if non-vps, since in that case we
        # just create them.
        if machine_type != 'vps':
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
            if machine_type == 'vps':
                ok_machs = {}
                for machine in machines:
                    if provision.create_if_vm(ctx, machine):
                        ok_machs[machine] = machines[machine]
                    else:
                        log.error('Unable to create virtual machine: %s',
                                  machine)
                        unlock_one(ctx, machine, user)
                return ok_machs
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
    if not provision.destroy_if_vm(ctx, name, user, description):
        log.error('destroy failed for %s', name)
    request = dict(name=name, locked=False, locked_by=user,
                   description=description)
    uri = os.path.join(config.lock_server, 'nodes', name, 'lock', '')
    response = requests.put(uri, json.dumps(request))
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


def list_locks(keyed_by_name=False, **kwargs):
    uri = os.path.join(config.lock_server, 'nodes', '')
    for key, value in kwargs.iteritems():
        if kwargs[key] is False:
            kwargs[key] = '0'
        if kwargs[key] is True:
            kwargs[key] = '1'
    if kwargs:
        if 'machine_type' in kwargs:
            kwargs['machine_type'] = kwargs['machine_type'].replace(',','|')
        uri += '?' + urllib.urlencode(kwargs)
    try:
        response = requests.get(uri)
    except requests.ConnectionError:
        success = False
        log.exception("Could not contact lock server: %s", config.lock_server)
    else:
        success = response.ok
    if success:
        if not keyed_by_name:
            return response.json()
        else:
            return {node['name']: node
                    for node in response.json()}
    return dict()


def find_stale_locks(owner=None):
    """
    Return a list of node dicts corresponding to nodes that were locked to run
    a job, but the job is no longer running. The purpose of this is to enable
    us to nuke nodes that were left locked due to e.g. infrastructure failures
    and return them to the pool.

    :param owner: If non-None, return nodes locked by owner. Default is None.
    """
    def might_be_stale(node_dict):
        """
        Answer the question: "might this be a stale lock?"

        The answer is yes if:
            It is locked
            It has a non-null description containing multiple '/' characters

        ... because we really want "nodes that were locked for a particular job
        and are still locked" and the above is currently the best way to guess.
        """
        desc = node_dict['description']
        if (node_dict['locked'] is True and
            desc is not None and desc.startswith('/') and
                desc.count('/') > 1):
            return True
        return False

    # Which nodes are locked for jobs?
    nodes = list_locks()
    if owner is not None:
        nodes = [node for node in nodes if node['locked_by'] == owner]
    nodes = filter(might_be_stale, nodes)

    def node_job_is_active(node, cache):
        """
        Is this node's job active (e.g. running or waiting)?

        :param node:  The node dict as returned from the lock server
        :param cache: A set() used for caching results
        :returns:     True or False
        """
        description = node['description']
        if description in cache:
            return True
        (name, job_id) = description.split('/')[-2:]
        url = os.path.join(config.results_server, 'runs', name, 'jobs', job_id,
                           '')
        resp = requests.get(url)
        job_info = resp.json()
        if job_info['status'] in ('running', 'waiting'):
            cache.add(description)
            return True
        return False

    result = list()
    # Here we build the list of of nodes that are locked, for a job (as opposed
    # to being locked manually for random monkeying), where the job is not
    # running
    active_jobs = set()
    for node in nodes:
        if node_job_is_active(node, active_jobs):
            continue
        result.append(node)
    return result


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


def updatekeys(args):
    loglevel = logging.DEBUG if args['--verbose'] else logging.INFO
    logging.basicConfig(
        level=loglevel,
    )
    all_ = args['--all']
    if all_:
        machines = []
    elif args['<machine>']:
        machines = [misc.canonicalize_hostname(m, user=None)
                    for m in args['<machine>']]
    elif args['--targets']:
        targets = args['--targets']
        with file(targets) as f:
            docs = yaml.safe_load_all(f)
            for doc in docs:
                machines = [n for n in doc.get('targets', dict()).iterkeys()]

    return do_update_keys(machines, all_)


def do_update_keys(machines, all_=False):
    reference = list_locks(keyed_by_name=True)
    if all_:
        machines = reference.keys()
    keys_dict = misc.ssh_keyscan(machines)
    return push_new_keys(keys_dict, reference)


def push_new_keys(keys_dict, reference):
    ret = 0
    for hostname, pubkey in keys_dict.iteritems():
        log.info('Checking %s', hostname)
        if reference[hostname]['ssh_pub_key'] != pubkey:
            log.info('New key found. Updating...')
            if not update_lock(hostname, ssh_pub_key=pubkey):
                log.error('failed to update %s!', hostname)
                ret = 1
    return ret


def do_summary(ctx):
    lockd = collections.defaultdict(lambda: [0, 0, 'unknown'])
    if ctx.machine_type:
        locks = list_locks(machine_type=ctx.machine_type)
    else:
        locks = list_locks()
    for l in locks:
        who = l['locked_by'] if l['locked'] == 1 \
            else '(free)', l['machine_type']
        lockd[who][0] += 1
        lockd[who][1] += 1 if l['up'] else 0
        lockd[who][2] = l['machine_type']

    locks = sorted([p for p in lockd.iteritems()
                    ], key=lambda sort: (sort[1][2], sort[1][0]))
    total_count, total_up = 0, 0
    print "TYPE     COUNT  UP  OWNER"

    for (owner, (count, upcount, machinetype)) in locks:
            # if machinetype == spectype:
            print "{machinetype:8s} {count:3d}  {up:3d}  {owner}".format(
                count=count, up=upcount, owner=owner[0],
                machinetype=machinetype)
            total_count += count
            total_up += upcount

    print "         ---  ---"
    print "{cnt:12d}  {up:3d}".format(cnt=total_count, up=total_up)
