import argparse
import collections
import json
import logging
import re

import yaml

import teuthology
import teuthology.parallel
import teuthology.provision
from teuthology import misc
from teuthology.config import set_config_attr

from teuthology.lock import (
    ops,
    util,
    query,
)


log = logging.getLogger(__name__)


def main(ctx):
    if ctx.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    set_config_attr(ctx)

    ret = 0
    user = ctx.owner
    machines = [misc.canonicalize_hostname(m, user=False)
                for m in ctx.machines]
    machines_to_update = []

    if ctx.targets:
        try:
            with open(ctx.targets) as f:
                g = yaml.safe_load_all(f)
                for new in g:
                    if 'targets' in new:
                        for t in new['targets'].keys():
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
        statuses = query.get_statuses(machines)
        owner = ctx.owner or misc.get_user()
        for machine in statuses:
            if query.is_vm(status=machine) and machine['locked'] and \
               (machines or machine['locked_by'] == owner):
                vmachines.append(machine['name'])
        if vmachines:
            log.info("updating host keys for %s", ' '.join(sorted(vmachines)))
            ops.do_update_keys(vmachines, _raise=False)
            # get statuses again to refresh any updated keys
            statuses = query.get_statuses(machines)
        if statuses:
            statuses = util.winnow(statuses, ctx.machine_type, 'machine_type')
            if not machines and ctx.owner is None and not ctx.all:
                ctx.owner = misc.get_user()
            statuses = util.winnow(statuses, ctx.owner, 'locked_by')
            statuses = util.winnow(statuses, ctx.status, 'up',
                                lambda s: s['up'] == (ctx.status == 'up'))
            statuses = util.winnow(statuses, ctx.locked, 'locked',
                                lambda s: s['locked'] == (ctx.locked == 'true'))
            statuses = util.winnow(statuses, ctx.desc, 'description')
            statuses = util.winnow(statuses, ctx.desc_pattern, 'description',
                              lambda s: s['description'] and \
                                        ctx.desc_pattern in s['description'])
            if ctx.json_query:
                statuses = util.json_matching_statuses(ctx.json_query, statuses)
            statuses = util.winnow(statuses, ctx.os_type, 'os_type')
            statuses = util.winnow(statuses, ctx.os_version, 'os_version')

            # When listing, only show the vm_host's name, not every detail
            for s in statuses:
                if not query.is_vm(status=s):
                    continue
                # with an OpenStack API, there is no host for a VM
                if s['vm_host'] is None:
                    continue
                vm_host_name = s.get('vm_host', dict())['name']
                if vm_host_name:
                    s['vm_host'] = vm_host_name
            if ctx.list:
                    print(json.dumps(statuses, indent=4))

            elif ctx.brief:
                maxname = max((len(_['name'] or '')
                                            for _ in statuses), default=0)
                maxuser = max((len(_['locked_by'] or 'None')
                                            for _ in statuses), default=0)
                node_status_template = (
                    '{{host:<{name}}} {{up:<4}} {{locked:<8}} '
                    '{{owner:<{user}}} "{{desc}}"'
                    ).format(name=maxname, user=maxuser)
                for s in sorted(statuses, key=lambda s: s.get('name')):
                    locked = 'unlocked' if s['locked'] == 0 else 'locked'
                    up = 'up' if s['up'] else 'down'
                    mo = re.match('\w+@(\w+?)\..*', s['name'])
                    host = mo.group(1) if mo else s['name']
                    print(node_status_template.format(
                        up=up, locked=locked, host=host,
                        owner=s['locked_by'] or 'None', desc=s['description']))

            else:
                frag = {'targets': {}}
                for f in statuses:
                    frag['targets'][f['name']] = f['ssh_pub_key']
                print(yaml.safe_dump(frag, default_flow_style=False))
        else:
            log.error('error retrieving lock statuses')
            ret = 1

    elif ctx.summary:
        do_summary(ctx)
        return 0

    elif ctx.lock:
        if not util.vps_version_or_type_valid(
                ctx.machine_type, ctx.os_type, ctx.os_version):
            log.error('Invalid os-type or version detected -- lock failed')
            return 1
        reimage_types = teuthology.provision.get_reimage_types()
        reimage_machines = list()
        updatekeys_machines = list()
        machine_types = dict()
        for machine in machines:
            resp = ops.lock_one(machine, user, ctx.desc)
            if resp.ok:
                machine_status = resp.json()
                machine_type = machine_status['machine_type']
                machine_types[machine] = machine_type
            if not resp.ok:
                ret = 1
                if not ctx.f:
                    return ret
            elif not query.is_vm(machine, machine_status):
                if machine_type in reimage_types:
                    # Reimage in parallel just below here
                    reimage_machines.append(machine)
                # Update keys last
                updatekeys_machines = list()
            else:
                machines_to_update.append(machine)
                ops.update_nodes([machine], True)
                teuthology.provision.create_if_vm(
                    ctx,
                    misc.canonicalize_hostname(machine),
                )
        with teuthology.parallel.parallel() as p:
            ops.update_nodes(reimage_machines, True)
            for machine in reimage_machines:
                p.spawn(teuthology.provision.reimage, ctx, machine, machine_types[machine])
        for machine in updatekeys_machines:
            ops.do_update_keys([machine])
        ops.update_nodes(reimage_machines + machines_to_update)

    elif ctx.unlock:
        if ctx.owner is None and user is None:
            user = misc.get_user()
        # If none of them are vpm, do them all in one shot
        if not filter(query.is_vm, machines):
            res = ops.unlock_many(machines, user)
            return 0 if res else 1
        for machine in machines:
            if not ops.unlock_one(ctx, machine, user):
                ret = 1
                if not ctx.f:
                    return ret
            else:
                machines_to_update.append(machine)
    elif ctx.num_to_lock:
        result = ops.lock_many(ctx, ctx.num_to_lock, ctx.machine_type, user,
                           ctx.desc, ctx.os_type, ctx.os_version, ctx.arch)
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
                        ops.unlock_one(ctx, machine, user)
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
                print(yaml.safe_dump(
                    dict(targets=result),
                    default_flow_style=False))
    elif ctx.update:
        assert ctx.desc is not None or ctx.status is not None, \
            'you must specify description or status to update'
        assert ctx.owner is None, 'only description and status may be updated'
        machines_to_update = machines

        if ctx.desc is not None or ctx.status is not None:
            for machine in machines_to_update:
                ops.update_lock(machine, ctx.desc, ctx.status)

    return ret


def do_summary(ctx):
    lockd = collections.defaultdict(lambda: [0, 0, 'unknown'])
    if ctx.machine_type:
        locks = query.list_locks(machine_type=ctx.machine_type)
    else:
        locks = query.list_locks()
    for l in locks:
        who = l['locked_by'] if l['locked'] == 1 \
            else '(free)', l['machine_type']
        lockd[who][0] += 1
        lockd[who][1] += 1 if l['up'] else 0
        lockd[who][2] = l['machine_type']

    # sort locks by machine type and count
    locks = sorted([p for p in lockd.items()
                    ], key=lambda sort: (sort[1][2] or '', sort[1][0]))
    total_count, total_up = 0, 0
    print("TYPE     COUNT  UP  OWNER")

    for (owner, (count, upcount, machinetype)) in locks:
            # if machinetype == spectype:
            print("{machinetype:8s} {count:3d}  {up:3d}  {owner}".format(
                count=count, up=upcount, owner=owner[0],
                machinetype=machinetype or '(none)'))
            total_count += count
            total_up += upcount

    print("         ---  ---")
    print("{cnt:12d}  {up:3d}".format(cnt=total_count, up=total_up))


def updatekeys(args):
    loglevel = logging.DEBUG if args['--verbose'] else logging.INFO
    logging.basicConfig(
        level=loglevel,
    )
    all_ = args['--all']
    machines = []
    if args['<machine>']:
        machines = [misc.canonicalize_hostname(m, user=None)
                    for m in args['<machine>']]
    elif args['--targets']:
        targets = args['--targets']
        with open(targets) as f:
            docs = yaml.safe_load_all(f)
            for doc in docs:
                machines = [n for n in doc.get('targets', dict()).keys()]

    return ops.do_update_keys(machines, all_)[0]
