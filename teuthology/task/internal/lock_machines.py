import contextlib
import logging
import time
import yaml

import teuthology.lock.ops
import teuthology.lock.query
import teuthology.lock.util
from teuthology import misc
from teuthology import provision
from teuthology import report

from teuthology.config import config as teuth_config
from teuthology.job_status import get_status, set_status

log = logging.getLogger(__name__)


@contextlib.contextmanager
def lock_machines(ctx, config):
    """
    Lock machines.  Called when the teuthology run finds and locks
    new machines.  This is not called if the one has teuthology-locked
    machines and placed those keys in the Targets section of a yaml file.
    """
    lock_machines_helper(ctx, config)
    try:
        yield
    finally:
        unlock_machines(ctx)


def lock_machines_helper(ctx, config, reimage=True):
    # It's OK for os_type and os_version to be None here.  If we're trying
    # to lock a bare metal machine, we'll take whatever is available.  If
    # we want a vps, defaults will be provided by misc.get_distro and
    # misc.get_distro_version in provision.create_if_vm
    os_type = ctx.config.get("os_type")
    os_version = ctx.config.get("os_version")
    arch = ctx.config.get('arch')
    log.info('Locking machines...')
    assert isinstance(config[0], int), 'config[0] must be an integer'
    machine_type = config[1]
    total_requested = config[0]
    # We want to make sure there are always this many machines available
    reserved = teuth_config.reserve_machines
    assert isinstance(reserved, int), 'reserve_machines must be integer'
    assert (reserved >= 0), 'reserve_machines should >= 0'

    # change the status during the locking process
    report.try_push_job_info(ctx.config, dict(status='waiting'))

    all_locked = dict()
    requested = total_requested
    while True:
        # get a candidate list of machines
        machines = teuthology.lock.query.list_locks(machine_type=machine_type, up=True,
                                                    locked=False, count=requested + reserved)
        if machines is None:
            if ctx.block:
                log.error('Error listing machines, trying again')
                time.sleep(20)
                continue
            else:
                raise RuntimeError('Error listing machines')

        # make sure there are machines for non-automated jobs to run
        if len(machines) < reserved + requested and ctx.owner.startswith('scheduled'):
            if ctx.block:
                log.info(
                    'waiting for more %s machines to be free (need %s + %s, have %s)...',
                    machine_type,
                    reserved,
                    requested,
                    len(machines),
                )
                time.sleep(10)
                continue
            else:
                assert 0, ('not enough machines free; need %s + %s, have %s' %
                           (reserved, requested, len(machines)))

        try:
            newly_locked = teuthology.lock.ops.lock_many(ctx, requested, machine_type,
                                                         ctx.owner, ctx.archive, os_type,
                                                         os_version, arch, reimage=reimage)
        except Exception:
            # Lock failures should map to the 'dead' status instead of 'fail'
            if 'summary' in ctx:
                set_status(ctx.summary, 'dead')
            raise
        all_locked.update(newly_locked)
        log.info(
            '{newly_locked} {mtype} machines locked this try, '
            '{total_locked}/{total_requested} locked so far'.format(
                newly_locked=len(newly_locked),
                mtype=machine_type,
                total_locked=len(all_locked),
                total_requested=total_requested,
            )
        )
        if len(all_locked) == total_requested:
            vmlist = []
            for lmach in all_locked:
                if teuthology.lock.query.is_vm(lmach):
                    vmlist.append(lmach)
            if vmlist:
                log.info('Waiting for virtual machines to come up')
                keys_dict = dict()
                loopcount = 0
                while len(keys_dict) != len(vmlist):
                    loopcount += 1
                    time.sleep(10)
                    keys_dict = misc.ssh_keyscan(vmlist)
                    log.info('virtual machine is still unavailable')
                    if loopcount == 40:
                        loopcount = 0
                        log.info('virtual machine(s) still not up, ' +
                                 'recreating unresponsive ones.')
                        for guest in vmlist:
                            if guest not in keys_dict.keys():
                                log.info('recreating: ' + guest)
                                full_name = misc.canonicalize_hostname(guest)
                                provision.destroy_if_vm(ctx, full_name)
                                provision.create_if_vm(ctx, full_name)
                if teuthology.lock.ops.do_update_keys(keys_dict)[0]:
                    log.info("Error in virtual machine keys")
                newscandict = {}
                for dkey in all_locked.keys():
                    stats = teuthology.lock.query.get_status(dkey)
                    newscandict[dkey] = stats['ssh_pub_key']
                ctx.config['targets'] = newscandict
            else:
                ctx.config['targets'] = all_locked
            locked_targets = yaml.safe_dump(
                ctx.config['targets'],
                default_flow_style=False
            ).splitlines()
            log.info('\n  '.join(['Locked targets:', ] + locked_targets))
            # successfully locked machines, change status back to running
            report.try_push_job_info(ctx.config, dict(status='running'))
            break
        elif not ctx.block:
            assert 0, 'not enough machines are available'
        else:
            requested = requested - len(newly_locked)
            assert requested > 0, "lock_machines: requested counter went" \
                                  "negative, this shouldn't happen"

        log.info(
            "{total} machines locked ({new} new); need {more} more".format(
                total=len(all_locked), new=len(newly_locked), more=requested)
        )
        log.warn('Could not lock enough machines, waiting...')
        time.sleep(10)


def unlock_machines(ctx):
    # If both unlock_on_failure and nuke-on-error are set, don't unlock now
    # because we're just going to nuke (and unlock) later.
    unlock_on_failure = (
            ctx.config.get('unlock_on_failure', False)
            and not ctx.config.get('nuke-on-error', False)
        )
    if get_status(ctx.summary) == 'pass' or unlock_on_failure:
        log.info('Unlocking machines...')
        for machine in ctx.config['targets'].keys():
            teuthology.lock.ops.unlock_one(ctx, machine, ctx.owner, ctx.archive)
