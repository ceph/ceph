import contextlib
import logging

import teuthology.lock.ops
import teuthology.lock.query
import teuthology.lock.util
from teuthology.job_status import get_status

log = logging.getLogger(__name__)


@contextlib.contextmanager
def lock_machines(ctx, config):
    """
    Lock machines.  Called when the teuthology run finds and locks
    new machines.  This is not called if the one has teuthology-locked
    machines and placed those keys in the Targets section of a yaml file.
    """
    assert isinstance(config[0], int), 'config[0] must be an integer'
    machine_type = config[1]
    total_requested = config[0]
    # We want to make sure there are always this many machines available
    teuthology.lock.ops.block_and_lock_machines(ctx, total_requested, machine_type)
    try:
        yield
    finally:
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
