import logging

import teuthology.lock.query
import teuthology.lock.util

from teuthology.config import config as teuth_config

log = logging.getLogger(__name__)


def check_lock(ctx, config, check_up=True):
    """
    Check lock status of remote machines.
    """
    if not teuth_config.lock_server or ctx.config.get('check-locks') is False:
        log.info('Lock checking disabled.')
        return
    log.info('Checking locks...')
    for machine in ctx.config['targets'].keys():
        status = teuthology.lock.query.get_status(machine)
        log.debug('machine status is %s', repr(status))
        assert status is not None, \
            'could not read lock status for {name}'.format(name=machine)
        if check_up:
            assert status['up'], 'machine {name} is marked down'.format(
                name=machine
            )
        assert status['locked'], \
            'machine {name} is not locked'.format(name=machine)
        assert status['locked_by'] == ctx.owner, \
            'machine {name} is locked by {user}, not {owner}'.format(
                name=machine,
                user=status['locked_by'],
                owner=ctx.owner,
            )
