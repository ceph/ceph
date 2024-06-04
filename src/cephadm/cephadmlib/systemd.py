# systemd.py - general systemd related types and funcs

import logging

from typing import Tuple, List, Optional

from .context import CephadmContext
from .call_wrappers import call, CallVerbosity
from .packagers import Packager

logger = logging.getLogger()


def check_unit(ctx, unit_name):
    # type: (CephadmContext, str) -> Tuple[bool, str, bool]
    # NOTE: we ignore the exit code here because systemctl outputs
    # various exit codes based on the state of the service, but the
    # string result is more explicit (and sufficient).
    enabled = False
    installed = False
    try:
        out, err, code = call(
            ctx,
            ['systemctl', 'is-enabled', unit_name],
            verbosity=CallVerbosity.QUIET,
        )
        if code == 0:
            enabled = True
            installed = True
        elif 'disabled' in out:
            installed = True
    except Exception as e:
        logger.warning('unable to run systemctl: %s' % e)
        enabled = False
        installed = False

    state = 'unknown'
    try:
        out, err, code = call(
            ctx,
            ['systemctl', 'is-active', unit_name],
            verbosity=CallVerbosity.QUIET,
        )
        out = out.strip()
        if out in ['active']:
            state = 'running'
        elif out in ['inactive']:
            state = 'stopped'
        elif out in ['failed', 'auto-restart']:
            state = 'error'
        else:
            state = 'unknown'
    except Exception as e:
        logger.warning('unable to run systemctl: %s' % e)
        state = 'unknown'
    return (enabled, state, installed)


def check_units(ctx, units, enabler=None):
    # type: (CephadmContext, List[str], Optional[Packager]) -> bool
    for u in units:
        (enabled, state, installed) = check_unit(ctx, u)
        if enabled and state == 'running':
            logger.info('Unit %s is enabled and running' % u)
            return True
        if enabler is not None:
            if installed:
                logger.info('Enabling unit %s' % u)
                enabler.enable_service(u)
    return False


def terminate_service(ctx: CephadmContext, service_name: str) -> None:
    call(
        ctx,
        ['systemctl', 'stop', service_name],
        verbosity=CallVerbosity.DEBUG,
    )
    call(
        ctx,
        ['systemctl', 'reset-failed', service_name],
        verbosity=CallVerbosity.DEBUG,
    )
    call(
        ctx,
        ['systemctl', 'disable', service_name],
        verbosity=CallVerbosity.DEBUG,
    )
