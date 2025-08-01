# functions related to sending signals

import logging
import signal

from typing import Optional, Union

from .call_wrappers import call, CallVerbosity
from .context import CephadmContext
from .exceptions import Error

logger = logging.getLogger()


def send_signal_to_container_entrypoint(
    ctx: CephadmContext,
    container_id: str,
    sig: Union[str, int]
) -> int:
    sig_str: Optional[str] = None
    sig_int: Optional[int] = None
    if isinstance(sig, int) or sig.isdigit():
        # when you convert the Signal object to a str it gives you
        # something of the form "Signals.<signal-name>" e.g. Signals.SIGHUP
        try:
            sig_str = str(signal.Signals(int(sig))).split('.')[1]
        except ValueError:
            raise Error(f'Failed to find signal name for signal ({sig})')
        sig_int = int(sig)
    else:
        sig_str = sig.upper()
        try:
            sig_int = signal.Signals[sig if sig.startswith('SIG') else 'SIG'+sig]
        except KeyError:
            raise Error(f'Failed to find signal number for signal "{sig}"')

    logger.info(
        f'Sending signal {sig_str} ({sig_int}) to entrypoint of container with id {container_id}'
    )

    _, _, code = call(
        ctx,
        [
            ctx.container_engine.path,
            'kill',
            container_id,
            '--signal',
            sig_str,
        ],
        verbosity=CallVerbosity.VERBOSE,
        desc='',
    )
    return code
