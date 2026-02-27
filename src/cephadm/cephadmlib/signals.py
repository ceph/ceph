# functions related to sending signals

import logging
import signal

from typing import Optional, Union

from .call_wrappers import call, CallVerbosity
from .context import CephadmContext
from .exceptions import Error

logger = logging.getLogger()


def send_signal_to_container_entrypoint(
    ctx: CephadmContext, container_name: str, sig: Union[str, int]
) -> int:
    sig_str: Optional[str] = None
    if isinstance(sig, int) or sig.isdigit():
        try:
            sig_value = signal.Signals(int(sig))
        except ValueError:
            raise Error(f'Failed to find signal name for signal ({sig})')
    else:
        name = sig.upper()
        sig_value = signal.Signals[
            name if name.startswith('SIG') else 'SIG' + name
        ]
    sig_str = str(sig_value.name)

    logger.info(
        f'Sending signal {sig_str} to entrypoint of container {container_name}'
    )

    _, _, code = call(
        ctx,
        [
            ctx.container_engine.path,
            'kill',
            container_name,
            '--signal',
            sig_str,
        ],
        verbosity=CallVerbosity.VERBOSE,
        desc='',
    )
    return code
