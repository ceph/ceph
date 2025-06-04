# functions related to sending signals

import logging
import signal

from typing import Optional

from .call_wrappers import call, CallVerbosity
from .context import CephadmContext
from .exceptions import Error

logger = logging.getLogger()


def send_signal_to_container_entrypoint(
    ctx: CephadmContext,
    container_id: str,
    signal_name: Optional[str],
    signal_number: Optional[int],
) -> int:
    if not signal_name and not signal_number:
        raise Error(
            f'Got request to send signal to entrypoint of container with id {container_id} but no signal specified'
        )

    if signal_number and signal_name:
        raise Error(
            'signal_name and signal_number params to send_signal_to_container_entrypoint are mutually exclusive'
        )

    if signal_number:
        signal_name = signal.Signals(signal_number).name
    elif signal_name:
        signal_name = signal_name.upper()
        signal_number = signal.Signals[signal_name].value
    assert signal_name is not None and signal_number is not None

    logger.info(
        f'Sending signal {signal_name} ({signal_number}) to entrypoint of container with id {container_id}'
    )

    _, _, code = call(
        ctx,
        [
            ctx.container_engine.path,
            'kill',
            container_id,
            '--signal',
            signal_name,
        ],
        verbosity=CallVerbosity.VERBOSE,
        desc='',
    )
    return code
