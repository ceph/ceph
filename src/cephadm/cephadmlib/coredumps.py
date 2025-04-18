# coredumps.py - cephadm functionality related to coredumps

import contextlib
import logging

from pathlib import Path

from cephadmlib.call_wrappers import call_throws
from cephadmlib.context import CephadmContext
from cephadmlib.file_utils import read_file, write_new, unlink_file
from cephadmlib.systemd_unit import write_coredumpctl_override_drop_in

logger = logging.getLogger()


def set_coredump_overrides(
    ctx: CephadmContext, fsid: str, max_coredump_size: str
) -> None:
    """
    Override coredumpctl settings
    """
    coredump_overrides_dir = Path('/etc/systemd/coredump.conf.d')
    coredump_override_path = Path(coredump_overrides_dir).joinpath(
        f'90-cephadm-{fsid}-coredumps-overrides.conf'
    )

    if coredump_override_path.exists():
        coredump_override_content = read_file([str(coredump_override_path)])
        if (
            f'ProcessSizeMax={max_coredump_size}' in coredump_override_content
            and f'ExternalSizeMax={max_coredump_size}'
            in coredump_override_content
        ):
            logger.info(
                f'{str(coredump_override_path)} already has sizes set to {max_coredump_size}, skipping...'
            )
            return

    if not coredump_overrides_dir.is_dir():
        coredump_overrides_dir.mkdir(parents=True, exist_ok=True)

    with contextlib.ExitStack() as estack:
        coredumpctl_override_fh = estack.enter_context(
            write_new(coredump_override_path, perms=None)
        )
        write_coredumpctl_override_drop_in(
            coredumpctl_override_fh, ctx, max_coredump_size=max_coredump_size
        )

    logger.info(
        f'Set coredump max sizes in {str(coredump_override_path)} to {max_coredump_size}. '
        'Restarting systemd-coredump.socket'
    )
    call_throws(ctx, ['systemctl', 'restart', 'systemd-coredump.socket'])
    logger.info('systemd-coredump.socket restarted successfully')


def remove_coredump_overrides(ctx: CephadmContext, fsid: str) -> None:
    """
    Remove drop-in file for setting coredump max size
    """
    coredump_overrides_path = Path(
        f'/etc/systemd/coredump.conf.d/90-cephadm-{fsid}-coredumps-overrides.conf'
    )
    unlink_file(coredump_overrides_path, missing_ok=True)
