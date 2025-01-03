# sysctl.py - functions for working with linux sysctl properties

import logging
import os
import shutil

from glob import glob
from pathlib import Path
from typing import List, Union

from .call_wrappers import call, call_throws
from .context import CephadmContext
from .daemon_form import DaemonForm, SysctlDaemonForm
from .file_utils import write_new

logger = logging.getLogger()


def install_sysctl(
    ctx: CephadmContext, fsid: str, daemon: DaemonForm
) -> None:
    """
    Set up sysctl settings
    """

    def _write(conf: Path, lines: List[str]) -> None:
        lines = [
            '# created by cephadm',
            '',
            *lines,
            '',
        ]
        with write_new(conf, owner=None, perms=None) as f:
            f.write('\n'.join(lines))

    if not isinstance(daemon, SysctlDaemonForm):
        return

    daemon_type = daemon.identity.daemon_type
    conf = Path(ctx.sysctl_dir).joinpath(f'90-ceph-{fsid}-{daemon_type}.conf')

    lines = daemon.get_sysctl_settings()
    lines = filter_sysctl_settings(ctx, lines)

    # apply the sysctl settings
    if lines:
        Path(ctx.sysctl_dir).mkdir(mode=0o755, exist_ok=True)
        _write(conf, lines)
        call_throws(ctx, ['sysctl', '--system'])


def sysctl_get(ctx: CephadmContext, variable: str) -> Union[str, None]:
    """
    Read a sysctl setting by executing 'sysctl -b {variable}'
    """
    out, err, code = call(ctx, ['sysctl', '-b', variable])
    return out or None


def filter_sysctl_settings(
    ctx: CephadmContext, lines: List[str]
) -> List[str]:
    """
    Given a list of sysctl settings, examine the system's current configuration
    and return those which are not currently set as described.
    """

    def test_setting(desired_line: str) -> bool:
        # Remove any comments
        comment_start = desired_line.find('#')
        if comment_start != -1:
            desired_line = desired_line[:comment_start]
        desired_line = desired_line.strip()
        if not desired_line or desired_line.isspace():
            return False
        setting, desired_value = map(
            lambda s: s.strip(), desired_line.split('=')
        )
        if not setting or not desired_value:
            return False
        actual_value = sysctl_get(ctx, setting)
        return desired_value != actual_value

    return list(filter(test_setting, lines))


def migrate_sysctl_dir(ctx: CephadmContext, fsid: str) -> None:
    """
    Cephadm once used '/usr/lib/sysctl.d' for storing sysctl configuration.
    This moves it to '/etc/sysctl.d'.
    """
    deprecated_location: str = '/usr/lib/sysctl.d'
    deprecated_confs: List[str] = glob(
        f'{deprecated_location}/90-ceph-{fsid}-*.conf'
    )
    if not deprecated_confs:
        return

    file_count: int = len(deprecated_confs)
    logger.info(
        f'Found sysctl {file_count} files in deprecated location {deprecated_location}. Starting Migration.'
    )
    for conf in deprecated_confs:
        try:
            shutil.move(conf, ctx.sysctl_dir)
            file_count -= 1
        except shutil.Error as err:
            if str(err).endswith('already exists'):
                logger.warning(
                    f'Destination file already exists. Deleting {conf}.'
                )
                try:
                    os.unlink(conf)
                    file_count -= 1
                except OSError as del_err:
                    logger.warning(f'Could not remove {conf}: {del_err}.')
            else:
                logger.warning(
                    f'Could not move {conf} from {deprecated_location} to {ctx.sysctl_dir}: {err}'
                )

    # Log successful migration
    if file_count == 0:
        logger.info(
            f'Successfully migrated sysctl config to {ctx.sysctl_dir}.'
        )
        return

    # Log partially successful / unsuccessful migration
    files_processed: int = len(deprecated_confs)
    if file_count < files_processed:
        status: str = (
            f'partially successful (failed {file_count}/{files_processed})'
        )
    elif file_count == files_processed:
        status = 'unsuccessful'
    logger.warning(
        f'Migration of sysctl configuration {status}. You may want to perform a migration manually.'
    )
