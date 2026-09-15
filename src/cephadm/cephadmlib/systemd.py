import logging
import os
import threading
from typing import List, Optional, Sequence, Tuple, Union

import pystemd.run as pystemd_run
from pystemd.dbusexc import (
    DBusFileNotFoundError,
    DBusNoSuchUnitError,
)
from pystemd.systemd1 import Manager as PystemdManager
from pystemd.systemd1 import Unit as PystemdUnit

from .constants import DISABLED_SERVICES
from .context import CephadmContext
from .listing import DaemonEntry, daemons_matching

logger = logging.getLogger()

# D-Bus ActiveState -> cephadm state string
_ACTIVE_STATE_MAP = {
    'active': 'running',
    'inactive': 'stopped',
    'failed': 'error',
    'maintenance': 'error',
}

# D-Bus UnitFileState values that count as "enabled"
_ENABLED_STATES = {'enabled', 'enabled-runtime', 'static'}

# Absolute path to iproute2 ``ip`` on the host (pystemd requires absolute paths).
_HOST_IP = '/usr/sbin/ip'


def _b(value: str) -> bytes:
    """Encode a str for pystemd D-Bus calls (which expect bytes)."""
    return value.encode()


def _decode(value: object) -> str:
    """Decode pystemd property/method results that may be bytes or str."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    return str(value)


def _exc_str(exc: BaseException) -> str:
    """Format an exception for logging, decoding any bytes from pystemd."""
    err_message = getattr(exc, 'err_message', None)
    if isinstance(err_message, (bytes, bytearray, str)) and err_message:
        errno = getattr(exc, 'errno', None)
        msg = _decode(err_message)
        if errno is not None:
            return f'[err {errno}]: {msg}'
        return msg
    if exc.args:
        return ': '.join(_decode(a) for a in exc.args)
    return _decode(str(exc))


def run_in_host(
    cmd: Union[Sequence[str], str, bytes],
    name: str = 'cephadm-run-in-host.service',
    *,
    raise_on_fail: bool = False,
    capture_output: bool = False,
    **kwargs: object,
) -> Union[int, Tuple[int, str]]:
    """Run *cmd* on the host via a transient systemd oneshot (pystemd.run).

    Useful when cephadm itself runs inside a container and needs to execute
    something in the host mount/network/pid namespaces that systemd manages.

    Returns the command exit status, or ``(status, stdout)`` when
    *capture_output* is True.  By default does not raise on non-zero exit so
    callers can inspect the status (e.g. errno from a bind check).
    Pass raise_on_fail=True to raise PystemdRunError on failure instead.
    """
    opts: dict = {
        'service_type': 'oneshot',
        'wait': True,
        'raise_on_fail': raise_on_fail,
        'collect': True,
        'user_mode': False,
    }
    opts.update(kwargs)

    if not capture_output:
        unit = pystemd_run(cmd, name=name, **opts)
        if raise_on_fail:
            return 0
        status = unit.Service.ExecMainStatus
        return int(status) if status is not None else 0

    # Capture host stdout via an SCM_RIGHTS-passed pipe on the system bus.
    r_fd, w_fd = os.pipe()
    chunks: List[bytes] = []

    def _reader() -> None:
        try:
            while True:
                data = os.read(r_fd, 1 << 16)
                if not data:
                    break
                chunks.append(data)
        finally:
            os.close(r_fd)

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()
    try:
        opts['stdout'] = w_fd
        unit = pystemd_run(cmd, name=name, **opts)
    finally:
        try:
            os.close(w_fd)
        except OSError:
            pass
        reader.join(timeout=60)

    out = b''.join(chunks).decode('utf-8', errors='replace')
    if raise_on_fail:
        return 0, out
    status = unit.Service.ExecMainStatus
    return (int(status) if status is not None else 0), out


def host_ip(args: Sequence[str], name: str = 'cephadm-ip.service') -> str:
    """Run ``/usr/sbin/ip`` on the host with *args*; return stdout.

    Raises RuntimeError if the command exits non-zero.
    """
    result = run_in_host(
        [_HOST_IP, *args],
        name=name,
        capture_output=True,
    )
    assert isinstance(result, tuple)
    status, out = result
    if status != 0:
        raise RuntimeError(
            f'host ip {" ".join(args)} failed with status {status}'
        )
    return out


class SystemdManager:
    """Singleton gateway to the org.freedesktop.systemd1 D-Bus service via pystemd."""

    _instance: Optional['SystemdManager'] = None

    def __init__(self) -> None:
        self._manager = PystemdManager()
        self._manager.load()

    @classmethod
    def get(cls) -> 'SystemdManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def daemon_reload(self) -> None:
        self._manager.Manager.Reload()

    def start_unit(self, unit_name: str, mode: str = 'replace') -> None:
        self._manager.Manager.StartUnit(_b(unit_name), _b(mode))

    def stop_unit(self, unit_name: str, mode: str = 'replace') -> None:
        self._manager.Manager.StopUnit(_b(unit_name), _b(mode))

    def restart_unit(self, unit_name: str, mode: str = 'replace') -> None:
        self._manager.Manager.RestartUnit(_b(unit_name), _b(mode))

    def reset_failed_unit(self, unit_name: str) -> None:
        self._manager.Manager.ResetFailedUnit(_b(unit_name))

    def enable_unit_file(self, unit_name: str) -> None:
        self._manager.Manager.EnableUnitFiles([_b(unit_name)], False, True)

    def disable_unit_file(self, unit_name: str) -> None:
        self._manager.Manager.DisableUnitFiles([_b(unit_name)], False)

    def is_active(self, unit_name: str) -> str:
        """Return the ActiveState for *unit_name* using LoadUnit so stopped units return 'inactive'."""
        self._manager.Manager.LoadUnit(_b(unit_name))
        unit = PystemdUnit(_b(unit_name))
        unit.load()
        return _decode(unit.Unit.ActiveState)

    def is_enabled(self, unit_name: str) -> Optional[str]:
        """Return the UnitFileState for *unit_name*, or None if the unit file does not exist."""
        try:
            return _decode(self._manager.Manager.GetUnitFileState(_b(unit_name)))
        except (DBusNoSuchUnitError, DBusFileNotFoundError):
            return None


def check_unit(ctx: CephadmContext, unit_name: str) -> Tuple[bool, str, bool]:
    sysd_mgr = SystemdManager.get()
    enabled = False
    installed = False
    try:
        unit_file_state = sysd_mgr.is_enabled(unit_name)
        if unit_file_state is not None:
            installed = True
            if unit_file_state in _ENABLED_STATES:
                enabled = True
    except Exception as e:
        logger.warning('unable to query systemd for %s: %s', unit_name, _exc_str(e))

    state = 'unknown'
    try:
        state = _ACTIVE_STATE_MAP.get(sysd_mgr.is_active(unit_name), 'unknown')
    except Exception as e:
        logger.warning('unable to query systemd for %s: %s', unit_name, _exc_str(e))

    return (enabled, state, installed)


def check_units(ctx: CephadmContext, units: List[str]) -> bool:
    for u in units:
        (enabled, state, installed) = check_unit(ctx, u)
        if enabled and state == 'running':
            logger.info('Unit %s is enabled and running' % u)
            return True
        if installed:
            logger.info('Enabling unit %s' % u)
            enable_service(ctx, u)
    return False


def terminate_service(ctx: CephadmContext, service_name: str) -> None:
    sysd_mgr = SystemdManager.get()
    for op, method in [
        ('stop', sysd_mgr.stop_unit),
        ('reset-failed', sysd_mgr.reset_failed_unit),
        ('disable', sysd_mgr.disable_unit_file),
    ]:
        try:
            method(service_name)
        except Exception as e:
            logger.warning('unable to %s %s: %s', op, service_name, _exc_str(e))


def enable_service(ctx: CephadmContext, service_name: str) -> None:
    """Start and enable the service (typically using systemd)."""
    sysd_mgr = SystemdManager.get()
    try:
        sysd_mgr.enable_unit_file(service_name)
    except Exception as e:
        logger.warning('unable to enable %s: %s', service_name, _exc_str(e))
        return
    try:
        sysd_mgr.start_unit(service_name)
    except Exception as e:
        logger.warning('unable to start %s: %s', service_name, _exc_str(e))


def start_disabled_services_after_maintenance_exit(
    ctx: CephadmContext,
) -> None:
    """Start nfs/keepalived units after host-maintenance exit."""
    if not ctx.fsid:
        return
    sysd_mgr = SystemdManager.get()
    for daemon_type in DISABLED_SERVICES:
        for entry in daemons_matching(
            ctx, fsid=ctx.fsid, daemon_type=daemon_type
        ):
            if isinstance(entry, DaemonEntry):
                unit = entry.identity.unit_name
            else:
                unit = entry.status['systemd_unit']
            try:
                sysd_mgr.start_unit(unit)
                logger.info('Started %s after maintenance exit', unit)
            except Exception as e:
                logger.warning(
                    'Failed to start %s after maintenance exit: %s',
                    unit,
                    _exc_str(e),
                )
