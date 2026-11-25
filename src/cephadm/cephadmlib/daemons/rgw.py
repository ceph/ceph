# rgw.py - the RGW daemon form and its host resources: the D3N cache and
# the systemd timer that pushes bucket usage metrics to pushgateway.

import json
import logging
import os
import shlex
import shutil

from enum import Enum
from typing import Any, Dict, List, Optional

from ceph.cephadm.d3n_types import D3NCache, D3NCacheError

from .. import systemd_unit
from .. import templating
from ..call_wrappers import CallVerbosity, call, call_throws
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..exceptions import Error
from ..file_utils import unlink_file, write_new
from .ceph import Ceph


logger = logging.getLogger()


def _d3n_fstab_entry(uuid: str, mountpoint: str, fs_type: str) -> str:
    return f'UUID={uuid} {mountpoint} {fs_type} defaults,noatime 0 2\n'


def _ensure_fstab_entry(ctx: CephadmContext, device: str, mountpoint: str, fs_type: str) -> None:
    """
    Ensure the device is present in /etc/fstab for the given mountpoint.
    If an entry for mountpoint already exists, no changes are made.
    """
    out, _, code = call(ctx, ['blkid', '-s', 'UUID', '-o', 'value', device])
    if code != 0 or not out.strip():
        raise Error(f'Failed to get UUID for {device}')
    uuid = out.strip()

    entry = _d3n_fstab_entry(uuid, mountpoint, fs_type)

    # check if mountpoint already present in fstab
    with open('/etc/fstab', 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) >= 2 and parts[1] == mountpoint:
                return

    with open('/etc/fstab', 'a') as f:
        f.write(entry)


class D3NStateAction(str, Enum):
    WRITE = 'write'
    CLEANUP = 'cleanup'


def d3n_state(
        ctx: CephadmContext,
        data_dir: str,
        action: D3NStateAction,
        d3n: Optional[D3NCache] = None,
        uid: int = 0,
        gid: int = 0,
) -> None:
    """
    Persist/read minimal D3N info in the daemon's data directory
    so that rm-daemon can cleanup properly.
    """
    path = os.path.join(data_dir, 'd3n_state.json')

    if action == D3NStateAction.WRITE:
        if d3n is None:
            return
        state = {
            'cache_path': d3n.cache_path,
            'mount_path': d3n.mountpoint,
        }
        payload = json.dumps(state, sort_keys=True) + '\n'
        with write_new(path, owner=(uid, gid)) as f:
            f.write(payload)
        return

    if action == D3NStateAction.CLEANUP:
        if not os.path.exists(path):
            return

        try:
            with open(path, 'r') as f:
                state = json.load(f)
        except Exception:
            state = {}

        cache_path = state.get('cache_path') if isinstance(state, dict) else None
        if isinstance(cache_path, str) and cache_path:
            try:
                shutil.rmtree(cache_path, ignore_errors=True)
                logger.info(f'[D3N] removed cache directory: {cache_path}')
            except Exception as e:
                logger.warning(f'[D3N] failed to remove cache directory {cache_path}: {e}')

        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f'[D3N] failed to remove {path}: {e}')
        return

    raise Error(f'[D3N] invalid d3n_state action: {action}')


def prepare_d3n_cache(ctx: CephadmContext, d3n: D3NCache, uid: int, gid: int) -> None:
    """
    Prepare a D3N cache mount and directory.

    Steps:
      1. Ensure mountpoint directory exists
      2. Format device if it has no filesystem
      3. Ensure /etc/fstab entry exists
      4. Mount if not mounted
      5. Ensure cache_path exists and is owned by daemon uid/gid
    """
    device = d3n.device
    fs_type = d3n.filesystem
    mountpoint = d3n.mountpoint
    cache_path = d3n.cache_path
    size_bytes = d3n.size_bytes

    logger.debug(
        f'[D3N] prepare_d3n_cache: device={device!r} fs_type={fs_type!r} mountpoint={mountpoint!r} cache_path={cache_path!r} size_bytes={size_bytes!r}'
    )

    # Ensure mountpoint exists
    os.makedirs(mountpoint, mode=0o755, exist_ok=True)
    logger.debug(f'[D3N] checking filesystem on device {device}')

    # Format the device if needed
    if not _has_filesystem(ctx, device):
        logger.debug(f'Formatting {device} with {fs_type} for D3N')
        call_throws(ctx, ['mkfs', '-t', fs_type, device])

    # Ensure the mount is persistent across reboot by ensuring an /etc/fstab entry exists.
    _ensure_fstab_entry(ctx, device, mountpoint, fs_type)

    if not _is_mountpoint(ctx, mountpoint):
        logger.debug(f'[D3N] mountpoint not mounted, running mount {mountpoint}')
        call_throws(ctx, ['mount', mountpoint])
    else:
        logger.debug(f'[D3N] mountpoint already mounted according to _is_mountpoint(): {mountpoint}')

    if size_bytes is not None:
        avail = _avail_bytes(ctx, mountpoint)
        if avail < size_bytes:
            raise Error(
                f'Not enough free space for D3N cache on {mountpoint}: '
                f'need {size_bytes} bytes, have {avail} bytes'
            )

    # Create per-daemon cache directory
    os.makedirs(cache_path, mode=0o755, exist_ok=True)
    call_throws(ctx, ['chown', '-R', f'{uid}:{gid}', cache_path])


def _has_filesystem(ctx: CephadmContext, device: str) -> bool:
    if not os.path.exists(device):
        raise Error(f'D3N device does not exist: {device}')
    out, _, code = call(ctx, ['blkid', '-o', 'value', '-s', 'TYPE', device])
    return code == 0 and bool(out.strip())


def _is_mountpoint(ctx: CephadmContext, path: str) -> bool:
    out, err, code = call(
        ctx,
        ['findmnt', '-n', '-o', 'TARGET,SOURCE,FSTYPE', '--mountpoint', path],
    )
    logger.debug(
        f'[D3N] _is_mountpoint({path}): code={code} out={out.strip()!r} err={err.strip()!r}'
    )
    return code == 0


def _avail_bytes(ctx: CephadmContext, path: str) -> int:
    out, _, code = call(ctx, ['df', '-B1', '--output=avail', path])
    if code != 0:
        raise Error(f'Failed to check free space for {path}')
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if len(lines) < 2:
        raise Error(f'Unexpected df output for {path}: {out!r}')
    logger.debug(f'[D3N] df avail bytes for {path}: {lines[1]!r}')

    return int(lines[1])


def _read_metrics_push_endpoint(path: str) -> Dict[str, str]:
    saved: Dict[str, str] = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                saved[key] = value
    except FileNotFoundError:
        return {}
    return saved


def _metrics_push_target_changed(
    saved: Dict[str, str],
    pushgateway_url: str,
    zone_name: str,
) -> bool:
    # No saved push means there is nothing to drop. Disable uses
    # force_delete so it does not depend on this comparison.
    if not saved.get('endpoint'):
        return False
    return (
        saved.get('pushgateway_url', '') != pushgateway_url
        or saved.get('zone_name', '') != zone_name
    )


def _delete_pushgateway_group(
    ctx: CephadmContext,
    ident: 'DaemonIdentity',
    endpoint: str,
) -> bool:
    # 404 means the group is already gone. Pushgateway returns 202 on
    # delete. A failure must not abort reconfig; the caller keeps the
    # endpoint file so a later reconfig or push can retry.
    if not endpoint:
        return True
    data_dir = ident.data_dir(ctx.data_dir)
    cert_names = (
        'metrics-push-ca.pem',
        'metrics-push.crt',
        'metrics-push.key',
    )
    have_certs = all(
        os.path.exists(os.path.join(data_dir, name)) for name in cert_names
    )
    engine = getattr(getattr(ctx, 'container_engine', None), 'path', '')
    cid = ''
    if engine:
        for name in (ident.container_name, ident.legacy_container_name):
            out, _, code = call(
                ctx,
                [engine, 'ps', '-q', '--filter', f'name={name}'],
                verbosity=CallVerbosity.DEBUG,
                timeout=30,
            )
            if code != 0:
                continue
            ids = [line.strip() for line in out.splitlines() if line.strip()]
            if ids:
                cid = ids[0]
                break
    cert_dir = (
        f'/var/lib/ceph/radosgw/ceph-rgw.{ident.daemon_id}' if cid else data_dir
    )
    cmd = [
        'curl', '-sS', '-o', '/dev/null', '-w', '%{http_code}',
        '-X', 'DELETE',
    ]
    if have_certs:
        cmd.extend([
            '--cacert', os.path.join(cert_dir, cert_names[0]),
            '--cert', os.path.join(cert_dir, cert_names[1]),
            '--key', os.path.join(cert_dir, cert_names[2]),
        ])
    cmd.append(endpoint)
    if cid and engine:
        cmd = [engine, 'exec', cid, *cmd]
    out, err, code = call(
        ctx, cmd, verbosity=CallVerbosity.DEBUG, timeout=30,
    )
    http = out.split()[-1] if out.split() else ''
    if http in ('200', '202', '404'):
        return True
    detail = err.strip().replace('\n', ' ')[:200]
    logger.warning(
        f'failed to delete pushgateway group {endpoint} for '
        f'{ident.daemon_name} (http={http!r} rc={code}'
        f'{": " + detail if detail else ""})'
    )
    return False


def _retire_metrics_push_timer(
    ctx: CephadmContext,
    ident: 'DaemonIdentity',
    desired: Optional[Dict[str, Any]],
    force_delete: bool,
) -> None:
    unit_id = f'metrics-push@{ident.daemon_type}.{ident.daemon_id}'
    # Stop the timer before deleting so an in-flight push cannot
    # rewrite the group we just removed.
    systemd_unit.remove_timer_unit(ctx, ident.fsid, unit_id)
    data_dir = ident.data_dir(ctx.data_dir)
    path = os.path.join(data_dir, 'metrics-push.endpoint')
    saved = _read_metrics_push_endpoint(path)
    if not saved.get('endpoint'):
        return
    changed = False
    if desired is not None:
        changed = _metrics_push_target_changed(
            saved,
            str(desired.get('pushgateway_url', '')),
            str(desired.get('zone_name', '')),
        )
    if force_delete or changed:
        endpoint = saved.get('endpoint', '')
        if endpoint and _delete_pushgateway_group(ctx, ident, endpoint):
            unlink_file(path, missing_ok=True)
            logger.info(
                f'Removed pushgateway group {endpoint} for {ident.daemon_name}'
            )
        elif endpoint:
            # Keep the file so the next reconfig or push can retry.
            logger.warning(
                f'Leaving {path} in place so the pushgateway group for '
                f'{ident.daemon_name} can be deleted on a later reconfig'
            )
        return
    if desired is not None:
        # Same target on a daemon that is no longer elected: handoff.
        # The new exporter replaces the group on its next push.
        logger.debug(
            f'{ident.daemon_name}: keeping pushgateway group across '
            f'metrics-push handoff'
        )


def _deploy_metrics_push_timer(
    ctx: CephadmContext,
    ident: 'DaemonIdentity',
    metrics_push: Dict[str, Any],
) -> None:
    """Set up a systemd timer that periodically pushes RGW bucket usage
    metrics to pushgateway via rgw-export-bucket-usage."""
    pushgateway_url = str(metrics_push.get('pushgateway_url', ''))
    zone_name = str(metrics_push.get('zone_name', ''))
    interval = int(metrics_push.get('interval', 3600))
    timeout = int(metrics_push.get('timeout', 300))

    if not pushgateway_url:
        logger.warning('metrics_push enabled but no pushgateway_url provided, skipping timer')
        return

    data_dir = ident.data_dir(ctx.data_dir)
    unit_id = f'metrics-push@{ident.daemon_type}.{ident.daemon_id}'
    endpoint_path = os.path.join(data_dir, 'metrics-push.endpoint')
    saved_endpoint = _read_metrics_push_endpoint(endpoint_path)
    if _metrics_push_target_changed(saved_endpoint, pushgateway_url, zone_name):
        systemd_unit.remove_timer_unit(ctx, ident.fsid, unit_id)
        endpoint = saved_endpoint.get('endpoint', '')
        if endpoint and _delete_pushgateway_group(ctx, ident, endpoint):
            unlink_file(endpoint_path, missing_ok=True)
            logger.info(
                f'Removed pushgateway group {endpoint} for {ident.daemon_name}'
            )
        elif endpoint:
            logger.warning(
                f'Leaving {endpoint_path} in place so the pushgateway group '
                f'for {ident.daemon_name} can be deleted on a later reconfig'
            )

    # The endpoint URL expected by rgw-export-bucket-usage includes the job path
    endpoint = f'{pushgateway_url}/metrics/job/rgw-bucket-usage'

    container_name = ident.container_name
    legacy_container_name = ident.legacy_container_name
    script_path = os.path.join(data_dir, 'metrics-push.run')
    status_file = os.path.join(data_dir, 'metrics-push.status')

    # The RGW container only has the daemon keyring, not the admin keyring.
    # CEPH_ARGS lets radosgw-admin authenticate with the daemon keyring.
    daemon_name = f'client.rgw.{ident.daemon_id}'
    keyring_path = f'/var/lib/ceph/radosgw/ceph-rgw.{ident.daemon_id}/keyring'
    ceph_args = f'-n {daemon_name} -k {keyring_path}'

    # Build the zone-id resolution command.  If a zone name was provided in
    # the spec, query that specific zone, otherwise query the default zone.
    if zone_name:
        zone_get_cmd = f'radosgw-admin zone get --rgw-zone {shlex.quote(zone_name)}'
    else:
        zone_get_cmd = 'radosgw-admin zone get'

    # When secure_monitoring_stack is enabled, write TLS certs to the data
    # directory (which is mounted into the RGW container) so curl can
    # authenticate with pushgateway.
    pushgateway_args = ''
    root_cert = metrics_push.get('root_cert', '')
    client_cert = metrics_push.get('client_cert', '')
    client_key = metrics_push.get('client_key', '')
    if root_cert and client_cert and client_key:
        for fname, content in [('metrics-push-ca.pem', root_cert),
                               ('metrics-push.crt', client_cert),
                               ('metrics-push.key', client_key)]:
            fpath = os.path.join(data_dir, fname)
            with write_new(fpath, owner=None, perms=0o600) as f:
                f.write(content)
        # cert paths inside the container
        cdata_dir = f'/var/lib/ceph/radosgw/ceph-rgw.{ident.daemon_id}'
        pushgateway_args = (
            f'--cacert {cdata_dir}/metrics-push-ca.pem '
            f'--cert {cdata_dir}/metrics-push.crt '
            f'--key {cdata_dir}/metrics-push.key'
        )

    # generate the bash script that exec's into the RGW container
    engine = ctx.container_engine.path
    script = templating.render(
        ctx,
        templating.Templates.metrics_push_run,
        status_file=status_file,
        engine=engine,
        container_name=container_name,
        legacy_container_name=legacy_container_name,
        ceph_args=ceph_args,
        zone_get_cmd=zone_get_cmd,
        endpoint=endpoint,
        endpoint_file=endpoint_path,
        pushgateway_url=pushgateway_url,
        zone_name=zone_name,
        pushgateway_args=pushgateway_args,
        timeout=timeout,
    )
    with write_new(script_path, owner=None, perms=0o755) as f:
        f.write(script)

    logger.info(
        f'Installing metrics push timer for {ident.daemon_name} '
        f'(interval={interval}s, pushgateway={pushgateway_url})'
    )
    # install a systemd timer to run the script periodically
    systemd_unit.install_timer_unit(
        ctx,
        fsid=ident.fsid,
        unit_id=unit_id,
        job_description=f'RGW metrics push for {ident.daemon_name}',
        script_path=script_path,
        interval_sec=interval,
        timeout_sec=timeout,
        randomized_delay_sec=min(60, interval // 10),
    )


@register_daemon_form
class RGW(Ceph):
    """Defines an RGW daemon. The container is set up by Ceph; this form
    adds the D3N cache and the metrics push timer on the host.
    """

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return daemon_type == 'rgw'

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'Ceph':
        return cls(ctx, ident)

    @classmethod
    def prepare_host_resources(
        cls, ctx: CephadmContext, ident: DaemonIdentity, uid: int, gid: int
    ) -> None:
        data_dir = ident.data_dir(ctx.data_dir)
        config_json = fetch_configs(ctx)
        d3n_cache: Any = config_json.get('d3n_cache')

        if d3n_cache:
            try:
                d3n = D3NCache.from_json(d3n_cache)
            except D3NCacheError as e:
                raise Error(str(e))
            prepare_d3n_cache(ctx, d3n, uid, gid)
            try:
                d3n_state(ctx, data_dir, D3NStateAction.WRITE, d3n, uid, gid)
            except Exception as e:
                logger.warning(f'[D3N] failed to persist D3N state in {data_dir}: {e}')

        metrics_push: Any = config_json.get('metrics_push')
        if metrics_push:
            assert isinstance(metrics_push, dict)
            _deploy_metrics_push_timer(ctx, ident, metrics_push)
        else:
            target: Any = config_json.get('metrics_push_target')
            _retire_metrics_push_timer(
                ctx,
                ident,
                desired=target if isinstance(target, dict) else None,
                force_delete=bool(config_json.get('metrics_push_cleanup')),
            )

    @classmethod
    def remove_host_resources(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> None:
        # force remove rgw admin socket file if leftover
        rgw_asok_path = f'/var/run/ceph/{ident.fsid}/ceph-client.{ident.daemon_name}.*.asok'
        call(ctx, ['rm', '-rf', rgw_asok_path],
             verbosity=CallVerbosity.DEBUG)

        data_dir = ident.data_dir(ctx.data_dir)
        d3n_state(ctx, data_dir, action=D3NStateAction.CLEANUP)
        timer_unit_id = f'metrics-push@{ident.daemon_type}.{ident.daemon_id}'
        systemd_unit.remove_timer_unit(ctx, ident.fsid, timer_unit_id)

    @classmethod
    def host_resource_unit_globs(cls, fsid: str) -> List[str]:
        return [f'ceph-{fsid}-metrics-push@*']
