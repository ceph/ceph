"""
Cluster shutdown/startup/status operations helper functions.
"""

import datetime
import json
import logging
import os
import time
from concurrent.futures import (
    CancelledError,
    ThreadPoolExecutor,
    as_completed,
)
from typing import Any, Callable, Dict, List, Optional, Tuple

from .call_wrappers import call, CallVerbosity
from .net_utils import get_hostname
from .constants import (
    ADMIN_LABEL,
    CEPH_CONF,
    CEPH_CONF_DIR,
    CEPH_DEFAULT_CONF,
    CEPH_DEFAULT_KEYRING,
    CEPH_KEYRING,
    DATA_DIR,
)
from .container_types import CephContainer
from .context import CephadmContext
from .daemon_identity import DaemonIdentity
from .exceptions import Error


logger = logging.getLogger()


# Daemon type ordering for cluster shutdown operations.
# Shutdown order: monitoring/mgmt first, then gateways, then storage,
# then core last. This is the reverse of CEPH_UPGRADE_ORDER from
# mgr/cephadm/utils.py
DAEMON_SHUTDOWN_ORDER = [
    # Management and monitoring (stop first)
    'oauth2-proxy',
    'mgmt-gateway',
    'alloy',
    'promtail',
    'loki',
    'grafana',
    'alertmanager',
    'prometheus',
    'node-exporter',
    # Gateways
    'smb',
    'nvmeof',
    'nfs',
    'iscsi',
    # Ceph services (stop last - core services)
    'ceph-exporter',
    'cephfs-mirror',
    'rbd-mirror',
    'rgw',
    'mds',
    'osd',
    'crash',
    'mon',
    'mgr',
]

# Daemon types that should be stopped individually before stopping hosts
# These are client-facing services that should be gracefully stopped first
# Order: gateways/clients first (they depend on core services)
DAEMONS_TO_STOP_FIRST = [
    'nvmeof',
    'iscsi',
    'nfs',
    'smb',
    'rgw',
    'mds',
]

# OSD flags to set during shutdown (and unset during startup)
SHUTDOWN_OSD_FLAGS = ['noout']

# Maximum number of hosts to process in parallel during shutdown/startup
DEFAULT_PARALLEL_HOSTS = 5

# Timeout for parallel host operations (seconds)
HOST_OPERATION_TIMEOUT = 300

# Timeout waiting for cluster to become accessible (seconds)
CLUSTER_ACCESSIBLE_TIMEOUT = 120

# Timeout waiting for PGs to become clean (seconds)
PG_CLEAN_TIMEOUT = 300


def print_section_header(title: str, dry_run: bool = False) -> None:
    """Print a section header with consistent formatting."""
    suffix = ' DRY-RUN' if dry_run else ''
    print('=' * 60)
    print(f'{title}{suffix}')
    print('=' * 60)


def print_section_footer() -> None:
    """Print a section footer."""
    print('=' * 60)


def print_host_list(
    hosts: List[str],
    host_daemon_map: Dict[str, List[str]],
    admin_host: Optional[str],
    title: str = 'Hosts',
) -> None:
    """Print a numbered list of hosts with their daemons."""
    print(f'\n{title} ({len(hosts)}):')
    for i, hostname in enumerate(hosts, 1):
        daemon_types = host_daemon_map.get(hostname, [])
        admin_marker = ' (admin)' if hostname == admin_host else ''
        print(f'  {i}. {hostname}{admin_marker}')
        if daemon_types:
            daemons_str = ', '.join(sorted(daemon_types))
        else:
            daemons_str = 'none'
        print(f'     Daemons: {daemons_str}')


def wait_for_cluster_accessible(
    ctx: 'CephadmContext',
    timeout: int = CLUSTER_ACCESSIBLE_TIMEOUT,
) -> bool:
    """
    Wait for cluster to become accessible with exponential backoff.

    Returns:
        True if cluster became accessible, False if timeout
    """
    waited = 0
    sleep_interval = 2

    while waited < timeout:
        try:
            run_ceph_command(ctx, ['ceph', 'status'])
            logger.info('Cluster is accessible')
            return True
        except RuntimeError:
            pass
        time.sleep(sleep_interval)
        waited += sleep_interval
        logger.info(f'Waiting for cluster... ({waited}s)')
        sleep_interval = min(sleep_interval * 2, 30)

    logger.warning(
        f'Timeout waiting for cluster to become accessible after {timeout}s'
    )
    return False


def wait_for_pgs_clean(
    ctx: 'CephadmContext',
    timeout: int = PG_CLEAN_TIMEOUT,
) -> bool:
    """
    Wait for all PGs to become active+clean.

    Returns:
        True if PGs became clean, False if timeout
    """
    waited = 0

    while waited < timeout:
        is_clean, pg_msg = check_pgs_clean(ctx)
        if is_clean:
            logger.info(pg_msg)
            return True
        logger.info(f'{pg_msg} ({waited}s)')
        time.sleep(10)
        waited += 10

    logger.warning(
        f'Timeout waiting for PGs to become clean after {timeout}s'
    )
    return False


def set_osd_flags(
    ctx: 'CephadmContext', flags: List[str], force: bool = False
) -> List[str]:
    """
    Set OSD flags.

    Returns:
        List of flags that were successfully set
    """
    flags_set = []
    for flag in flags:
        try:
            run_ceph_command(ctx, ['ceph', 'osd', 'set', flag])
            flags_set.append(flag)
            logger.info(f'Set OSD {flag} flag')
        except RuntimeError as e:
            if not force:
                raise Error(f'Failed to set {flag} flag: {e}')
            logger.warning(f'Failed to set {flag} flag: {e}')
    return flags_set


def unset_osd_flags(ctx: 'CephadmContext', flags: List[str]) -> None:
    """Unset OSD flags."""
    for flag in flags:
        try:
            run_ceph_command(ctx, ['ceph', 'osd', 'unset', flag])
            logger.info(f'Unset OSD {flag} flag')
        except RuntimeError as e:
            logger.warning(f'Failed to unset {flag} flag: {e}')


def get_cluster_state_file(fsid: str) -> str:
    """Get the path to the cluster shutdown state file."""
    return os.path.join(DATA_DIR, fsid, 'cluster-shutdown-state.json')


def save_cluster_state(fsid: str, state: Dict[str, Any]) -> None:
    """Save cluster state to a JSON file."""
    state_file = get_cluster_state_file(fsid)
    try:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f'Cluster state saved to {state_file}')
    except OSError as e:
        raise Error(f'Failed to save cluster state to {state_file}: {e}')


def load_cluster_state(fsid: str) -> Optional[Dict[str, Any]]:
    """Load cluster state from JSON file."""
    state_file = get_cluster_state_file(fsid)
    if not os.path.exists(state_file):
        logger.info(f'No cluster state file found for FSID {fsid}')
        return None
    try:
        with open(state_file, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.warning(f'Corrupt state file {state_file}: {e}')
        return None
    except OSError as e:
        logger.warning(f'Failed to read state file {state_file}: {e}')
        return None


def remove_cluster_state(fsid: str) -> None:
    """Remove the cluster state file."""
    state_file = get_cluster_state_file(fsid)
    if os.path.exists(state_file):
        os.remove(state_file)
        logger.info(f'Cluster state file removed: {state_file}')


def find_admin_keyring(ctx: CephadmContext) -> Optional[str]:
    """
    Find the admin keyring file path.

    Returns:
        Path to the keyring file, or None if not found.
    """
    keyring_file = f'{ctx.data_dir}/{ctx.fsid}/{CEPH_CONF_DIR}/{CEPH_KEYRING}'
    if os.path.exists(keyring_file):
        return keyring_file
    if os.path.exists(CEPH_DEFAULT_KEYRING):
        return CEPH_DEFAULT_KEYRING
    return None


def find_ceph_config(ctx: CephadmContext) -> Optional[str]:
    """
    Find the ceph config file path.

    Returns:
        Path to the config file, or None if not found.
    """
    config_file = f'{ctx.data_dir}/{ctx.fsid}/{CEPH_CONF_DIR}/{CEPH_CONF}'
    if os.path.exists(config_file):
        return config_file
    if os.path.exists(CEPH_DEFAULT_CONF):
        return CEPH_DEFAULT_CONF
    return None


def run_ceph_command(
    ctx: CephadmContext, cmd: List[str], json_output: bool = False
) -> Any:
    """
    Run a ceph command using CephContainer directly.

    Args:
        ctx: CephadmContext
        cmd: Command to run (e.g., ['ceph', 'osd', 'set', 'noout'])
        json_output: If True, parse output as JSON

    Returns:
        Command output as string, or parsed JSON if json_output=True

    Raises:
        RuntimeError: If the command fails
    """
    # Build volume mounts for config and keyring
    mounts: Dict[str, str] = {}

    keyring = find_admin_keyring(ctx)
    if keyring:
        mounts[keyring] = '/etc/ceph/ceph.client.admin.keyring:z'

    config = find_ceph_config(ctx)
    if config:
        mounts[config] = '/etc/ceph/ceph.conf:z'

    # Strip 'ceph' prefix if present (entrypoint is /usr/bin/ceph)
    args = cmd[1:] if cmd and cmd[0] == 'ceph' else cmd

    out = CephContainer(
        ctx,
        image=ctx.image,
        entrypoint='/usr/bin/ceph',
        args=args,
        volume_mounts=mounts,
    ).run(verbosity=CallVerbosity.QUIET_UNLESS_ERROR)

    if json_output and out.strip():
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            logger.warning(f'Failed to parse JSON output: {out}')

    return out


def get_orch_hosts(ctx: CephadmContext) -> List[Dict[str, Any]]:
    """Get list of hosts from orchestrator."""
    try:
        result = run_ceph_command(
            ctx,
            ['ceph', 'orch', 'host', 'ls', '--format', 'json'],
            json_output=True,
        )
        return result if isinstance(result, list) else []
    except RuntimeError as e:
        raise Error(f'Failed to get host list: {e}')


def get_orch_daemons(ctx: CephadmContext) -> List[Dict[str, Any]]:
    """Get list of daemons from orchestrator."""
    try:
        result = run_ceph_command(
            ctx, ['ceph', 'orch', 'ps', '--format', 'json'], json_output=True
        )
        return result if isinstance(result, list) else []
    except RuntimeError as e:
        raise Error(f'Failed to get daemon list: {e}')


def get_admin_host(hosts: List[Dict[str, Any]]) -> Optional[str]:
    """Find the admin host (host with _admin label)."""
    for host in hosts:
        labels = host.get('labels', [])
        if ADMIN_LABEL in labels:
            return host.get('hostname') or host.get('addr')
    return None


def build_host_daemon_map(
    daemons: List[Dict[str, Any]]
) -> Dict[str, List[str]]:
    """Build a map of hostname -> list of daemon types."""
    host_map: Dict[str, List[str]] = {}
    for daemon in daemons:
        hostname = daemon.get('hostname', '')
        daemon_type = daemon.get('daemon_type', '')
        if hostname and daemon_type:
            if hostname not in host_map:
                host_map[hostname] = []
            if daemon_type not in host_map[hostname]:
                host_map[hostname].append(daemon_type)
    return host_map


def get_daemon_type_priority(daemon_types: List[str]) -> int:
    """
    Get the shutdown priority for a list of daemon types.
    Lower number = process earlier in shutdown.

    Monitoring/gateways first (low number), core services last (high number).
    """
    min_priority = len(DAEMON_SHUTDOWN_ORDER)

    for dtype in daemon_types:
        if dtype in DAEMON_SHUTDOWN_ORDER:
            priority = DAEMON_SHUTDOWN_ORDER.index(dtype)
            min_priority = min(min_priority, priority)

    return min_priority


def order_hosts_for_shutdown(
    host_daemon_map: Dict[str, List[str]],
    admin_host: Optional[str],
) -> List[str]:
    """
    Order hosts for shutdown based on their daemon types.

    Monitoring hosts first, core (mon/mgr) hosts last, admin host very last.
    """
    hosts_with_priority = []
    for hostname, daemon_types in host_daemon_map.items():
        priority = get_daemon_type_priority(daemon_types)
        hosts_with_priority.append((hostname, priority))

    # Sort by priority
    hosts_with_priority.sort(key=lambda x: x[1])
    ordered_hosts = [h[0] for h in hosts_with_priority]

    # Admin host is always last for shutdown
    if admin_host and admin_host in ordered_hosts:
        ordered_hosts.remove(admin_host)
        ordered_hosts.append(admin_host)

    return ordered_hosts


def get_cephadm_ssh_key(ctx: CephadmContext) -> Optional[str]:
    """
    Get the cephadm SSH private key and cache it locally.

    Returns:
        Path to the cached SSH key file, or None if not available
    """
    cached_key_path = os.path.join(DATA_DIR, ctx.fsid, '.ssh_key')

    # Check for cached key first
    if os.path.exists(cached_key_path):
        return cached_key_path

    # Retrieve from config-key store
    try:
        out = run_ceph_command(
            ctx, ['ceph', 'config-key', 'get', 'mgr/cephadm/ssh_identity_key']
        )
        if out.strip():
            parent_dir = os.path.dirname(cached_key_path)
            os.makedirs(parent_dir, exist_ok=True)
            os.chmod(parent_dir, 0o700)
            with open(cached_key_path, 'w') as f:
                f.write(out)
            os.chmod(cached_key_path, 0o600)
            logger.debug('Cached SSH key for remote operations')
            return cached_key_path
    except RuntimeError as e:
        logger.warning(f'Failed to retrieve SSH key: {e}')

    return None


def remove_cached_ssh_key(fsid: str) -> None:
    """Remove the cached SSH key file."""
    cached_key_path = os.path.join(DATA_DIR, fsid, '.ssh_key')
    if os.path.exists(cached_key_path):
        os.remove(cached_key_path)
        logger.info('Removed cached SSH key')


def remote_systemctl(
    ctx: CephadmContext,
    hostname: str,
    action: str,
    target: str,
    is_local: bool = False,
    ssh_key: Optional[str] = None,
    timeout: Optional[int] = None,
) -> Tuple[str, str, int]:
    """
    Run systemctl command on a remote host (or locally).

    Args:
        ctx: CephadmContext
        hostname: Target hostname
        action: systemctl action (stop, start, enable, disable, is-active)
        target: systemd target/unit name
        is_local: If True, run locally instead of via SSH
        ssh_key: Path to SSH private key file
        timeout: Command timeout in seconds

    Returns:
        Tuple of (stdout, stderr, return_code)
    """
    if is_local:
        cmd = ['systemctl', action, target]
    else:
        cmd = ['ssh', '-o', 'StrictHostKeyChecking=accept-new']
        if ssh_key:
            cmd.extend(['-i', ssh_key])
        cmd.extend([f'root@{hostname}', 'systemctl', action, target])

    return call(ctx, cmd, verbosity=CallVerbosity.QUIET, timeout=timeout)


def stop_daemons_by_type(
    ctx: CephadmContext,
    daemons: List[Dict[str, Any]],
    daemon_types: List[str],
    ssh_key: Optional[str] = None,
) -> List[str]:
    """
    Stop daemons of specified types individually.

    Args:
        ctx: CephadmContext
        daemons: List of daemon info dicts from orchestrator
        daemon_types: List of daemon types to stop
        ssh_key: Path to SSH private key file

    Returns:
        List of daemon names that failed to stop
    """
    current_host = get_hostname()
    failed = []

    for daemon_type in daemon_types:
        # Find all daemons of this type
        type_daemons = [
            d for d in daemons if d.get('daemon_type') == daemon_type
        ]
        if not type_daemons:
            continue

        count = len(type_daemons)
        logger.info(f'Stopping {count} {daemon_type} daemon(s)...')

        for daemon in type_daemons:
            hostname = daemon.get('hostname', '')
            daemon_id = daemon.get('daemon_id', '')
            daemon_name = f'{daemon_type}.{daemon_id}'

            if not hostname or not daemon_id:
                logger.warning(
                    f'Skipping daemon with incomplete data: {daemon}'
                )
                continue

            unit = DaemonIdentity(
                ctx.fsid, daemon_type, daemon_id
            ).service_name
            is_local = hostname == current_host

            logger.info(f'  Stopping {daemon_name} on {hostname}')
            _, err, code = remote_systemctl(
                ctx,
                hostname,
                'stop',
                unit,
                is_local,
                ssh_key,
                timeout=HOST_OPERATION_TIMEOUT,
            )
            if code != 0:
                logger.warning(f'  Failed to stop {daemon_name}: {err}')
                failed.append(daemon_name)
            else:
                logger.info(f'  Stopped {daemon_name}')

    return failed


def _apply_to_cephfs(
    ctx: CephadmContext,
    action: str,
    cmd_builder: Callable[[str], List[str]],
    success_msg: str,
    failure_msg: str,
) -> List[str]:
    """
    Apply an action to all CephFS filesystems.

    Args:
        ctx: CephadmContext
        action: Description of action for logging
        cmd_builder: Function that takes fs_name and returns command list
        success_msg: Log message format for success ({fs_name} placeholder)
        failure_msg: Log message format for failure ({fs_name} placeholder)

    Returns:
        List of filesystem names that failed
    """
    try:
        fs_result = run_ceph_command(
            ctx, ['ceph', 'fs', 'ls', '--format', 'json'], json_output=True
        )
    except RuntimeError as e:
        logger.warning(f'Failed to list filesystems: {e}')
        return []

    if not isinstance(fs_result, list):
        return []

    failed = []
    for fs in fs_result:
        fs_name = fs.get('name')
        if not fs_name:
            continue
        try:
            run_ceph_command(ctx, cmd_builder(fs_name))
            logger.info(success_msg.format(fs_name=fs_name))
        except RuntimeError as e:
            logger.warning(f'{failure_msg.format(fs_name=fs_name)}: {e}')
            failed.append(fs_name)

    return failed


def fail_cephfs_filesystems(ctx: CephadmContext) -> List[str]:
    """Fail all CephFS filesystems for graceful shutdown."""
    return _apply_to_cephfs(
        ctx,
        action='fail',
        cmd_builder=lambda fs_name: ['ceph', 'fs', 'fail', fs_name],
        success_msg='Failed filesystem: {fs_name}',
        failure_msg='Failed to fail filesystem {fs_name}',
    )


def set_cephfs_joinable(ctx: CephadmContext) -> List[str]:
    """Set all CephFS filesystems to joinable for startup."""
    return _apply_to_cephfs(
        ctx,
        action='set joinable',
        cmd_builder=lambda fs_name: [
            'ceph',
            'fs',
            'set',
            fs_name,
            'joinable',
            'true',
        ],
        success_msg='Set filesystem {fs_name} to joinable',
        failure_msg='Failed to set filesystem {fs_name} joinable',
    )


def _get_cluster_status(
    ctx: CephadmContext,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Get cluster status as JSON.

    Returns:
        Tuple of (status_dict or None, error_message)
    """
    try:
        status_result = run_ceph_command(
            ctx, ['ceph', 'status', '--format', 'json'], json_output=True
        )
    except RuntimeError as e:
        return None, f'Failed to get cluster status: {e}'

    if not isinstance(status_result, dict):
        return None, 'Invalid status response from cluster'

    return status_result, ''


def _check_pgs_from_status(status: Dict[str, Any]) -> Tuple[bool, int, int]:
    """
    Check PG status from a cluster status dict.

    Returns:
        Tuple of (all_clean, active_clean_count, total_pgs)
    """
    pgmap = status.get('pgmap', {})
    num_pgs = pgmap.get('num_pgs', 0)
    pgs_by_state = pgmap.get('pgs_by_state', [])

    if num_pgs == 0:
        return True, 0, 0

    active_clean = 0
    for state_info in pgs_by_state:
        state = state_info.get('state_name', '')
        count = state_info.get('count', 0)
        if state == 'active+clean':
            active_clean = count
            break

    return active_clean == num_pgs, active_clean, num_pgs


def check_cluster_health(ctx: CephadmContext) -> Tuple[bool, str]:
    """
    Check if cluster is healthy and all PGs are active+clean.

    Returns:
        Tuple of (is_healthy, message)
    """
    status, err = _get_cluster_status(ctx)
    if status is None:
        return False, err

    # Check health status
    health = status.get('health', {})
    health_status = health.get('status', 'HEALTH_ERR')
    if health_status != 'HEALTH_OK':
        return False, f'Cluster is not healthy: {health_status}'

    # Check PG status
    pgs_clean, active_clean, num_pgs = _check_pgs_from_status(status)
    if not pgs_clean:
        return (
            False,
            f'Not all PGs are active+clean: {active_clean}/{num_pgs}',
        )

    return True, 'Cluster is healthy and all PGs are active+clean'


def check_pgs_clean(ctx: CephadmContext) -> Tuple[bool, str]:
    """
    Check if all PGs are active+clean (ignores overall health status).

    Returns:
        Tuple of (all_clean, message)
    """
    status, err = _get_cluster_status(ctx)
    if status is None:
        return False, err

    pgs_clean, active_clean, num_pgs = _check_pgs_from_status(status)

    if num_pgs == 0:
        return True, 'No PGs in cluster'

    if pgs_clean:
        return True, f'All PGs are active+clean ({num_pgs}/{num_pgs})'

    return False, f'PGs not yet clean: {active_clean}/{num_pgs} active+clean'


def check_admin_keyring(ctx: CephadmContext) -> None:
    """
    Check that the admin keyring exists and is accessible.
    Raises Error if not found.
    """
    if find_admin_keyring(ctx) is not None:
        return

    raise Error(
        f'Admin keyring not found. Checked:\n'
        f'  - {ctx.data_dir}/{ctx.fsid}/{CEPH_CONF_DIR}/{CEPH_KEYRING}\n'
        f'  - {CEPH_DEFAULT_KEYRING}\n'
        f'Cannot execute ceph commands without admin keyring.'
    )


def _start_host(
    ctx: CephadmContext,
    hostname: str,
    target: str,
    current_host: str,
    ssh_key: Optional[str],
) -> Tuple[str, bool, str]:
    """
    Start a single host's Ceph services.

    Returns:
        Tuple of (hostname, success, error_message)
    """
    is_local = hostname == current_host

    # Skip if this is the local host and we're already running
    if is_local:
        out, _, code = call(
            ctx,
            ['systemctl', 'is-active', target],
            verbosity=CallVerbosity.QUIET,
        )
        if code == 0 and 'active' in out:
            logger.info(f'{hostname} (local) is already running')
            return (hostname, True, '')

    logger.info(f'Starting {hostname}...')

    _, err, code = remote_systemctl(
        ctx,
        hostname,
        'enable',
        target,
        is_local,
        ssh_key,
        timeout=HOST_OPERATION_TIMEOUT,
    )
    if code != 0:
        msg = f'Failed to enable target on {hostname}: {err}'
        logger.warning(msg)
        return (hostname, False, msg)

    _, err, code = remote_systemctl(
        ctx,
        hostname,
        'start',
        target,
        is_local,
        ssh_key,
        timeout=HOST_OPERATION_TIMEOUT,
    )
    if code != 0:
        msg = f'Failed to start target on {hostname}: {err}'
        logger.warning(msg)
        return (hostname, False, msg)

    logger.info(f'Started {hostname}')
    return (hostname, True, '')


def start_hosts_parallel(
    ctx: CephadmContext,
    hosts: List[str],
    target: str,
    current_host: str,
    ssh_key: Optional[str],
    max_parallel: int = DEFAULT_PARALLEL_HOSTS,
) -> List[str]:
    """
    Start multiple hosts in parallel.

    Args:
        ctx: CephadmContext
        hosts: List of hostnames to start
        target: Systemd target name
        current_host: The local hostname
        ssh_key: SSH key for remote operations
        max_parallel: Maximum number of hosts to start in parallel

    Returns:
        List of hostnames that failed to start
    """
    failed_hosts: List[str] = []

    if not hosts:
        return failed_hosts

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(
                _start_host, ctx, hostname, target, current_host, ssh_key
            ): hostname
            for hostname in hosts
        }

        for future in as_completed(futures):
            hostname = futures[future]
            try:
                _, success, _ = future.result()
                if not success:
                    failed_hosts.append(hostname)
            except CancelledError:
                logger.error(f'Operation cancelled for host {hostname}')
                failed_hosts.append(hostname)
            except Exception:
                logger.exception(f'Unexpected error starting host {hostname}')
                failed_hosts.append(hostname)

    return failed_hosts


# Shutdown helpers


def validate_shutdown_preconditions(ctx: CephadmContext) -> str:
    """Validate preconditions for shutdown. Returns health message."""
    if not ctx.fsid:
        raise Error('must pass --fsid to specify cluster')

    existing_state = load_cluster_state(ctx.fsid)
    if existing_state:
        state_file = get_cluster_state_file(ctx.fsid)
        shutdown_time = existing_state.get('timestamp', 'unknown')
        logger.warning('A shutdown state file already exists.')
        logger.warning(f'  State file: {state_file}')
        logger.warning(f'  Shutdown timestamp: {shutdown_time}')
        logger.warning('')
        logger.warning('This indicates the cluster was previously shut down.')
        logger.warning(
            'If this is expected, use "cephadm cluster-start" '
            'to start the cluster.'
        )
        logger.warning('')
        logger.warning(
            'If you believe the cluster is actually running '
            'and this file is stale,'
        )
        logger.warning('rename or remove the state file and retry:')
        logger.warning(f'  mv {state_file} {state_file}.bak')
        raise SystemExit(0)

    check_admin_keyring(ctx)

    if not ctx.dry_run and not ctx.yes_i_really_mean_it:
        raise Error(
            'This will shut down the entire cluster! '
            'Pass --yes-i-really-mean-it to proceed.'
        )

    logger.info('Checking cluster health...')
    is_healthy, health_msg = check_cluster_health(ctx)
    if not is_healthy:
        if ctx.force:
            logger.warning(f'Cluster health check failed: {health_msg}')
            logger.warning('Proceeding anyway due to --force flag')
        else:
            raise Error(f'Cluster health check failed: {health_msg}')
    else:
        logger.info(health_msg)

    return health_msg


def gather_shutdown_info(ctx: CephadmContext) -> Dict[str, Any]:
    """Gather cluster information for shutdown."""
    logger.info('Gathering cluster information from orchestrator...')
    hosts = get_orch_hosts(ctx)
    daemons = get_orch_daemons(ctx)

    if not hosts:
        raise Error('No hosts found in the cluster')

    admin_host = get_admin_host(hosts) or get_hostname()
    if not get_admin_host(hosts):
        logger.warning('No admin host found (no host with _admin label)')

    host_daemon_map = build_host_daemon_map(daemons)
    shutdown_order = order_hosts_for_shutdown(host_daemon_map, admin_host)

    logger.info(f'Admin host: {admin_host}')
    logger.info(f'Shutdown order: {shutdown_order}')

    return {
        'admin_host': admin_host,
        'host_daemon_map': host_daemon_map,
        'shutdown_order': shutdown_order,
        'daemons': daemons,
        'target': f'ceph-{ctx.fsid}.target',
    }


def print_shutdown_plan(
    ctx: CephadmContext,
    info: Dict[str, Any],
    health_msg: str,
    dry_run: bool,
) -> None:
    """Print the shutdown plan."""
    print_section_header('CLUSTER SHUTDOWN', dry_run)
    print(f'FSID: {ctx.fsid}')
    print(f'\nCluster health: {health_msg}')
    print(f'Admin host: {info["admin_host"]}')
    print_host_list(
        info['shutdown_order'],
        info['host_daemon_map'],
        info['admin_host'],
        'Shutdown order',
    )


def set_orchestrator_state(ctx: CephadmContext, action: str) -> None:
    """Pause or resume the cephadm orchestrator.

    Args:
        action: 'pause' or 'resume'
    """
    try:
        run_ceph_command(ctx, ['ceph', 'orch', action])
        logger.info(f'{action.capitalize()}d cephadm orchestrator')
    except RuntimeError as e:
        logger.warning(f'Failed to {action} orchestrator: {e}')


def prepare_for_shutdown(
    ctx: CephadmContext, info: Dict[str, Any], dry_run: bool
) -> Tuple[Optional[str], List[str]]:
    """Get SSH key, pause orch, set OSD flags."""
    ssh_key = None
    flags_set: List[str] = []

    if not dry_run:
        ssh_key = get_cephadm_ssh_key(ctx)
        if not ssh_key:
            raise Error(
                'Could not retrieve SSH key from cluster. Cannot proceed.'
            )

    # Pause orchestrator to prevent it from restarting daemons we stop
    action = '[DRY-RUN] Would pause' if dry_run else 'Pausing'
    print(f'{action} cephadm orchestrator...')
    if not dry_run:
        set_orchestrator_state(ctx, 'pause')

    flags_str = ', '.join(SHUTDOWN_OSD_FLAGS)
    action = '[DRY-RUN] Would set' if dry_run else 'Setting'
    print(f'{action} OSD safety flags: {flags_str}')
    if not dry_run:
        flags_set = set_osd_flags(ctx, SHUTDOWN_OSD_FLAGS, ctx.force)

    action = '[DRY-RUN] Would fail' if dry_run else 'Failing'
    print(f'{action} CephFS filesystems...')
    if not dry_run:
        fail_cephfs_filesystems(ctx)

    return ssh_key, flags_set


def stop_client_daemons(
    ctx: CephadmContext,
    info: Dict[str, Any],
    ssh_key: Optional[str],
    dry_run: bool,
) -> None:
    """Stop client-facing daemons before full shutdown."""
    daemons_to_stop = [
        d
        for d in DAEMONS_TO_STOP_FIRST
        if any(
            d in info['host_daemon_map'].get(h, [])
            for h in info['shutdown_order']
        )
    ]
    if daemons_to_stop:
        action = '[DRY-RUN] Would stop' if dry_run else 'Stopping'
        services = ', '.join(daemons_to_stop)
        print(f'{action} client-facing services: {services}...')
        if not dry_run:
            failed_daemons = stop_daemons_by_type(
                ctx, info['daemons'], DAEMONS_TO_STOP_FIRST, ssh_key
            )
            if failed_daemons:
                logger.warning(
                    f'Some daemons failed to stop: {failed_daemons}'
                )


def save_shutdown_state(
    ctx: CephadmContext, info: Dict[str, Any], flags_set: List[str]
) -> None:
    """Save shutdown state for later restart."""
    state = {
        'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
        'fsid': ctx.fsid,
        'admin_host': info['admin_host'],
        'shutdown_order': info['shutdown_order'],
        'hosts': info['host_daemon_map'],
        'flags_set': flags_set,
    }
    save_cluster_state(ctx.fsid, state)


def stop_all_hosts(
    ctx: CephadmContext,
    info: Dict[str, Any],
    ssh_key: Optional[str],
    dry_run: bool,
) -> List[str]:
    """Stop all hosts sequentially (admin last). Returns failed hosts."""
    action = '[DRY-RUN] Would stop' if dry_run else 'Stopping'
    host_count = len(info['shutdown_order'])
    print(f'{action} {host_count} hosts (admin host last)...')

    if dry_run:
        return []

    failed_hosts = []
    current_host = get_hostname()
    target = info['target']
    admin_host = info['admin_host']

    for hostname in info['shutdown_order']:
        is_local = hostname == current_host
        admin_suffix = ' (admin)' if hostname == admin_host else ''
        logger.info(f'Stopping {hostname}{admin_suffix}...')

        _, err, code = remote_systemctl(
            ctx,
            hostname,
            'disable',
            target,
            is_local,
            ssh_key,
            timeout=HOST_OPERATION_TIMEOUT,
        )
        if code != 0:
            logger.warning(f'Failed to disable target on {hostname}: {err}')
            if not ctx.force:
                failed_hosts.append(hostname)
                continue

        _, err, code = remote_systemctl(
            ctx,
            hostname,
            'stop',
            target,
            is_local,
            ssh_key,
            timeout=HOST_OPERATION_TIMEOUT,
        )
        if code != 0:
            logger.warning(f'Failed to stop target on {hostname}: {err}')
            if not ctx.force:
                failed_hosts.append(hostname)
                continue

        logger.info(f'Stopped {hostname}')

    return failed_hosts


def print_shutdown_complete(
    ctx: CephadmContext, host_count: int, dry_run: bool
) -> None:
    """Print shutdown completion message."""
    print_section_footer()
    if dry_run:
        print('DRY-RUN COMPLETE - No changes were made')
    else:
        print('CLUSTER SHUTDOWN COMPLETE')
        print(f'\nAll {host_count} hosts have been stopped.')
    print('\nTo restart the cluster later:')
    print(f'  cephadm cluster-start --fsid {ctx.fsid}')
    print_section_footer()


# Startup helpers


def load_startup_info(ctx: CephadmContext) -> Optional[Dict[str, Any]]:
    """Load saved state and build startup info. Returns None if no state."""
    if not ctx.fsid:
        raise Error('must pass --fsid to specify cluster')

    state = load_cluster_state(ctx.fsid)
    if not state:
        logger.info('No shutdown state file found - nothing to start.')
        return None

    admin_host = state.get('admin_host', get_hostname())
    flags_set = state.get('flags_set', [])
    host_daemon_map = state.get('hosts', {})

    all_hosts = list(host_daemon_map.keys())
    startup_order = [admin_host] if admin_host in all_hosts else []
    startup_order.extend(h for h in all_hosts if h != admin_host)

    logger.info(f'Admin host: {admin_host}')
    logger.info(f'Hosts to start: {startup_order}')

    return {
        'admin_host': admin_host,
        'flags_set': flags_set,
        'host_daemon_map': host_daemon_map,
        'startup_order': startup_order,
        'target': f'ceph-{ctx.fsid}.target',
    }


def print_startup_plan(ctx: CephadmContext, info: Dict[str, Any]) -> None:
    """Print the startup plan for dry-run."""
    print_section_header('CLUSTER STARTUP', dry_run=True)
    print(f'FSID: {ctx.fsid}')
    print(f'\nAdmin host: {info["admin_host"]}')
    print_host_list(
        info['startup_order'],
        info['host_daemon_map'],
        info['admin_host'],
        'Hosts to start',
    )
    print(f'\nFlags to unset: {info["flags_set"]}')
    print_section_footer()


def start_all_hosts(
    ctx: CephadmContext, info: Dict[str, Any], ssh_key: Optional[str]
) -> List[str]:
    """Start all hosts (admin first, then parallel). Returns failed hosts."""
    target = info['target']
    admin_host = info['admin_host']
    current_host = get_hostname()
    max_parallel = getattr(ctx, 'parallel', DEFAULT_PARALLEL_HOSTS)
    non_admin_hosts = [h for h in info['startup_order'] if h != admin_host]

    logger.info(
        f'Starting {len(info["startup_order"])} hosts '
        f'(max {max_parallel} in parallel, admin first)...'
    )

    failed_hosts = []

    if admin_host:
        logger.info(f'Starting admin host {admin_host} first...')
        admin_failed = start_hosts_parallel(
            ctx, [admin_host], target, current_host, ssh_key, max_parallel=1
        )
        failed_hosts.extend(admin_failed)

    if non_admin_hosts:
        other_failed = start_hosts_parallel(
            ctx, non_admin_hosts, target, current_host, ssh_key, max_parallel
        )
        failed_hosts.extend(other_failed)

    if failed_hosts:
        logger.error(f'Failed to start hosts: {failed_hosts}')

    return failed_hosts


def wait_for_cluster_ready(ctx: CephadmContext) -> bool:
    """Wait for cluster to be accessible and PGs clean."""
    logger.info('Waiting for cluster to become accessible...')
    if not wait_for_cluster_accessible(ctx):
        logger.error(
            'Cluster is not responding. Cannot proceed with startup.'
        )
        logger.error('Please check cluster status manually.')
        return False

    logger.info('Waiting for all PGs to be active+clean...')
    if not wait_for_pgs_clean(ctx):
        logger.warning('PGs did not become clean within timeout.')
        logger.warning('Proceeding with flag removal anyway.')

    return True


def restore_cluster_services(
    ctx: CephadmContext, flags_set: List[str]
) -> None:
    """Unset safety flags, restore CephFS, and resume orchestrator."""
    logger.info('Unsetting cluster safety flags...')
    unset_osd_flags(ctx, flags_set)

    logger.info('Setting CephFS filesystems to joinable...')
    set_cephfs_joinable(ctx)

    logger.info('Resuming cephadm orchestrator...')
    set_orchestrator_state(ctx, 'resume')

    logger.info('Checking cluster health...')
    is_healthy, health_msg = check_cluster_health(ctx)
    if is_healthy:
        logger.info(health_msg)
    else:
        logger.warning(f'Cluster health: {health_msg}')
        logger.warning('Check cluster status manually: ceph -s')


def cleanup_startup(ctx: CephadmContext) -> None:
    """Clean up state file and SSH key after startup."""
    remove_cluster_state(ctx.fsid)
    remove_cached_ssh_key(ctx.fsid)


def print_startup_complete(failed_hosts: List[str]) -> None:
    """Print startup completion message."""
    print_section_header('CLUSTER STARTUP COMPLETE')
    if failed_hosts:
        print(f'\nWarning: Some hosts failed to start: {failed_hosts}')
    print('\nMonitor cluster health with: ceph -s')
    print_section_footer()


# Status helpers


def print_cluster_status_shutdown(
    ctx: CephadmContext,
    state: Dict[str, Any],
    current_host: str,
) -> None:
    """Print status for a cluster in shutdown state."""
    fsid = ctx.fsid
    print('Cluster state: SHUTDOWN')
    print(f'State file: {get_cluster_state_file(fsid)}')
    print(f'Shutdown time: {state.get("timestamp", "unknown")}')
    print(f'Admin host: {state.get("admin_host", "unknown")}')

    flags_set = state.get('flags_set', [])
    if flags_set:
        print(f'Flags set: {", ".join(flags_set)}')

    hosts = state.get('hosts', {})
    if hosts:
        print(f'\nHosts ({len(hosts)}):')

        ssh_key = get_cephadm_ssh_key(ctx)
        target = f'ceph-{fsid}.target'

        for hostname, daemon_types in hosts.items():
            is_local = hostname == current_host
            _, _, code = remote_systemctl(
                ctx, hostname, 'is-active', target, is_local, ssh_key
            )
            status = 'RUNNING' if code == 0 else 'STOPPED'

            is_admin = hostname == state.get('admin_host')
            admin_marker = ' (admin)' if is_admin else ''
            print(f'  {hostname}{admin_marker}: {status}')
            if daemon_types:
                print(f'    Daemons: {", ".join(sorted(daemon_types))}')

    print(f'\nTo start the cluster: cephadm cluster-start --fsid {fsid}')


def print_cluster_status_running(ctx: CephadmContext) -> int:
    """Print status for a running cluster. Returns 0 on success, 1 on err."""
    print('Cluster state: RUNNING (no shutdown state file found)')

    try:
        check_admin_keyring(ctx)
    except Error as e:
        print(f'\n{e}')
        return 1

    try:
        hosts_result = run_ceph_command(
            ctx,
            ['ceph', 'orch', 'host', 'ls', '--format', 'json'],
            json_output=True,
        )
        if isinstance(hosts_result, list):
            print(f'\nHosts ({len(hosts_result)}):')
            for host in hosts_result:
                hostname = host.get('hostname', 'unknown')
                labels = host.get('labels', [])
                status = host.get('status', '')
                admin_marker = ' (admin)' if ADMIN_LABEL in labels else ''
                print(f'  {hostname}{admin_marker}: {status or "OK"}')

        health_result = run_ceph_command(ctx, ['ceph', 'health'])
        print(f'\nCluster health: {health_result.strip()}')

    except RuntimeError as e:
        print(f'\nCannot query cluster: {e}')
        print('Cluster may be starting up or not accessible.')

    return 0
