import asyncio
import json
import logging
from concurrent.futures import Future
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from cephadm.serve import CephadmServe
from orchestrator import DaemonDescription, OrchestratorError

if TYPE_CHECKING:
    from .module import CephadmOrchestrator
    from .upgrade import CephadmUpgrade

logger = logging.getLogger(__name__)

SHA256_PREFIX = 'sha256:'
SHA256_REPO_DIGEST_SEPARATOR = '@sha256:'


class PrePullBatchResult(str, Enum):
    COMPLETE = 'complete'
    IN_PROGRESS = 'in_progress'
    FAILED = 'failed'


def _bare_sha256(value: str) -> str:
    return value.replace(SHA256_PREFIX, '')


class UpgradeImagePrePull:
    """Registry pre-pull of the upgrade target image before daemon upgrades.

    All pulls run as background tasks on the cephadm event loop. The serve
    thread only starts them and polls for results on later serve
    iterations, so a slow pull never blocks the serve loop and ``ceph orch
    upgrade pause/stop`` keep working while pulls are in flight. All
    upgrade-state mutation (marking hosts done, failing the upgrade)
    happens on the serve thread when results are harvested; the background
    coroutines only collect per-host results.
    """

    def __init__(self, upgrade: 'CephadmUpgrade') -> None:
        self.upgrade = upgrade
        self.mgr: 'CephadmOrchestrator' = upgrade.mgr
        # Background task handles. They do not survive an mgr restart; on
        # restart the remaining hosts are simply pulled again (a re-pull of
        # an already present image is cheap).
        self._first_pull_future: Optional[Future] = None
        self._first_pull_origin: Optional[Tuple[Optional[str], str]] = None
        self.first_pull_host: Optional[str] = None
        self._batch_future: Optional[Future] = None
        self._batch_origin: Optional[Tuple[Optional[str], str]] = None
        self._batch_hosts: List[str] = []

    def enabled(self) -> bool:
        return bool(getattr(self.mgr, 'upgrade_prepull_image', False))

    def get_upgrade_scope_hosts(self, daemons: List[DaemonDescription]) -> List[str]:
        hosts = sorted({d.hostname for d in daemons if d.hostname})
        return [h for h in hosts if h not in self.mgr.offline_hosts]

    def pull_timeout_sec(self) -> int:
        """cephadm pull timeout (passed as --timeout to cephadm pull).

        default_cephadm_command_timeout is too short for multi-GB images.
        Callers must use this only when pre-pull is enabled.
        """
        return max(
            self.mgr.upgrade_prepull_timeout,
            self.mgr.default_cephadm_command_timeout,
        )

    def wait_timeout_sec(self) -> int:
        """Bounds the background pull coroutines: gives the cephadm
        --timeout a chance to fire before the mgr-side wait times out."""
        return self.pull_timeout_sec() + 60

    def done_hosts(self) -> List[str]:
        assert self.upgrade.upgrade_state is not None
        return list(self.upgrade.upgrade_state.target_image_pre_pull_hosts or [])

    def all_hosts_done(self, hosts: List[str]) -> bool:
        return set(hosts).issubset(set(self.done_hosts()))

    def _origin(self) -> Optional[str]:
        st = self.upgrade.upgrade_state
        return st.progress_id if st else None

    def _is_same_upgrade(self, progress_id: Optional[str]) -> bool:
        st = self.upgrade.upgrade_state
        return bool(progress_id and st and st.progress_id == progress_id)

    def mark_host_done(self, host: Optional[str]) -> None:
        if not host or not self.upgrade.upgrade_state:
            return
        done = self.done_hosts()
        if host not in done:
            done.append(host)
            self.upgrade.upgrade_state.target_image_pre_pull_hosts = done
            self.upgrade._save_upgrade_state()

    def _inspect_info_matches_target_image(
        self,
        inspect_info: Optional[Dict[str, Any]],
        target_digests: List[str],
    ) -> bool:
        if not inspect_info:
            return False
        repo_digests = inspect_info.get('repo_digests', []) or []
        target_id = (
            self.upgrade.upgrade_state.target_id
            if self.upgrade.upgrade_state else None
        )
        image_id = inspect_info.get('image_id')
        if target_id and image_id:
            if _bare_sha256(image_id) == _bare_sha256(target_id):
                return True
        if any(d in target_digests for d in repo_digests):
            return True
        target_shas = {
            d.split(SHA256_REPO_DIGEST_SEPARATOR, 1)[1]
            for d in target_digests
            if SHA256_REPO_DIGEST_SEPARATOR in d
        }
        for digest in repo_digests:
            if (
                SHA256_REPO_DIGEST_SEPARATOR in digest
                and digest.split(SHA256_REPO_DIGEST_SEPARATOR, 1)[1] in target_shas
            ):
                return True
        return False

    async def _registry_login_if_needed(self, host: str) -> None:
        if self.mgr.cache.host_needs_registry_login(host) and self.mgr.registry_url:
            creds = self.mgr.get_store('registry_credentials')
            if creds:
                await CephadmServe(self.mgr)._registry_login(host, json.loads(str(creds)))

    def _parse_pull_image_info(self, out: List[str]) -> Optional[Dict[str, Any]]:
        try:
            info = json.loads(''.join(out))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(info, dict):
            return None
        return info

    # ---- first (metadata) pull -----------------------------------------

    def first_pull_start_or_poll(self, target_image: str) -> Optional[Tuple[str, Any]]:
        """Run the first pull of target_image in the background.

        This is the metadata source (digest, ceph version) for the
        cluster-wide pre-pull. Uses the long pre-pull timeout without
        blocking the serve loop: the pull runs as a background task and is
        polled on later serve iterations.

        Returns:
          None -- pull in flight (or just started)
          ('ok', (target_id, ceph_version, repo_digests)) -- completed
          ('error', Exception) -- failed
        """
        origin = self._origin()
        if (self._first_pull_future is None
                or self._first_pull_origin != (origin, target_image)):
            if (self._first_pull_future is not None
                    and not self._first_pull_future.done()):
                # abandoned pull from another upgrade/image still running;
                # ignore it and start ours on a later iteration
                return None
            self._first_pull_future = None
            self.first_pull_host = self.mgr._any_inventory_host()
            self._first_pull_origin = (origin, target_image)
            timeout = self.pull_timeout_sec()
            logger.info(
                'Upgrade: registry pre-pull first pull using %d second '
                'cephadm pull timeout', timeout)
            self._first_pull_future = self.mgr.event_loop.spawn(
                self._first_pull_async(target_image, timeout))
            return None
        if not self._first_pull_future.done():
            return None
        future, self._first_pull_future = self._first_pull_future, None
        try:
            return ('ok', future.result(timeout=0))
        except Exception as e:
            return ('error', e)

    async def _first_pull_async(self, target_image: str, timeout: int) -> Any:
        # wait_for bounds mgr-side hangs (e.g. ssh) without blocking serve
        try:
            return await asyncio.wait_for(
                CephadmServe(self.mgr)._get_container_image_info(
                    target_image, timeout=timeout),
                self.wait_timeout_sec())
        except asyncio.TimeoutError:
            raise OrchestratorError(
                f'first pull timed out after {self.wait_timeout_sec()} seconds')

    # ---- parallel per-host batches --------------------------------------

    def pre_pull_next_batch(
        self,
        target_image: str,
        target_digests: List[str],
        hosts: List[str],
    ) -> PrePullBatchResult:
        """Pull the next batch of hosts in the background, poll the running one.

        Never blocks the serve loop: the batch runs as a background task
        bounded by upgrade_prepull_max_parallel, and results are applied on
        the serve thread on a later iteration.
        """
        if not self.upgrade.upgrade_state:
            self._batch_future = None
            return PrePullBatchResult.IN_PROGRESS
        if not hosts or self.all_hosts_done(hosts):
            self._batch_future = None
            return PrePullBatchResult.COMPLETE

        origin = self._origin()
        if (self._batch_future is None
                or self._batch_origin != (origin, target_image)):
            if (self._batch_future is not None
                    and not self._batch_future.done()):
                # abandoned batch from another upgrade/image still running;
                # ignore it and start ours on a later iteration
                return PrePullBatchResult.IN_PROGRESS
            self._batch_future = None
            remaining = [h for h in hosts if h not in self.done_hosts()]
            max_parallel = max(1, self.mgr.upgrade_prepull_max_parallel)
            batch = remaining[:max_parallel]
            self._batch_origin = (origin, target_image)
            self._batch_hosts = list(batch)
            logger.info(
                'Upgrade: registry pre-pull using %d second cephadm pull timeout',
                self.pull_timeout_sec())
            logger.info(
                'Upgrade: pre-pulling image %s on %d host(s) this serve iteration '
                '(%d/%d already done)',
                target_image, len(batch), len(hosts) - len(remaining), len(hosts))
            self.upgrade.upgrade_info_str = (
                f'Pre-pulling upgrade image on {len(hosts) - len(remaining) + len(batch)}/'
                f'{len(hosts)} host(s)')
            self._batch_future = self.mgr.event_loop.spawn(
                self._pre_pull_batch_async(target_image, batch, self.pull_timeout_sec()))
            return PrePullBatchResult.IN_PROGRESS

        if not self._batch_future.done():
            return PrePullBatchResult.IN_PROGRESS

        # harvest the results on the serve thread
        future, self._batch_future = self._batch_future, None
        batch = list(self._batch_hosts)
        try:
            results, timed_out = future.result(timeout=0)
        except Exception as e:
            results = {h: (1, None, str(e)) for h in batch}
            timed_out = False

        # the upgrade this batch belongs to may be gone or paused by now;
        # discard the results in that case (after a resume the hosts are
        # simply pulled again, which is cheap for present images)
        if not self._is_same_upgrade(origin):
            return PrePullBatchResult.IN_PROGRESS
        assert self.upgrade.upgrade_state is not None
        if self.upgrade.upgrade_state.paused:
            return PrePullBatchResult.IN_PROGRESS

        failed_hosts: List[Tuple[str, str]] = []
        for host in batch:
            if host not in results:
                if timed_out:
                    reason = f'timed out after {self.wait_timeout_sec()} seconds'
                else:
                    reason = 'pre-pull returned no result'
                failed_hosts.append((host, reason))
                continue
            code, info, reason = results[host]
            if code:
                failed_hosts.append((host, reason))
                continue
            if target_digests and not self._inspect_info_matches_target_image(
                    info, target_digests):
                if self.mgr.use_repo_digest:
                    got = info.get('repo_digests') if info else None
                    failed_hosts.append((
                        host,
                        f'pull returned mismatched digests (got {got}, '
                        f'expected {target_digests})',
                    ))
                    continue
                logger.warning(
                    'Upgrade: host %s pull digests %s did not match %s; '
                    'continuing because use_repo_digest is false',
                    host,
                    info.get('repo_digests') if info else None,
                    target_digests)
            self.mark_host_done(host)

        if failed_hosts:
            detail = [
                f'failed to pull image on {host}: {reason}'
                for host, reason in failed_hosts
            ]
            self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                'severity': 'warning',
                'summary': 'Upgrade: failed to pre-pull target image',
                'count': len(failed_hosts),
                'detail': detail,
            })
            return PrePullBatchResult.FAILED

        if self.all_hosts_done(hosts):
            logger.info('Upgrade: registry pre-pull complete')
            return PrePullBatchResult.COMPLETE
        return PrePullBatchResult.IN_PROGRESS

    async def _pre_pull_batch_async(
        self,
        target_image: str,
        batch: List[str],
        pull_timeout: int,
    ) -> Tuple[Dict[str, Tuple[int, Optional[Dict[str, Any]], str]], bool]:
        """Pull target_image on every host in batch, in parallel.

        Collects per-host results only; the caller (serve thread) applies
        them to the upgrade state. Returns (results, timed_out); hosts
        missing from results did not finish before the mgr-side timeout.
        """
        results: Dict[str, Tuple[int, Optional[Dict[str, Any]], str]] = {}

        pullargs: List[str] = []
        if self.mgr.registry_insecure:
            pullargs.append('--insecure')

        async def _pull_on_host(host: str) -> None:
            try:
                await self._registry_login_if_needed(host)
                logger.info('Upgrade: pulling image %s on host %s', target_image, host)
                out, errs, code = await CephadmServe(self.mgr)._run_cephadm(
                    host, '', 'pull', pullargs,
                    image=target_image, no_fsid=True, error_ok=True,
                    timeout=pull_timeout)
                if code:
                    reason = ''.join(errs) if errs else 'unknown error'
                    logger.error('Upgrade: failed to pull image on host %s', host)
                    results[host] = (code, None, reason)
                    return
                info = self._parse_pull_image_info(out)
                if not info:
                    results[host] = (1, None, 'invalid or empty pull JSON')
                    return
                results[host] = (0, info, '')
                logger.info('Upgrade: pulled image on host %s', host)
            except Exception as e:
                results[host] = (1, None, str(e))

        try:
            await asyncio.wait_for(
                asyncio.gather(*[_pull_on_host(host) for host in batch]),
                self.wait_timeout_sec())
        except asyncio.TimeoutError:
            logger.error(
                'Upgrade: registry pre-pull batch timed out after %d seconds',
                self.wait_timeout_sec())
            return results, True
        return results, False
