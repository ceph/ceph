import asyncio
import json
import logging
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

# Per-host cephadm pull. default_cephadm_command_timeout (15m) is too
# short for multi-GB images. This value is passed as --timeout on the
# pull itself, not only on the mgr wait_async wrapper.
UPGRADE_IMAGE_PRE_PULL_MIN_TIMEOUT_SEC = 7200


class UpgradeImagePrePullMethod(str, Enum):
    NONE = 'none'
    REGISTRY = 'registry'

    @classmethod
    def from_config(cls, raw: Optional[str]) -> 'UpgradeImagePrePullMethod':
        """Normalize config to a method. Empty string is a default, not a method."""
        value = (raw or '').strip().lower()
        if not value:
            return cls.NONE
        return cls(value)


class PrePullBatchResult(str, Enum):
    COMPLETE = 'complete'
    IN_PROGRESS = 'in_progress'
    FAILED = 'failed'


def _bare_sha256(value: str) -> str:
    return value.replace(SHA256_PREFIX, '')


class UpgradeImagePrePull:
    """Registry pre-pull of the upgrade target image before daemon upgrades."""

    def __init__(self, upgrade: 'CephadmUpgrade') -> None:
        self.upgrade = upgrade
        self.mgr: 'CephadmOrchestrator' = upgrade.mgr

    def parse_method_or_fail(self) -> Optional[UpgradeImagePrePullMethod]:
        raw = getattr(self.mgr, 'upgrade_prepull_method', '') or ''
        try:
            return UpgradeImagePrePullMethod.from_config(raw)
        except ValueError:
            self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                'severity': 'error',
                'summary': 'Upgrade: invalid upgrade_prepull_method',
                'count': 1,
                'detail': [
                    f'unknown upgrade_prepull_method {str(raw).strip().lower()!r}; '
                    f'expected empty/{UpgradeImagePrePullMethod.NONE.value!r} '
                    f'(disabled) or {UpgradeImagePrePullMethod.REGISTRY.value!r}',
                ],
            })
            return None

    def get_upgrade_scope_hosts(self, daemons: List[DaemonDescription]) -> List[str]:
        hosts = sorted({d.hostname for d in daemons if d.hostname})
        return [h for h in hosts if h not in self.mgr.offline_hosts]

    def pull_timeout_sec(self) -> int:
        return max(
            UPGRADE_IMAGE_PRE_PULL_MIN_TIMEOUT_SEC,
            self.mgr.default_cephadm_command_timeout,
            1800,
        )

    def done_hosts(self) -> List[str]:
        assert self.upgrade.upgrade_state is not None
        return list(self.upgrade.upgrade_state.target_image_pre_pull_hosts or [])

    def all_hosts_done(self, hosts: List[str]) -> bool:
        return set(hosts).issubset(set(self.done_hosts()))

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

    def pre_pull_next_batch(
        self,
        target_image: str,
        target_digests: List[str],
        hosts: List[str],
    ) -> PrePullBatchResult:
        """Pull at most max_parallel remaining hosts, then return to the serve loop."""
        if not hosts or self.all_hosts_done(hosts):
            return PrePullBatchResult.COMPLETE

        origin_id = (
            self.upgrade.upgrade_state.progress_id
            if self.upgrade.upgrade_state else None
        )
        timeout = self.pull_timeout_sec()
        # Give cephadm --timeout a chance to fire before wait_async.
        wait_timeout = timeout + 60
        logger.info(
            'Upgrade: registry pre-pull using %d second cephadm pull timeout',
            timeout)
        try:
            with self.mgr.async_timeout_handler(
                    cmd='cephadm pull (upgrade pre-pull)', timeout=wait_timeout):
                return self.mgr.wait_async(
                    self._pre_pull_next_batch_async(
                        target_image, target_digests, hosts, timeout),
                    timeout=wait_timeout)
        except OrchestratorError as e:
            if not self._is_same_upgrade(origin_id):
                return PrePullBatchResult.IN_PROGRESS
            remaining = [h for h in hosts if h not in self.done_hosts()]
            batch = remaining[:max(1, self.mgr.upgrade_prepull_max_parallel)]
            self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                'severity': 'warning',
                'summary': 'Upgrade: failed to pre-pull target image',
                'count': 1,
                'detail': [
                    f'pre-pull timed out on host(s) {", ".join(batch)}: {e}',
                ],
            })
            return PrePullBatchResult.FAILED

    async def _pre_pull_next_batch_async(
        self,
        target_image: str,
        target_digests: List[str],
        hosts: List[str],
        pull_timeout: int,
    ) -> PrePullBatchResult:
        assert self.upgrade.upgrade_state is not None
        origin_id = self.upgrade.upgrade_state.progress_id
        remaining = [h for h in hosts if h not in self.done_hosts()]
        if not remaining:
            return PrePullBatchResult.COMPLETE

        max_parallel = max(1, self.mgr.upgrade_prepull_max_parallel)
        batch = remaining[:max_parallel]
        logger.info(
            'Upgrade: pre-pulling image %s on %d host(s) this serve iteration '
            '(%d/%d already done)',
            target_image, len(batch), len(hosts) - len(remaining), len(hosts))
        self.upgrade.upgrade_info_str = (
            f'Pre-pulling upgrade image on {len(hosts) - len(remaining) + len(batch)}/'
            f'{len(hosts)} host(s)')

        pullargs: List[str] = []
        if self.mgr.registry_insecure:
            pullargs.append('--insecure')

        results: Dict[str, Tuple[int, Optional[Dict[str, Any]], str]] = {}

        async def _pull_on_host(host: str) -> None:
            if not self._is_same_upgrade(origin_id) or self.upgrade.upgrade_state.paused:
                return
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

        await asyncio.gather(*[_pull_on_host(host) for host in batch])

        if not self._is_same_upgrade(origin_id):
            return PrePullBatchResult.IN_PROGRESS
        assert self.upgrade.upgrade_state is not None
        if self.upgrade.upgrade_state.paused:
            return PrePullBatchResult.IN_PROGRESS

        failed_hosts: List[Tuple[str, str]] = []
        for host in batch:
            if host not in results:
                failed_hosts.append((host, 'pre-pull returned no result'))
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
