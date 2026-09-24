import asyncio
import json
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from cephadm.serve import CephadmServe
from orchestrator import DaemonDescription

if TYPE_CHECKING:
    from .module import CephadmOrchestrator
    from .upgrade import CephadmUpgrade

logger = logging.getLogger(__name__)

SHA256_PREFIX = 'sha256:'
SHA256_REPO_DIGEST_SEPARATOR = '@sha256:'

# Parallel registry pre-pull across many hosts. Single-command
# default_cephadm_command_timeout (15m) is too short for multi-GB images.
UPGRADE_IMAGE_PRE_PULL_MIN_TIMEOUT_SEC = 7200


class UpgradeImageMirrorMethod(str, Enum):
    NONE = 'none'
    REGISTRY = 'registry'

    @classmethod
    def from_config(cls, raw: Optional[str]) -> 'UpgradeImageMirrorMethod':
        """Normalize config to a method. Empty string is a default, not a method."""
        value = (raw or '').strip().lower()
        if not value:
            return cls.NONE
        return cls(value)


def _bare_sha256(value: str) -> str:
    return value.replace(SHA256_PREFIX, '')


class UpgradeImagePrePull:
    """Registry pre-pull of the upgrade target image before daemon upgrades."""

    def __init__(self, upgrade: 'CephadmUpgrade') -> None:
        self.upgrade = upgrade
        self.mgr: 'CephadmOrchestrator' = upgrade.mgr

    def get_method(self) -> UpgradeImageMirrorMethod:
        raw = getattr(self.mgr, 'upgrade_image_mirror_method', '') or ''
        return UpgradeImageMirrorMethod.from_config(raw)

    def parse_method_or_fail(self) -> Optional[UpgradeImageMirrorMethod]:
        raw = getattr(self.mgr, 'upgrade_image_mirror_method', '') or ''
        try:
            return UpgradeImageMirrorMethod.from_config(raw)
        except ValueError:
            self.pre_distribute('', [], [])
            return None

    def is_enabled(self) -> bool:
        try:
            return self.get_method() == UpgradeImageMirrorMethod.REGISTRY
        except ValueError:
            return False

    def get_upgrade_scope_hosts(self, daemons: List[DaemonDescription]) -> List[str]:
        hosts = sorted({d.hostname for d in daemons if d.hostname})
        return [h for h in hosts if h not in self.mgr.offline_hosts]

    def operation_timeout_sec(self, host_count: int) -> int:
        per_host = max(self.mgr.default_cephadm_command_timeout, 1800)
        return max(UPGRADE_IMAGE_PRE_PULL_MIN_TIMEOUT_SEC, per_host * max(host_count, 1))

    def pre_distribute(
        self,
        target_image: str,
        target_digests: List[str],
        hosts: List[str],
    ) -> bool:
        raw = getattr(self.mgr, 'upgrade_image_mirror_method', '') or ''
        try:
            method = UpgradeImageMirrorMethod.from_config(raw)
        except ValueError:
            self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                'severity': 'error',
                'summary': 'Upgrade: invalid upgrade_image_mirror_method',
                'count': 1,
                'detail': [
                    f'unknown upgrade_image_mirror_method {str(raw).strip().lower()!r}; '
                    f'expected empty/{UpgradeImageMirrorMethod.NONE.value!r} '
                    f'(disabled) or {UpgradeImageMirrorMethod.REGISTRY.value!r}',
                ],
            })
            return False
        if method == UpgradeImageMirrorMethod.NONE:
            return True
        if method == UpgradeImageMirrorMethod.REGISTRY:
            return self.pre_pull_image_on_hosts(
                target_image, target_digests, hosts)
        self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
            'severity': 'error',
            'summary': 'Upgrade: invalid upgrade_image_mirror_method',
            'count': 1,
            'detail': [
                f'unknown upgrade_image_mirror_method {method.value!r}; '
                f'expected empty/{UpgradeImageMirrorMethod.NONE.value!r} '
                f'(disabled) or {UpgradeImageMirrorMethod.REGISTRY.value!r}',
            ],
        })
        return False

    def pre_pull_image_on_hosts(
        self,
        target_image: str,
        target_digests: List[str],
        hosts: List[str],
    ) -> bool:
        timeout = self.operation_timeout_sec(len(hosts))
        logger.info(
            'Upgrade: registry pre-pull timeout set to %d seconds for %d host(s)',
            timeout, len(hosts))
        return self.mgr.wait_async(
            self.pre_pull_image_on_hosts_async(
                target_image, target_digests, hosts),
            timeout=timeout)

    async def _inspect_image_on_host(
        self,
        host: str,
        target_image: str,
    ) -> Optional[Dict[str, Any]]:
        out, _, code = await CephadmServe(self.mgr)._run_cephadm(
            host, '', 'inspect-image', [],
            image=target_image, no_fsid=True, error_ok=True)
        if code:
            return None
        try:
            return json.loads(''.join(out))
        except json.JSONDecodeError:
            return None

    def _get_target_image_inspect_refs(self, target_image: str) -> List[str]:
        refs: List[str] = []
        assert self.upgrade.upgrade_state is not None
        for candidate in (
            target_image,
            self.upgrade.upgrade_state._target_name,
        ):
            if candidate and candidate not in refs:
                refs.append(candidate)
        target_id = self.upgrade.upgrade_state.target_id
        if target_id:
            bare_id = _bare_sha256(target_id)
            for id_ref in (bare_id, f'{SHA256_PREFIX}{bare_id}'):
                if id_ref not in refs:
                    refs.append(id_ref)
        return refs

    async def _host_has_target_image_on_host(
        self,
        host: str,
        target_image: str,
        target_digests: List[str],
    ) -> bool:
        for ref in self._get_target_image_inspect_refs(target_image):
            inspect_info = await self._inspect_image_on_host(host, ref)
            if self._inspect_info_matches_target_image(inspect_info, target_digests):
                return True
        return False

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

    def _ceph_version_token_from_pull(self, ceph_version: str) -> Optional[str]:
        """Normalize ``ceph --version`` output to the short token stored in upgrade state."""
        if not ceph_version:
            return None
        if ceph_version.startswith('ceph version '):
            parts = ceph_version.split(' ')
            if len(parts) >= 3:
                return parts[2]
            return None
        return ceph_version

    def _set_target_from_pull_info(self, info: Dict[str, Any]) -> Optional[str]:
        """
        Persist target_id / digests / version from cephadm pull/inspect JSON.

        Returns an error string on failure, or None on success.
        """
        assert self.upgrade.upgrade_state is not None
        image_id = info.get('image_id')
        digests = info.get('repo_digests') or []
        ceph_version = info.get('ceph_version') or ''
        if not image_id or not digests:
            return 'pull did not return image_id and repo_digests'
        version_token = self._ceph_version_token_from_pull(str(ceph_version))
        if not version_token:
            return 'unable to extract ceph version from container'
        self.upgrade.upgrade_state.target_id = str(image_id)
        self.upgrade.upgrade_state.target_digests = list(digests)
        self.upgrade.upgrade_state.target_version = version_token
        self.upgrade._save_upgrade_state()
        return None

    def _mark_pre_pull_done(self) -> None:
        assert self.upgrade.upgrade_state is not None
        self.upgrade.upgrade_state.target_image_pre_pull_done = True
        self.upgrade._save_upgrade_state()

    async def _pre_pull_image_discover_async(
        self,
        target_image: str,
        hosts: List[str],
    ) -> bool:
        """
        Parallel registry pull that also learns target digests/version.

        Used when upgrade_image_mirror_method=registry and upgrade state does not
        yet have target metadata, so we avoid a serial "First pull" on one host
        before pulling everywhere else.
        """
        assert self.upgrade.upgrade_state is not None
        max_parallel = max(1, self.mgr.upgrade_image_mirror_max_parallel)
        logger.info(
            'Upgrade: discovering and pre-pulling image %s on %d host(s), '
            'up to %d in parallel',
            target_image, len(hosts), max_parallel)
        self.upgrade.upgrade_info_str = (
            f'Discovering and pre-pulling upgrade image on {len(hosts)} host(s)')

        pullargs: List[str] = []
        if self.mgr.registry_insecure:
            pullargs.append('--insecure')

        sem = asyncio.Semaphore(max_parallel)
        results: Dict[str, Tuple[int, Optional[Dict[str, Any]], str]] = {}

        async def _pull_on_host(host: str) -> None:
            async with sem:
                if not self.upgrade.upgrade_state or self.upgrade.upgrade_state.paused:
                    return
                try:
                    await self._registry_login_if_needed(host)
                    logger.info('Upgrade: pulling image %s on host %s', target_image, host)
                    out, errs, code = await CephadmServe(self.mgr)._run_cephadm(
                        host, '', 'pull', pullargs,
                        image=target_image, no_fsid=True, error_ok=True)
                    if code:
                        reason = ''.join(errs) if errs else 'unknown error'
                        logger.error('Upgrade: failed to pull image on host %s', host)
                        results[host] = (code, None, reason)
                    else:
                        info = self._parse_pull_image_info(out)
                        if not info:
                            results[host] = (1, None, 'invalid or empty pull JSON')
                            logger.error(
                                'Upgrade: invalid pull JSON on host %s', host)
                        else:
                            results[host] = (0, info, '')
                            logger.info('Upgrade: pulled image on host %s', host)
                except Exception as e:
                    results[host] = (1, None, str(e))

        await asyncio.gather(*[_pull_on_host(host) for host in hosts])

        failed_hosts: List[Tuple[str, str]] = []
        for host in hosts:
            if host not in results:
                failed_hosts.append((host, 'pre-pull interrupted or skipped'))
                continue
            code, _info, reason = results[host]
            if code:
                failed_hosts.append((host, reason))

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
            return False

        reference: Optional[Dict[str, Any]] = None
        for host in hosts:
            _code, info, _reason = results[host]
            if not info:
                continue
            image_id = info.get('image_id')
            digests = info.get('repo_digests') or []
            version_token = self._ceph_version_token_from_pull(
                str(info.get('ceph_version') or ''))
            if image_id and digests and version_token:
                reference = info
                break

        if not reference:
            self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                'severity': 'warning',
                'summary': 'Upgrade: failed to pull target image',
                'count': 1,
                'detail': [
                    'unable to extract image digests/version from parallel pull'],
            })
            return False

        apply_err = self._set_target_from_pull_info(reference)
        if apply_err:
            self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                'severity': 'warning',
                'summary': 'Upgrade: failed to pull target image',
                'count': 1,
                'detail': [apply_err],
            })
            return False

        assert self.upgrade.upgrade_state.target_digests is not None
        target_digests = self.upgrade.upgrade_state.target_digests
        logger.info(
            'Upgrade: discovered target digests %s version %s from parallel pull',
            target_digests, self.upgrade.upgrade_state.target_version)

        for host in hosts:
            _code, info, _reason = results[host]
            if not self._inspect_info_matches_target_image(info, target_digests):
                got_digests = info.get('repo_digests') if info else None
                self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                    'severity': 'warning',
                    'summary': 'Upgrade: failed to pre-pull target image',
                    'count': 1,
                    'detail': [
                        f'host {host} pull returned mismatched digests '
                        f'(got {got_digests}, expected {target_digests})',
                    ],
                })
                return False

        self._mark_pre_pull_done()
        logger.info('Upgrade: registry pre-pull complete')
        return True

    async def pre_pull_image_on_hosts_async(
        self,
        target_image: str,
        target_digests: List[str],
        hosts: List[str],
    ) -> bool:
        assert self.upgrade.upgrade_state is not None
        discovering = not target_digests
        if discovering:
            return await self._pre_pull_image_discover_async(target_image, hosts)

        hosts_needing_image: List[str] = []
        for host in hosts:
            if not await self._host_has_target_image_on_host(
                    host, target_image, target_digests):
                hosts_needing_image.append(host)

        if not hosts_needing_image:
            logger.info('Upgrade: all in-scope hosts already have target image')
            self._mark_pre_pull_done()
            return True

        max_parallel = max(1, self.mgr.upgrade_image_mirror_max_parallel)
        logger.info(
            'Upgrade: pre-pulling image %s on %d host(s), up to %d in parallel',
            target_image, len(hosts_needing_image), max_parallel)
        self.upgrade.upgrade_info_str = (
            f'Pre-pulling upgrade image on {len(hosts_needing_image)} host(s)')

        pullargs: List[str] = []
        if self.mgr.registry_insecure:
            pullargs.append('--insecure')

        sem = asyncio.Semaphore(max_parallel)
        failed_hosts: List[Tuple[str, str]] = []
        done = 0
        total = len(hosts_needing_image)

        async def _pull_on_host(host: str) -> None:
            nonlocal done
            async with sem:
                if not self.upgrade.upgrade_state or self.upgrade.upgrade_state.paused:
                    return
                try:
                    await self._registry_login_if_needed(host)
                    logger.info('Upgrade: pulling image %s on host %s', target_image, host)
                    _out, errs, code = await CephadmServe(self.mgr)._run_cephadm(
                        host, '', 'pull', pullargs,
                        image=target_image, no_fsid=True, error_ok=True)
                    done += 1
                    if code:
                        reason = ''.join(errs) if errs else 'unknown error'
                        logger.error(
                            'Upgrade: failed to pull image on host %s (%d/%d done)',
                            host, done, total)
                        failed_hosts.append((host, reason))
                    else:
                        logger.info(
                            'Upgrade: pulled image on host %s (%d/%d done)',
                            host, done, total)
                except Exception as e:
                    done += 1
                    failed_hosts.append((host, str(e)))

        await asyncio.gather(*[_pull_on_host(host) for host in hosts_needing_image])

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
            return False

        for host in hosts_needing_image:
            if not await self._host_has_target_image_on_host(
                    host, target_image, target_digests):
                tried = self._get_target_image_inspect_refs(target_image)
                self.upgrade._fail_upgrade('UPGRADE_FAILED_PULL', {
                    'severity': 'warning',
                    'summary': 'Upgrade: failed to pre-pull target image',
                    'count': 1,
                    'detail': [
                        f'host {host} does not have target image after pull '
                        f'(tried {tried})',
                    ],
                })
                return False

        self._mark_pre_pull_done()
        logger.info('Upgrade: registry pre-pull complete')
        return True
