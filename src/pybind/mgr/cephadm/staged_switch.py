"""
Staged switch: upgrade a group of daemons with one restart each.

The regular upgrade path takes a daemon (or a group of daemons, e.g. the
MDS of a failed filesystem) out of service and *then* redeploys it, one
``cephadm deploy`` call at a time. Each call spends most of its time on
work that does not need the daemon to be down: starting cephadm on the
host, a throwaway container to look up uid/gid in the target image,
writing unit files, daemon-reload. The staged switch moves all of that
out of the outage window:

  stage    ``cephadm deploy --stage`` on every daemon of the group, hosts
           in parallel, while the daemons still serve. Config, keyring and
           unit.*.staged are written next to the live files, the target
           image is executed once. Anything that can go wrong with the
           image goes wrong here.
  down     the policy takes the group out of service (MDS: ``fs fail``,
           optionally preceded by a journal flush of the active ranks).
  switch   ``cephadm switch-staged`` on every daemon, hosts in parallel:
           one container stop/start each.
  verify   the policy asks the monitors - not cephadm's daemon cache -
           whether every daemon is back on the target version.
  restore  the policy puts the group back into service (MDS: ``fs set
           joinable true``), and cephadm's cache is refreshed for the
           hosts involved.

If staging fails nothing has been restarted and the upgrade pauses with
UPGRADE_STAGE_FAILED. If the switch or the verification fails every
daemon is switched back to its previous unit files, the group is
restored on the previous release and the upgrade pauses with
UPGRADE_SWITCH_FAILED. Progress is persisted in UpgradeState.staged_switch
so a mgr failover resumes at the right phase; every phase is idempotent.

The runner is daemon-type agnostic. What a "group" is, how it is taken
down, verified and restored is a StagedSwitchPolicy; MdsStagedSwitchPolicy
is the one shipped here (one filesystem at a time, behind ``fail_fs``).
"""

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Type

from orchestrator import DaemonDescription, OrchestratorError, daemon_type_to_service
from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.services.service_registry import service_registry
from cephadm.utils import name_to_config_section

if TYPE_CHECKING:
    from cephadm.upgrade import CephadmUpgrade

logger = logging.getLogger(__name__)

CEPH_MDSMAP_NOT_JOINABLE = (1 << 0)
CEPH_MDSMAP_ALLOW_STANDBY_REPLAY = (1 << 5)
CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS = (1 << 7)

PHASE_STAGING = 'staging'
PHASE_STAGED = 'staged'
PHASE_DOWN = 'down'
PHASE_SWITCHING = 'switching'
PHASE_SWITCHED = 'switched'
PHASE_ROLLING_BACK = 'rolling_back'


class StagedGroup:
    """The daemons switched together, and what the policy needs to know
    about them. `data` and `snapshot` are persisted as JSON."""

    def __init__(self, key: str, label: str, daemons: List[DaemonDescription],
                 data: Optional[Dict[str, Any]] = None) -> None:
        self.key = key
        self.label = label
        self.daemons = daemons
        self.data: Dict[str, Any] = data or {}
        self.snapshot: Dict[str, Any] = {}

    @property
    def names(self) -> List[str]:
        return [d.name() for d in self.daemons]

    @property
    def hosts(self) -> List[str]:
        return sorted({d.hostname for d in self.daemons if d.hostname})


class StagedSwitchPolicy(ABC):
    """What the runner needs to know about one daemon type."""

    daemon_type: str = ''

    def __init__(self, upgrade: 'CephadmUpgrade') -> None:
        self.upgrade = upgrade
        self.mgr = upgrade.mgr

    def enabled(self) -> bool:
        """Whether the staged switch can be used for this type right now
        (e.g. the MDS policy needs fail_fs). Log the reason when not."""
        return True

    @abstractmethod
    def groups(self, need_upgrade: List[DaemonDescription]) -> List[StagedGroup]:
        """Split the daemons still to be upgraded into ordered groups that
        can be taken down together. The runner handles the first one."""

    def preconditions(self, group: StagedGroup) -> Optional[str]:
        """A reason not to start on this group, or None."""
        return None

    @abstractmethod
    def take_down(self, group: StagedGroup) -> None:
        """Take the group out of service. Must be safe to call again on a
        group that is already down (mgr failover)."""

    def is_down(self, group: StagedGroup) -> bool:
        """Whether the group is currently out of service (used on resume)."""
        return True

    @abstractmethod
    def snapshot(self, group: StagedGroup) -> Dict[str, Any]:
        """What verify() compares against, taken right before the switch."""

    @abstractmethod
    def verify(self, group: StagedGroup, snapshot: Dict[str, Any],
               target_version: Optional[str]) -> Tuple[bool, str]:
        """Ask the monitors whether every daemon of the group has restarted
        since `snapshot` and - if target_version is given - runs it.
        Returns (ok, reason)."""

    @abstractmethod
    def restore(self, group: StagedGroup) -> None:
        """Put the group back into service. Called after a successful
        verify, and after a rollback."""


class StagedSwitchRunner:
    """Drives one group through stage / down / switch / verify / restore
    within one serve() pass. The caller returns afterwards; the next pass
    re-evaluates what is left."""

    def __init__(self, upgrade: 'CephadmUpgrade', policy: StagedSwitchPolicy) -> None:
        self.upgrade = upgrade
        self.mgr = upgrade.mgr
        self.policy = policy

    # ---------------------------------------------------------------- state
    @property
    def state(self) -> Dict[str, Any]:
        assert self.upgrade.upgrade_state is not None
        return self.upgrade.upgrade_state.staged_switch

    def _set_phase(self, phase: str) -> None:
        self.state['phase'] = phase
        self.upgrade._save_upgrade_state()

    def _clear(self) -> None:
        assert self.upgrade.upgrade_state is not None
        self.upgrade.upgrade_state.staged_switch = {}
        self.upgrade._save_upgrade_state()

    def _fail(self, alert_id: str, summary: str, detail: List[str]) -> None:
        self.upgrade._fail_upgrade(alert_id, {
            'severity': 'warning',
            'summary': summary,
            'count': max(1, len(detail)),
            'detail': detail,
        })

    # ------------------------------------------------------------ cephadm ops
    async def _stage_all(self, group: StagedGroup, target_image: str) -> Dict[str, str]:
        """deploy --stage every daemon, hosts in parallel. name -> error."""
        sem = asyncio.Semaphore(max(1, int(self.mgr.upgrade_staged_switch_max_parallel)))
        errors: Dict[str, str] = {}

        async def one(d: DaemonDescription) -> None:
            assert d.daemon_type is not None and d.daemon_id is not None
            async with sem:
                try:
                    self.mgr._daemon_action_set_image('redeploy', target_image, d.daemon_type, d.daemon_id)
                    spec = CephadmDaemonDeploySpec.from_daemon_description(d)
                    if d.daemon_type != 'osd':
                        spec = service_registry.get_service(
                            daemon_type_to_service(d.daemon_type)).prepare_create(spec)
                    await CephadmServe(self.mgr)._create_daemon(spec, stage=True)
                except Exception as e:
                    errors[d.name()] = str(e)

        await asyncio.gather(*[one(d) for d in group.daemons])
        return errors

    async def _switch_all(self, group: StagedGroup, target_image: str,
                          rollback: bool = False) -> Dict[str, str]:
        """cephadm switch-staged every daemon, hosts in parallel. name -> error."""
        sem = asyncio.Semaphore(max(1, int(self.mgr.upgrade_staged_switch_max_parallel)))
        errors: Dict[str, str] = {}

        async def one(d: DaemonDescription) -> None:
            assert d.hostname is not None
            args = ['--name', d.name()]
            args += ['--rollback'] if rollback else ['--expected-image', target_image]
            async with sem:
                try:
                    out, err, code = await CephadmServe(self.mgr)._run_cephadm(
                        d.hostname, d.name(), 'switch-staged', args,
                        image=target_image, error_ok=True)
                    if code:
                        errors[d.name()] = '\n'.join(err) or f'exit code {code}'
                except Exception as e:
                    errors[d.name()] = str(e)

        await asyncio.gather(*[one(d) for d in group.daemons])
        return errors

    async def _refresh_hosts(self, hosts: List[str]) -> None:
        """cephadm's daemon cache still shows the previous image for the
        hosts of the group; refresh it now so the next pass moves on."""
        async def one(host: str) -> None:
            try:
                ls = await CephadmServe(self.mgr)._run_cephadm_json(
                    host, 'mon', 'ls', [], no_fsid=True,
                    log_output=self.mgr.log_refresh_metadata)
                self.mgr._process_ls_output(host, ls)
            except Exception as e:
                logger.warning('Upgrade: refreshing daemons of %s failed: %s', host, e)
                self.mgr.cache.invalidate_host_daemons(host)
        await asyncio.gather(*[one(h) for h in hosts])

    def _drop_image_pins(self, group: StagedGroup) -> None:
        """Forget the per-daemon container_image set for staging, so the
        reconciler does not redeploy the new image on its own later."""
        for d in group.daemons:
            try:
                self.mgr.check_mon_command({
                    'prefix': 'config rm', 'name': 'container_image',
                    'who': name_to_config_section(d.name())})
            except Exception as e:
                logger.warning('Upgrade: could not drop container_image for %s: %s', d.name(), e)

    def _wait(self, group: StagedGroup, snapshot: Dict[str, Any],
              target_version: Optional[str], timeout: int) -> Tuple[bool, str]:
        deadline = time.time() + timeout
        while True:
            ok, why = self.policy.verify(group, snapshot, target_version)
            if ok:
                return True, ''
            if time.time() >= deadline:
                return False, why
            time.sleep(2)

    # --------------------------------------------------------------- driver
    def _group_from_state(self, need_upgrade: List[DaemonDescription]) -> Optional[StagedGroup]:
        st = self.state
        if not st or st.get('type') != self.policy.daemon_type:
            return None
        by_name = {d.name(): d for d in self.mgr.cache.get_daemons_by_type(self.policy.daemon_type)}
        missing = [n for n in st['daemons'] if n not in by_name]
        if missing:
            raise OrchestratorError(
                f'cannot resume the staged switch of {st["label"]}: '
                f'{", ".join(missing)} no longer known to cephadm')
        group = StagedGroup(st['key'], st['label'], [by_name[n] for n in st['daemons']], st.get('data'))
        group.snapshot = st.get('snapshot') or {}
        return group

    def _new_group(self, need_upgrade: List[DaemonDescription], target_image: str) -> Optional[StagedGroup]:
        groups = self.policy.groups(need_upgrade)
        if not groups:
            return None
        group = groups[0]
        offline = sorted(h for h in group.hosts if h in self.mgr.offline_hosts)
        reason = f'host(s) {", ".join(offline)} offline' if offline else self.policy.preconditions(group)
        if reason:
            self._fail('UPGRADE_STAGE_FAILED',
                       f'Cannot stage the upgrade of {group.label}: {reason}', [reason])
            return None
        assert self.upgrade.upgrade_state is not None
        self.upgrade.upgrade_state.staged_switch = {
            'type': self.policy.daemon_type, 'key': group.key, 'label': group.label,
            'daemons': group.names, 'hosts': group.hosts, 'image': target_image,
            'data': group.data, 'snapshot': {}, 'phase': PHASE_STAGING,
        }
        self.upgrade._save_upgrade_state()
        return group

    def run(self, need_upgrade: List[DaemonDescription], target_image: str) -> bool:
        """Handle one group. Returns False when the policy found nothing
        to handle (the caller then upgrades these daemons the regular
        way); True when the group was handled, or the upgrade was paused."""
        assert self.upgrade.upgrade_state is not None
        target_version = self.upgrade.upgrade_state.target_version
        timeout = int(self.mgr.upgrade_staged_switch_timeout)

        group = self._group_from_state(need_upgrade)
        if group is None:
            if not self.policy.groups(need_upgrade):
                return False
            group = self._new_group(need_upgrade, target_image)
            if group is None:
                return True
        else:
            target_image = self.state.get('image') or target_image
        st = self.state
        phase = st.get('phase')
        logger.info('Upgrade: staged switch of %s (%d %s on %d host(s)), phase %s',
                    group.label, len(group.daemons), self.policy.daemon_type, len(group.hosts), phase)

        if phase == PHASE_STAGING:
            self.upgrade.upgrade_info_str = f'Staging {self.policy.daemon_type} of {group.label}'
            errors = self.mgr.wait_async(self._stage_all(group, target_image))
            if errors:
                # Nothing has been taken down; staged files are inert.
                self._clear()
                self._fail('UPGRADE_STAGE_FAILED',
                           f'Staging the upgrade of {group.label} failed on '
                           f'{len(errors)} daemon(s); nothing was restarted',
                           [f'{k}: {v}' for k, v in errors.items()])
                return True
            self._set_phase(PHASE_STAGED)
            phase = PHASE_STAGED

        if phase == PHASE_STAGED:
            self.upgrade.upgrade_info_str = f'Taking {group.label} down for the staged switch'
            try:
                self.policy.take_down(group)
                group.snapshot = self.policy.snapshot(group)
                st['snapshot'] = group.snapshot
                st['data'] = group.data
                self._set_phase(PHASE_DOWN)
            except Exception as e:
                # Nothing has been switched: restore whatever was taken
                # down, keep the staged files for a retry, pause.
                logger.error('Upgrade: could not take %s down for the staged switch: %s', group.label, e)
                try:
                    self.policy.restore(group)
                finally:
                    self._clear()
                self._fail('UPGRADE_STAGE_FAILED',
                           f'Could not take {group.label} down for the staged switch: {e}; '
                           f'nothing was restarted', [str(e)])
                return True
            phase = PHASE_DOWN

        if phase in (PHASE_DOWN, PHASE_SWITCHING):
            if phase == PHASE_SWITCHING or not self.policy.is_down(group):
                # Resuming after a mgr failover: make sure the group really
                # is out of service before any daemon is restarted.
                self.policy.take_down(group)
            self.upgrade.upgrade_info_str = f'Switching {self.policy.daemon_type} of {group.label} to {target_image}'
            self._set_phase(PHASE_SWITCHING)
            errors = self.mgr.wait_async(self._switch_all(group, target_image))
            if errors:
                self._rollback(group, target_image,
                               'switch-staged failed on ' + ', '.join(f'{k} ({v})' for k, v in errors.items()))
                return True
            self._set_phase(PHASE_SWITCHED)
            phase = PHASE_SWITCHED

        if phase == PHASE_SWITCHED:
            self.upgrade.upgrade_info_str = f'Waiting for the {self.policy.daemon_type} of {group.label} on {target_version}'
            ok, why = self._wait(group, group.snapshot, target_version, timeout)
            if not ok:
                self._rollback(group, target_image, why)
                return True
            logger.info('Upgrade: all %d %s of %s are back on %s; restoring',
                        len(group.daemons), self.policy.daemon_type, group.label, target_version)
            self.policy.restore(group)
            self.mgr.wait_async(self._refresh_hosts(group.hosts))
            self._clear()
            logger.info('Upgrade: %s back on %s', group.label, target_version)
            return True

        if phase == PHASE_ROLLING_BACK:
            self._rollback(group, target_image, 'resumed after a mgr failover during rollback')
        return True

    def _rollback(self, group: StagedGroup, target_image: str, reason: str) -> None:
        """Undo a switch that did not complete: previous unit files back,
        daemons back on the previous release, group restored, upgrade paused."""
        logger.error('Upgrade: rolling back the staged switch of %s: %s', group.label, reason)
        self._set_phase(PHASE_ROLLING_BACK)
        before = self.policy.snapshot(group)
        errors = self.mgr.wait_async(self._switch_all(group, target_image, rollback=True))
        detail = [f'{k}: {v}' for k, v in errors.items()]
        if not errors:
            ok, why = self._wait(group, before, None, int(self.mgr.upgrade_staged_switch_timeout))
            if not ok:
                detail.append(f'after rollback: {why}')
        self._drop_image_pins(group)
        if not detail:
            self.policy.restore(group)
            summary = (f'Switching {group.label} to the staged image failed ({reason}); '
                       f'rolled back and restored on the previous image')
        else:
            summary = (f'Switching {group.label} to the staged image failed ({reason}) '
                       f'and the rollback did not complete; {group.label} is left out of service. '
                       f'Fix the daemons listed, then restore it by hand')
        self._clear()
        self._fail('UPGRADE_SWITCH_FAILED', summary, detail or [reason])


# ======================================================================
# MDS: one filesystem (group) at a time, behind fail_fs
# ======================================================================

class MdsStagedSwitchPolicy(StagedSwitchPolicy):
    daemon_type = 'mds'

    def enabled(self) -> bool:
        assert self.upgrade.upgrade_state is not None
        if not self.upgrade.upgrade_state.fail_fs:
            logger.warning('Upgrade: upgrade_staged_switch is set for mds but '
                           'mgr/orchestrator/fail_fs is not; MDS will be upgraded '
                           'the regular way')
            return False
        return True

    # -------------------------------------------------------------- helpers
    def _fsmap(self) -> Dict[str, Any]:
        return self.mgr.get('fs_map')

    def _filesystems(self, fs_names: List[str]) -> List[Dict[str, Any]]:
        return [fs for fs in self._fsmap().get('filesystems', [])
                if fs['mdsmap']['fs_name'] in fs_names]

    def _fs_by_id(self, fscid: int) -> Optional[Dict[str, Any]]:
        for fs in self._fsmap().get('filesystems', []):
            if fs['id'] == fscid:
                return fs
        return None

    def _mds_names(self, group: StagedGroup) -> List[str]:
        return [n.split('.', 1)[1] for n in group.names]

    def _gids(self, names: List[str]) -> Dict[str, int]:
        fsmap = self._fsmap()
        gids: Dict[str, int] = {}
        for info in fsmap.get('standbys', []):
            if info.get('name') in names:
                gids[info['name']] = info['gid']
        for fs in fsmap.get('filesystems', []):
            for info in (fs['mdsmap'].get('info') or {}).values():
                if info.get('name') in names:
                    gids[info['name']] = info['gid']
        return gids

    def _versions(self) -> Dict[str, str]:
        """MDS name -> ceph_version_short, from the monitors."""
        ret, out, err = self.mgr.check_mon_command({'prefix': 'mds metadata', 'format': 'json'})
        versions: Dict[str, str] = {}
        try:
            for md in json.loads(out or '[]'):
                v = md.get('ceph_version_short') or ''
                if not v and md.get('ceph_version', '').startswith('ceph version '):
                    v = md['ceph_version'].split(' ')[2]
                if md.get('name'):
                    versions[md['name']] = v
        except (ValueError, TypeError, AttributeError):
            logger.warning('Upgrade: could not parse `mds metadata`: %s', out)
        return versions

    # --------------------------------------------------------------- policy
    def groups(self, need_upgrade: List[DaemonDescription]) -> List[StagedGroup]:
        # _do_upgrade already restricted need_upgrade to one filesystem
        # (upgrade_fs_one_at_a_time); a standby takeover can make that two.
        fs_names = sorted(self.upgrade._fs_names_of_mds_daemons(need_upgrade))
        filesystems = self._filesystems(fs_names)
        if not filesystems:
            return []
        # Every MDS of those filesystems' services, not only the ones still
        # on the old image: fs fail restarts them all anyway, and they all
        # need a new gid before the filesystem is re-joined.
        daemons: List[DaemonDescription] = []
        seen: Set[str] = set()
        for fs_name in fs_names:
            for d in self.mgr.cache.get_daemons_by_service(f'mds.{fs_name}'):
                if d.name() not in seen:
                    seen.add(d.name())
                    daemons.append(d)
        for d in need_upgrade:
            if d.name() not in seen:
                seen.add(d.name())
                daemons.append(d)
        fscids = [fs['id'] for fs in filesystems]
        label = 'filesystem ' + ', '.join(fs_names) if len(fs_names) == 1 \
            else 'filesystems ' + ', '.join(fs_names)
        return [StagedGroup(','.join(str(i) for i in fscids), label, daemons,
                            {'fscids': fscids, 'fs_names': fs_names})]

    def preconditions(self, group: StagedGroup) -> Optional[str]:
        fsmap = self._fsmap()
        fs_names = group.data['fs_names']
        by_name = {fs['mdsmap']['fs_name']: fs for fs in fsmap.get('filesystems', [])}
        names = set(self._mds_names(group))
        # An MDS in the group that holds a rank / standby-replay in a
        # filesystem we are NOT about to fail would be restarted under a
        # live filesystem.
        for fs in fsmap.get('filesystems', []):
            if fs['mdsmap']['fs_name'] in fs_names:
                continue
            for info in (fs['mdsmap'].get('info') or {}).values():
                if info.get('name') in names:
                    return (f'mds.{info["name"]} currently serves filesystem '
                            f'{fs["mdsmap"]["fs_name"]} ({info.get("state")})')
        for fs_name in fs_names:
            fs = by_name.get(fs_name)
            if fs is None:
                return f'filesystem {fs_name} not found'
            mdsmap = fs['mdsmap']
            if mdsmap.get('damaged'):
                return f'filesystem {fs_name} has damaged rank(s) {mdsmap["damaged"]}'
            if len(by_name) > 1 and not (mdsmap['flags'] & CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS):
                logger.warning(
                    'Upgrade: filesystem %s does not have refuse_standby_for_another_fs '
                    'set; a standby from another filesystem could take one of its ranks '
                    'while it is being upgraded. Consider `ceph fs set %s '
                    'refuse_standby_for_another_fs true`', fs_name, fs_name)
        # Every rank must be taken back by a daemon of the group. On re-join
        # the monitors hand a rank to a standby pinned to the filesystem
        # (mds_join_fs) first, then to an unpinned one, then - without
        # refuse_standby_for_another_fs - to one pinned elsewhere
        # (FSMap::get_available_standby); refuse_standby_for_another_fs does
        # not keep unpinned standbys out. The group is every daemon of the
        # mds.<fs> service, all switched and pinned, so with at least one per
        # rank no standby from outside the group - not switched, possibly on
        # an older release - can be picked. With fewer, the leftover ranks
        # would go to such a standby: refuse before taking anything down.
        ranks = sum(int(by_name[f]['mdsmap'].get('max_mds') or 0) for f in fs_names)
        if len(names) < ranks:
            services = ', '.join(f'mds.{f}' for f in fs_names)
            return (f'{services} has {len(names)} MDS daemon(s) for {ranks} rank(s): the '
                    f'ranks left over after the switch would be handed to standbys outside '
                    f'the upgraded group. Add MDS daemons to the service, or disable '
                    f'mgr/cephadm/upgrade_staged_switch')
        return None

    def _flush_journals(self, fs: Dict[str, Any]) -> None:
        # Shortens the replay after the switch at the price of metadata
        # pool I/O before the outage (upgrade_staged_switch_flush_mds_journal,
        # on by default). One rank at a time, and best effort - a failed
        # flush only means a longer replay.
        if not getattr(self.mgr, 'upgrade_staged_switch_flush_mds_journal', True):
            return
        for info in (fs['mdsmap'].get('info') or {}).values():
            if info.get('state') != 'up:active':
                continue
            try:
                r, outb, outs = self.mgr.tell_command('mds', info['name'], {'prefix': 'flush journal'})
                if r:
                    logger.info('Upgrade: flush journal on mds.%s failed (%s); replay may take longer',
                                info['name'], outs)
            except Exception as e:
                logger.info('Upgrade: flush journal on mds.%s failed (%s); replay may take longer',
                            info['name'], e)

    def _disable_standby_replay(self, fs: Dict[str, Any], timeout: int = 90) -> None:
        state = self.upgrade.upgrade_state
        assert state is not None
        fscid, mdsmap = fs['id'], fs['mdsmap']
        fs_name = mdsmap['fs_name']
        if mdsmap['flags'] & CEPH_MDSMAP_ALLOW_STANDBY_REPLAY:
            logger.info('Upgrade: Disabling standby-replay for filesystem %s', fs_name)
            if not state.fs_original_allow_standby_replay:
                state.fs_original_allow_standby_replay = {}
            state.fs_original_allow_standby_replay[fscid] = True
            self.upgrade._save_upgrade_state()
            self.mgr.check_mon_command({
                'prefix': 'fs set', 'fs_name': fs_name,
                'var': 'allow_standby_replay', 'val': '0'})
        deadline = time.time() + timeout
        while True:
            cur = self._fs_by_id(fscid)
            if cur is None:
                return
            if not any(i.get('state') == 'up:standby-replay'
                       for i in (cur['mdsmap'].get('info') or {}).values()):
                return
            if time.time() >= deadline:
                raise OrchestratorError(
                    f'filesystem {fs_name} still has standby-replay daemons after {timeout}s')
            time.sleep(2)

    def take_down(self, group: StagedGroup) -> None:
        state = self.upgrade.upgrade_state
        assert state is not None
        if not state.fs_failed_for_upgrade:
            state.fs_failed_for_upgrade = []
        for fs in self._filesystems(group.data['fs_names']):
            fs_name = fs['mdsmap']['fs_name']
            if fs['mdsmap']['flags'] & CEPH_MDSMAP_NOT_JOINABLE:
                continue  # already failed (resume)
            self._flush_journals(fs)
            self._disable_standby_replay(fs)
            # remember that WE are failing this fs (saved before the
            # command, so a mgr failover in between still re-joins it)
            if fs['id'] not in state.fs_failed_for_upgrade:
                state.fs_failed_for_upgrade.append(fs['id'])
                self.upgrade._save_upgrade_state()
            logger.info('Upgrade: failing fs %s for the staged MDS switch', fs_name)
            ret, out, err = self.mgr.mon_command({'prefix': 'fs fail', 'fs_name': fs_name})
            if ret != 0:
                raise OrchestratorError(f'fs fail {fs_name} failed: {err}')

    def is_down(self, group: StagedGroup) -> bool:
        return all(fs['mdsmap']['flags'] & CEPH_MDSMAP_NOT_JOINABLE
                   for fs in self._filesystems(group.data['fs_names']))

    def snapshot(self, group: StagedGroup) -> Dict[str, Any]:
        return {'gids': self._gids(self._mds_names(group))}

    def verify(self, group: StagedGroup, snapshot: Dict[str, Any],
               target_version: Optional[str]) -> Tuple[bool, str]:
        names = self._mds_names(group)
        pre_gids = {k: int(v) for k, v in (snapshot.get('gids') or {}).items()}
        standbys = {s['name']: s for s in self._fsmap().get('standbys', [])}
        for n in names:
            s = standbys.get(n)
            if s is None:
                return False, f'mds.{n} is not a standby yet'
            if pre_gids.get(n) is not None and s['gid'] == pre_gids[n]:
                return False, f'mds.{n} has not re-registered yet'
        if target_version:
            versions = self._versions()
            for n in names:
                if versions.get(n) != target_version:
                    return False, f'mds.{n} reports version {versions.get(n)!r}, want {target_version!r}'
            # The group covers every rank (preconditions), so an unpinned
            # standby is never picked. Any other standby pinned to one of our
            # filesystems must be on
            # the target version too: the monitors pick the first pinned
            # standby by gid, so an old one would take rank 0 and the
            # filesystem would then refuse every new-version standby.
            fscids = group.data['fscids']
            for s in standbys.values():
                if s.get('join_fscid') in fscids and s['name'] not in names \
                        and versions.get(s['name']) != target_version:
                    return False, (f'standby mds.{s["name"]} is pinned to this filesystem '
                                   f'but runs {versions.get(s["name"])!r}')
        return True, ''

    def restore(self, group: StagedGroup) -> None:
        # joinable true, wait for the ranks, restore standby-replay - and
        # only for the filesystems this upgrade failed
        self.upgrade._complete_mds_upgrade(fs_names=list(group.data['fs_names']))


POLICIES: Dict[str, Type[StagedSwitchPolicy]] = {
    MdsStagedSwitchPolicy.daemon_type: MdsStagedSwitchPolicy,
}


def policy_for(upgrade: 'CephadmUpgrade', daemon_type: str) -> Optional[StagedSwitchPolicy]:
    """The policy to use for daemon_type in this upgrade, or None when the
    staged switch is off, not configured for the type, or not usable now."""
    mgr = upgrade.mgr
    if not getattr(mgr, 'upgrade_staged_switch', False):
        return None
    types = [t.strip() for t in str(getattr(mgr, 'upgrade_staged_switch_types', '') or '').split(',')]
    if daemon_type not in types:
        return None
    cls = POLICIES.get(daemon_type)
    if cls is None:
        logger.warning('Upgrade: upgrade_staged_switch_types lists %s but there is '
                       'no staged switch policy for it; upgrading it the regular way', daemon_type)
        return None
    policy = cls(upgrade)
    if not policy.enabled():
        return None
    return policy
