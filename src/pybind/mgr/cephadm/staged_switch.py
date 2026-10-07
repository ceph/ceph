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
           optionally preceded by a journal flush of the active ranks;
           OSD: ``osd set-group noout`` on the group).
  switch   ``cephadm switch-staged`` on every daemon, hosts in parallel:
           one container stop/start each.
  verify   the policy asks the monitors - not cephadm's daemon cache -
           whether every daemon is back on the target version (OSD: and
           every PG it holds has peered again with it).
  restore  the policy puts the group back into service (MDS: ``fs set
           joinable true``; OSD: ``osd unset-group noout``), and cephadm's
           cache is refreshed for the hosts involved.
  settle   the policy says whether the group has settled enough for the
           next one to be chosen (OSD: in the acting set of every PG it
           holds, caught up with the writes it missed).
           Not a failure when it takes long: no timeout, the upgrade is not
           paused, the next serve() pass asks again.

A policy that can tell in advance which daemons it will switch has them
all staged once, at the start of their phase (stage ahead, hosts in
parallel, daemons still serving); a group then re-stages only the daemons
whose staged deployment would no longer be the same - another target image,
another generated configuration - so the staging is mostly out of the time
between two groups.

If staging fails nothing has been restarted and the upgrade pauses with
UPGRADE_STAGE_FAILED. If the switch or the verification fails, a policy
with ``rollback_on_failure`` (MDS) has every daemon switched back to its
previous unit files and the group restored on the previous release; one
without (OSD: a store a newer ceph-osd has opened is not to be reopened
by the previous release) leaves the daemons as they are and the upgrade
resumes at the same phase once the admin has dealt with them. Either way
the upgrade pauses with UPGRADE_SWITCH_FAILED. Progress is persisted in
UpgradeState.staged_switch so a mgr failover resumes at the right phase;
every phase is idempotent.

A policy can also answer "not now" (StagedSwitchNotReady): nothing is
staged, the upgrade is not paused, and the next serve() pass asks again.
This is how the OSD policy waits for the PGs to recover between groups.

The runner is daemon-type agnostic. What a "group" is, how it is taken
down, verified and restored is a StagedSwitchPolicy. Two are shipped here:
MdsStagedSwitchPolicy (one filesystem at a time, behind ``fail_fs``) and
OsdStagedSwitchPolicy (every OSD still to upgrade under one CRUSH bucket of
a given type, when ``osd ok-to-stop`` says every PG stays active).
"""

import asyncio
import hashlib
import json
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Type

from orchestrator import DaemonDescription, OrchestratorError, daemon_type_to_service
from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec, DaemonDeployContext
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
PHASE_SETTLING = 'settling'
PHASE_ROLLING_BACK = 'rolling_back'

# how long one serve() pass polls a settling group before handing back
SETTLE_POLL_SECONDS = 30


class StagedSwitchNotReady(Exception):
    """Raised by a policy when there is nothing it can safely switch *right
    now* (e.g. no CRUSH bucket of OSDs is ok-to-stop while the PGs of the
    previous group recover). Not an error: the runner leaves the upgrade
    running and the next serve() pass asks again."""


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
    # Whether a switch that does not complete (switch-staged failed, or the
    # group is not back on the target version in time) is undone by putting
    # the previous unit files back. Right for daemons that keep no local
    # state the new release may have touched (MDS); wrong for OSDs, whose
    # stores a newer ceph-osd may have upgraded on boot. With False the
    # runner pauses the upgrade and keeps its state, so `ceph orch upgrade
    # resume` picks the group up again at the same phase.
    rollback_on_failure: bool = True

    def __init__(self, upgrade: 'CephadmUpgrade') -> None:
        self.upgrade = upgrade
        self.mgr = upgrade.mgr

    def verify_timeout(self) -> int:
        """Seconds to wait for the monitors to see the whole group back
        before giving up on the switch."""
        return int(self.mgr.upgrade_staged_switch_timeout)

    def enabled(self) -> bool:
        """Whether the staged switch can be used for this type right now
        (e.g. the MDS policy needs fail_fs). Log the reason when not."""
        return True

    @abstractmethod
    def groups(self, need_upgrade: List[DaemonDescription]) -> List[StagedGroup]:
        """Split the daemons still to be upgraded into ordered groups that
        can be taken down together. The runner handles the first one.

        An empty list means the policy has nothing to handle and the caller
        upgrades these daemons the regular way. Raise StagedSwitchNotReady
        when there is something to handle but it cannot be done safely right
        now; raise OrchestratorError for a configuration problem (the
        upgrade is then paused with UPGRADE_STAGE_FAILED)."""

    def preconditions(self, group: StagedGroup) -> Optional[str]:
        """A reason not to start on this group, or None."""
        return None

    @abstractmethod
    def take_down(self, group: StagedGroup) -> None:
        """Take the group out of service. Must be safe to call again on a
        group that is already down (mgr failover). May raise
        StagedSwitchNotReady if the group can no longer be taken down
        safely: the staged files are left behind (they are inert) and the
        next pass starts over from groups()."""

    def before_switch(self, group: StagedGroup) -> None:
        """Last word before the first daemon is restarted: the group is
        down (take_down ran, possibly a while ago on a resumed upgrade) and
        nothing has been switched yet. Raise StagedSwitchNotReady to call it
        off: the runner restores the group and starts over next pass."""
        return None

    def forget(self, state: Dict[str, Any], names: List[str]) -> None:
        """Daemons of a persisted group that are no longer known to cephadm
        (purged by the admin) are dropped from it on resume; adjust the
        policy's own `data` and `snapshot` entries in `state` accordingly."""
        return None

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

    def stage_ahead(self, need_upgrade: List[DaemonDescription]) -> List[DaemonDescription]:
        """The daemons to stage at once, ahead of their groups (default:
        none - each group is staged when it is picked). Only daemons this
        policy will switch itself; staged files of a daemon that ends up
        upgraded another way are inert."""
        return []

    def settled(self, group: StagedGroup) -> Tuple[bool, str]:
        """Whether the group, verified and restored, has settled enough for
        the next group to be chosen (e.g. caught up with what its daemons
        missed during the switch). Not bounded by verify_timeout() and never
        a failure: until it is, the runner keeps the group, without pausing
        the upgrade, and asks again on the next pass. Returns (ok, reason)."""
        return True, ''


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
    @staticmethod
    def _by_host(group: StagedGroup) -> Dict[str, List[DaemonDescription]]:
        by_host: Dict[str, List[DaemonDescription]] = {}
        for d in group.daemons:
            assert d.hostname is not None
            by_host.setdefault(d.hostname, []).append(d)
        return by_host

    @staticmethod
    def _stage_fingerprint(spec: CephadmDaemonDeploySpec, target_image: str) -> str:
        """What a staged deployment depends on: the target image, the
        generated configuration (config, keyring, files) and the deps."""
        blob = json.dumps({'image': target_image, 'config': spec.final_config,
                           'deps': spec.deps}, sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()

    def _stage_spec(self, d: DaemonDescription, target_image: str) -> CephadmDaemonDeploySpec:
        assert d.daemon_type is not None and d.daemon_id is not None
        self.mgr._daemon_action_set_image('redeploy', target_image, d.daemon_type, d.daemon_id)
        spec = CephadmDaemonDeploySpec.from_daemon_description(d)
        ctx = DaemonDeployContext(spec, self.mgr.spec_store.active_specs.get(spec.service_name))
        if d.daemon_type != 'osd':
            spec = service_registry.get_service(
                daemon_type_to_service(d.daemon_type)).prepare_create(ctx)
        else:
            # like _daemon_action: OSDs get their config refreshed but not
            # the full prepare_create
            spec.final_config, spec.deps = self.mgr.osd_service.generate_config(ctx)
        return spec

    def _ahead(self, target_image: str) -> Dict[str, str]:
        """name -> fingerprint of the daemons of this type staged ahead
        for this target image."""
        assert self.upgrade.upgrade_state is not None
        st = self.upgrade.upgrade_state.staged_ahead.get(self.policy.daemon_type) or {}
        return dict(st.get('daemons') or {}) if st.get('image') == target_image else {}

    async def _stage(self, daemons: List[DaemonDescription], target_image: str,
                     already: Optional[Dict[str, str]] = None
                     ) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
        """deploy --stage every daemon whose staged deployment is not already
        the one it would get now (`already`: name -> fingerprint): up to
        max_parallel hosts at a time, the daemons of one host one after the
        other (cephadm's per-host lock would serialize them anyway).
        Returns (name -> error, name -> fingerprint staged, names skipped)."""
        sem = asyncio.Semaphore(max(1, int(self.mgr.upgrade_staged_switch_max_parallel)))
        errors: Dict[str, str] = {}
        staged: Dict[str, str] = {}
        skipped: List[str] = []
        already = already or {}
        # one osdmap read for the whole call rather than one per OSD
        osd_uuid_map: Optional[Dict[str, Any]] = None
        if any(d.daemon_type == 'osd' for d in daemons):
            try:
                osd_uuid_map = self.mgr.get_osd_uuid_map()
            except Exception as e:
                logger.debug('Upgrade: could not read the osd uuid map up front: %s', e)

        async def one(d: DaemonDescription) -> None:
            try:
                spec = self._stage_spec(d, target_image)
                fp = self._stage_fingerprint(spec, target_image)
                if already.get(d.name()) == fp:
                    skipped.append(d.name())
                    return
                await CephadmServe(self.mgr)._create_daemon(
                    spec, osd_uuid_map=osd_uuid_map, stage=True)
                staged[d.name()] = fp
            except Exception as e:
                errors[d.name()] = str(e)

        async def host(daemons: List[DaemonDescription]) -> None:
            async with sem:
                for d in daemons:
                    await one(d)

        by_host: Dict[str, List[DaemonDescription]] = {}
        for d in daemons:
            assert d.hostname is not None
            by_host.setdefault(d.hostname, []).append(d)
        await asyncio.gather(*[host(ds) for ds in by_host.values()])
        return errors, staged, skipped

    async def _stage_all(self, group: StagedGroup, target_image: str) -> Dict[str, str]:
        """Stage the group, skipping the daemons staged ahead whose staged
        deployment is still the right one. name -> error."""
        errors, _, skipped = await self._stage(group.daemons, target_image,
                                               already=self._ahead(target_image))
        if skipped:
            logger.info('Upgrade: %d %s of %s already staged ahead', len(skipped),
                        self.policy.daemon_type, group.label)
        return errors

    def _stage_ahead(self, need_upgrade: List[DaemonDescription], target_image: str) -> None:
        """Once per daemon type and target image: stage every daemon the
        policy names, ahead of their groups. A daemon that fails to stage
        here is just not recorded: its group stages it again, and pauses
        the upgrade if that fails too."""
        assert self.upgrade.upgrade_state is not None
        if not getattr(self.mgr, 'upgrade_staged_switch_stage_ahead', True):
            return
        st = self.upgrade.upgrade_state.staged_ahead.get(self.policy.daemon_type) or {}
        if st.get('image') == target_image:
            return
        try:
            daemons = self.policy.stage_ahead(need_upgrade)
        except Exception as e:
            # e.g. a configuration error: groups() reports it properly
            logger.warning('Upgrade: not staging %s daemons ahead: %s', self.policy.daemon_type, e)
            return
        if not daemons:
            return
        self.upgrade.upgrade_info_str = (f'Staging {len(daemons)} {self.policy.daemon_type} '
                                         f'daemon(s) ahead of their groups')
        logger.info('Upgrade: staging %d %s daemon(s) on %d host(s) ahead of their groups',
                    len(daemons), self.policy.daemon_type,
                    len({d.hostname for d in daemons}))
        start = time.time()
        errors, staged, _ = self.mgr.wait_async(self._stage(daemons, target_image))
        self.upgrade.upgrade_state.staged_ahead[self.policy.daemon_type] = {
            'image': target_image, 'daemons': staged}
        self.upgrade._save_upgrade_state()
        logger.info('Upgrade: staged %d %s daemon(s) ahead in %.0fs%s', len(staged),
                    self.policy.daemon_type, time.time() - start,
                    f'; {len(errors)} will be staged with their group ({", ".join(sorted(errors)[:5])})'
                    if errors else '')

    async def _switch_all(self, group: StagedGroup, target_image: str,
                          rollback: bool = False) -> Dict[str, str]:
        """cephadm switch-staged, one call per host naming every daemon of
        the group on it (one stop / one start for all of them), up to
        max_parallel hosts at a time. name -> error."""
        sem = asyncio.Semaphore(max(1, int(self.mgr.upgrade_staged_switch_max_parallel)))
        errors: Dict[str, str] = {}

        async def host(hostname: str, daemons: List[DaemonDescription]) -> None:
            args: List[str] = []
            for d in daemons:
                args += ['--name', d.name()]
            args += ['--rollback'] if rollback else ['--expected-image', target_image]
            async with sem:
                try:
                    out, err, code = await CephadmServe(self.mgr)._run_cephadm(
                        hostname, daemons[0].name(), 'switch-staged', args,
                        image=target_image, error_ok=True)
                    if code:
                        # the command checks every daemon before stopping
                        # any; a failure is the host's, attribute it to each
                        why = '\n'.join(err) or f'exit code {code}'
                        for d in daemons:
                            errors[d.name()] = why
                except Exception as e:
                    for d in daemons:
                        errors[d.name()] = str(e)

        await asyncio.gather(*[host(h, ds) for h, ds in self._by_host(group).items()])
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
            time.sleep(1)

    def _wait_settled(self, group: StagedGroup) -> Tuple[bool, str]:
        deadline = time.time() + SETTLE_POLL_SECONDS
        while True:
            ok, why = self.policy.settled(group)
            if ok or time.time() >= deadline:
                return ok, why
            if self.upgrade.upgrade_state is None or self.upgrade.upgrade_state.paused:
                return ok, why
            time.sleep(1)

    # --------------------------------------------------------------- driver
    def _group_from_state(self, need_upgrade: List[DaemonDescription]) -> Optional[StagedGroup]:
        st = self.state
        if not st or st.get('type') != self.policy.daemon_type:
            return None
        by_name = {d.name(): d for d in self.mgr.cache.get_daemons_by_type(self.policy.daemon_type)}
        missing = [n for n in st['daemons'] if n not in by_name]
        if missing:
            # Removed from cephadm since the group was formed - typically
            # the admin purged a daemon that did not come back after the
            # switch. Go on without it rather than wedge the upgrade.
            logger.warning('Upgrade: %s no longer known to cephadm; resuming the staged '
                           'switch of %s without %s', ', '.join(missing), st['label'],
                           'it' if len(missing) == 1 else 'them')
            st['daemons'] = [n for n in st['daemons'] if n in by_name]
            self.policy.forget(st, missing)
            self.upgrade._save_upgrade_state()
            if not st['daemons']:
                self._clear()
                return None
        group = StagedGroup(st['key'], st['label'], [by_name[n] for n in st['daemons']], st.get('data'))
        group.snapshot = st.get('snapshot') or {}
        return group

    def _new_group(self, group: StagedGroup, target_image: str) -> Optional[StagedGroup]:
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

    def _not_ready(self, what: str, reason: str) -> None:
        msg = f'Waiting to stage {what}: {reason}'
        logger.info('Upgrade: %s', msg)
        self.upgrade.upgrade_info_str = msg

    def _count_against_limit(self, group: StagedGroup) -> None:
        """`ceph orch upgrade start --limit N`: the daemons of a switched
        group count like redeployed ones (redeploy-only ones excluded)."""
        state = self.upgrade.upgrade_state
        assert state is not None
        if state.remaining_count is None:
            return
        redeploy_only = set(self.state.get('redeploy_only') or [])
        state.remaining_count -= len([n for n in group.names if n not in redeploy_only])
        # saved by the caller's _set_phase(PHASE_SETTLING), in the same
        # write as the end of the switch, so a failover in between cannot
        # count it twice

    def run(self, need_upgrade: List[DaemonDescription], target_image: str,
            redeploy_only: Optional[Iterable[str]] = None) -> bool:
        """Handle one group. Returns False when the policy found nothing
        to handle (the caller then upgrades these daemons the regular
        way); True when the group was handled, the policy asked to wait,
        or the upgrade was paused.

        `redeploy_only` names the daemons of need_upgrade that are already
        on the target image and only need a redeploy (they do not count
        against `--limit`)."""
        assert self.upgrade.upgrade_state is not None
        target_version = self.upgrade.upgrade_state.target_version
        timeout = self.policy.verify_timeout()
        remaining = self.upgrade.upgrade_state.remaining_count

        group = self._group_from_state(need_upgrade)
        if group is None:
            if remaining is not None and remaining <= 0:
                return False  # --limit reached; the regular path ends the upgrade
            if need_upgrade:
                self._stage_ahead(need_upgrade, target_image)
            try:
                groups = self.policy.groups(need_upgrade)
            except StagedSwitchNotReady as e:
                self._not_ready(f'{self.policy.daemon_type} daemons', str(e))
                return True
            except OrchestratorError as e:
                self._fail('UPGRADE_STAGE_FAILED',
                           f'Cannot stage the upgrade of {self.policy.daemon_type} daemons: {e}', [str(e)])
                return True
            if not groups:
                return False
            group = self._new_group(groups[0], target_image)
            if group is None:
                return True
            self.state['redeploy_only'] = sorted(
                n for n in (redeploy_only or []) if n in set(group.names))
            self.upgrade._save_upgrade_state()
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
            except StagedSwitchNotReady as e:
                # The cluster changed under us since the group was chosen
                # (OSD: a bucket that was ok-to-stop no longer is). Nothing
                # was restarted; the staged files are inert and get
                # overwritten by the next staging. Start over next pass so
                # the policy can pick another group.
                self._drop_image_pins(group)
                self._clear()
                self._not_ready(group.label, f'{e}; a group will be chosen again next pass')
                return True
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
            if phase == PHASE_DOWN:
                try:
                    self.policy.before_switch(group)
                except StagedSwitchNotReady as e:
                    # Nothing switched yet (OSD: the group is no longer
                    # ok-to-stop, e.g. a failover brought us here long after
                    # take_down). Put it back, start over next pass.
                    self.policy.restore(group)
                    self._drop_image_pins(group)
                    self._clear()
                    self._not_ready(group.label, f'{e}; a group will be chosen again next pass')
                    return True
            self.upgrade.upgrade_info_str = f'Switching {self.policy.daemon_type} of {group.label} to {target_image}'
            self._set_phase(PHASE_SWITCHING)
            errors = self.mgr.wait_async(self._switch_all(group, target_image))
            if errors:
                why = 'switch-staged failed on ' + ', '.join(f'{k} ({v})' for k, v in errors.items())
                if self.policy.rollback_on_failure:
                    self._rollback(group, target_image, why, unswitched=set(errors))
                else:
                    self._pause_keeping_state(group, why, [f'{k}: {v}' for k, v in errors.items()])
                return True
            self._set_phase(PHASE_SWITCHED)
            phase = PHASE_SWITCHED

        if phase == PHASE_SWITCHED:
            self.upgrade.upgrade_info_str = f'Waiting for the {self.policy.daemon_type} of {group.label} on {target_version}'
            ok, why = self._wait(group, group.snapshot, target_version, timeout)
            if not ok:
                if self.policy.rollback_on_failure:
                    self._rollback(group, target_image, why)
                else:
                    self._pause_keeping_state(group, why, [why])
                return True
            logger.info('Upgrade: all %d %s of %s are back on %s; restoring',
                        len(group.daemons), self.policy.daemon_type, group.label, target_version)
            self.policy.restore(group)
            self._count_against_limit(group)
            self.mgr.wait_async(self._refresh_hosts(group.hosts))
            self._set_phase(PHASE_SETTLING)
            phase = PHASE_SETTLING

        if phase == PHASE_SETTLING:
            ok, why = self._wait_settled(group)
            if not ok:
                # Not a failure: the group is switched and back in service;
                # the next group waits for it. Ask again next pass.
                msg = f'Waiting for the {self.policy.daemon_type} of {group.label} to settle: {why}'
                logger.info('Upgrade: %s', msg)
                self.upgrade.upgrade_info_str = msg
                return True
            self._clear()
            logger.info('Upgrade: %s back on %s', group.label, target_version)
            return True

        if phase == PHASE_ROLLING_BACK:
            self._rollback(group, target_image, 'resumed after a mgr failover during rollback')
        return True

    def _pause_keeping_state(self, group: StagedGroup, reason: str, detail: List[str]) -> None:
        """A switch that did not complete, for a policy without rollback:
        pause the upgrade and keep the group's state at its current phase,
        so `ceph orch upgrade resume` retries the switch (idempotent) or
        the verification right there, once the daemons listed are dealt
        with. The group stays out of service meanwhile (OSD: noout set)."""
        logger.error('Upgrade: the staged switch of %s did not complete: %s; pausing, '
                     'the upgrade resumes at this group', group.label, reason)
        self._fail('UPGRADE_SWITCH_FAILED',
                   f'Switching {group.label} to the staged image did not complete ({reason}). '
                   f'The daemons are left as they are, on the new image where the switch went '
                   f'through; fix the ones listed, then `ceph orch upgrade resume` retries from '
                   f'this group. `cephadm switch-staged --rollback` puts a daemon back by hand',
                   detail or [reason])

    def _rollback(self, group: StagedGroup, target_image: str, reason: str,
                  unswitched: Optional[Set[str]] = None) -> None:
        """Undo a switch that did not complete: previous unit files back,
        daemons back on the previous release, group restored, upgrade paused.

        `unswitched`: daemons whose switch-staged call failed. The command
        checks everything before stopping anything, so these never
        restarted: their rollback only makes sure they run, and they are
        not expected to re-register."""
        logger.error('Upgrade: rolling back the staged switch of %s: %s', group.label, reason)
        self._set_phase(PHASE_ROLLING_BACK)
        restarted = StagedGroup(group.key, group.label,
                                [d for d in group.daemons if d.name() not in (unswitched or set())],
                                group.data)
        before = self.policy.snapshot(restarted)
        errors = self.mgr.wait_async(self._switch_all(group, target_image, rollback=True))
        detail = [f'{k}: {v}' for k, v in errors.items()]
        if not errors and restarted.daemons:
            ok, why = self._wait(restarted, before, None, self.policy.verify_timeout())
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

    @staticmethod
    def _mds_infos(fs: Dict[str, Any]) -> List[Dict[str, Any]]:
        # MDS entries of a filesystem's mdsmap (keyed by gid in the fs map)
        infos = fs['mdsmap'].get('info') or {}
        return list(infos.values())

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
            for info in self._mds_infos(fs):
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
            for info in self._mds_infos(fs):
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
        for info in self._mds_infos(fs):
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
                       for i in self._mds_infos(cur)):
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
        snapshot_gids = snapshot.get('gids') or {}
        pre_gids = {k: int(v) for k, v in snapshot_gids.items()}
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


# ======================================================================
# OSD: every OSD still to upgrade under one CRUSH bucket, when the
# monitors say the whole set can be stopped with every PG staying active
# ======================================================================

OSD_CRUSH_LEVEL_AUTO = 'auto'
# like CephadmUpgrade._wait_for_ok_to_stop: a few tries within one pass,
# then let the next pass ask again
OSD_OK_TO_STOP_TRIES = 4
OSD_OK_TO_STOP_RETRY_SECONDS = 15
# PG states in which a PG has not settled on an acting set yet
OSD_UNSETTLED_PG_STATES = frozenset(
    ('unknown', 'creating', 'peering', 'activating', 'stale', 'down', 'incomplete'))


def _natural_key(name: str) -> List[Any]:
    """host2 before host10."""
    return [(0, int(p), '') if p.isdigit() else (1, 0, p) for p in re.split(r'(\d+)', name)]


class CrushTree:
    """Read-only view of the mgr's `osd_map_tree`: buckets, their types, and
    the OSDs under each of them. Device-class shadow buckets are not part of
    that dump, so a bucket is seen once whatever classes it mixes."""

    def __init__(self, tree: Dict[str, Any]) -> None:
        self.nodes: Dict[int, Dict[str, Any]] = {}
        self.by_name: Dict[str, Dict[str, Any]] = {}
        self.osd_ids: Set[int] = set()
        has_parent: Set[int] = set()
        for n in tree.get('nodes', []) or []:
            nid = int(n['id'])
            if nid >= 0:
                self.osd_ids.add(nid)
                continue
            self.nodes[nid] = n
            self.by_name[str(n.get('name'))] = n
            for c in n.get('children', []) or []:
                has_parent.add(int(c))
        self.roots: List[int] = [i for i in self.nodes if i not in has_parent]

    def osds_under(self, bucket_id: int) -> List[int]:
        out: List[int] = []
        seen: Set[int] = set()
        stack = [bucket_id]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            if cur >= 0:
                out.append(cur)
                continue
            node = self.nodes.get(cur)
            if node:
                stack.extend(int(c) for c in (node.get('children') or []))
        return sorted(out)

    def buckets_of_type(self, btype: str) -> List[Dict[str, Any]]:
        return sorted((n for n in self.nodes.values() if n.get('type') == btype),
                      key=lambda n: _natural_key(str(n.get('name'))))

    def bucket_types_top_down(self) -> List[str]:
        """Bucket types present below a root, highest first (by type id):
        the levels `auto` tries, in order. Roots themselves are left out -
        stopping a whole hierarchy is never what an upgrade wants."""
        by_type: Dict[str, int] = {}
        for i, n in self.nodes.items():
            if i in self.roots:
                continue
            by_type[str(n.get('type'))] = max(by_type.get(str(n.get('type')), -1), int(n.get('type_id', 0)))
        return [t for t, _ in sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0]))]


class OsdStagedSwitchPolicy(StagedSwitchPolicy):
    """One CRUSH bucket per group: all the OSDs under it that still need the
    upgrade, restarted together, provided `osd ok-to-stop` on that exact set
    says every PG stays active (>= min_size) without them. The bucket type
    is `upgrade_staged_switch_osd_crush_level` (default `host`), or with
    `auto` the highest type below the root for which such a bucket exists
    right now, re-evaluated for every group: a rack whose hosts hold too many
    replicas of a pool is upgraded host by host while the others go in one
    go, and nothing is ever restarted that the monitors did not clear."""

    daemon_type = 'osd'
    # An OSD that booted on the new release may have upgraded its store
    # (BlueStore/RocksDB formats, omap layouts); starting the previous
    # ceph-osd on it is a downgrade Ceph does not support. Never roll a
    # group back: pause and resume from the same phase instead.
    rollback_on_failure = False

    def __init__(self, upgrade: 'CephadmUpgrade') -> None:
        super().__init__(upgrade)
        # (osd id, up_from) -> ceph_version_short, so `osd metadata` is asked
        # once per OSD restart, not once per verify poll
        self._versions: Dict[Tuple[int, int], str] = {}

    # -------------------------------------------------------------- options
    def _level(self) -> str:
        return str(getattr(self.mgr, 'upgrade_staged_switch_osd_crush_level', 'host') or 'host').strip().lower()

    def _noout(self) -> bool:
        return bool(getattr(self.mgr, 'upgrade_staged_switch_osd_noout', True))

    def _max_group(self) -> int:
        return max(0, int(getattr(self.mgr, 'upgrade_staged_switch_osd_max_group', 0) or 0))

    def verify_timeout(self) -> int:
        # a host of OSDs booting at once takes longer than an MDS group:
        # store open, PG load, on-disk conversions on a major upgrade
        return int(getattr(self.mgr, 'upgrade_staged_switch_osd_timeout', 600) or 600)

    # -------------------------------------------------------------- helpers
    def _tree(self) -> CrushTree:
        return CrushTree(self.mgr.get('osd_map_tree') or {})

    def _osdmap(self) -> Dict[str, Any]:
        return self.mgr.get('osd_map') or {}

    def _osds(self, osdmap: Optional[Dict[str, Any]] = None) -> Dict[int, Dict[str, Any]]:
        return {int(o['osd']): o for o in (osdmap or self._osdmap()).get('osds', []) or []}

    @staticmethod
    def _names(ids: Iterable[int]) -> List[str]:
        return [f'osd.{i}' for i in ids]

    @staticmethod
    def _up_fingerprint(osds: Dict[int, Dict[str, Any]], excluding: Iterable[int]) -> str:
        """Which OSDs outside the group the monitors see up, as a digest:
        compared between the choice of the group and the switch, it tells
        whether an OSD the ok-to-stop verdict counted on has gone down
        (or come back) meanwhile, whatever the PG stats say yet."""
        skip = set(excluding)
        up = sorted(i for i, o in osds.items() if o.get('up') and i not in skip)
        return hashlib.sha1(','.join(str(i) for i in up).encode()).hexdigest()

    def _ok_to_stop(self, ids: List[int]) -> Tuple[bool, str]:
        """Ask the monitors whether *exactly* this set can be stopped with
        every PG staying active. `max` = len(ids) keeps the mgr from adding
        OSDs of its own to the set."""
        ret, out, err = self.mgr.mon_command({
            'prefix': 'osd ok-to-stop', 'ids': [str(i) for i in ids], 'max': len(ids)})
        if ret == 0:
            return True, ''
        why = (err or '').strip() or f'osd ok-to-stop returned {ret}'
        try:
            report = json.loads(out or '{}')
            report = report.get('ok_to_stop', report) if isinstance(report, dict) else {}
            inactive = report.get('bad_become_inactive') or []
            already = report.get('bad_already_inactive') or []
            unknown = report.get('unknown_pgs') or []
            no_pool = report.get('bad_no_pool_pgs') or []
            bits = []
            if inactive:
                bits.append(f'{len(inactive)} PG(s) would become inactive')
            if already:
                bits.append(f'{len(already)} PG(s) already inactive')
            if unknown:
                bits.append(f'{len(unknown)} PG(s) unknown')
            if no_pool:
                bits.append(f'{len(no_pool)} PG(s) of a pool being created or deleted')
            if bits:
                why = ', '.join(bits)
        except (ValueError, TypeError, AttributeError):
            pass
        return False, why

    def _paused(self) -> bool:
        return bool(self.upgrade.upgrade_state is None or self.upgrade.upgrade_state.paused)

    def _version(self, osd_id: int, up_from: int) -> str:
        """ceph_version_short of osd_id as the monitors recorded it at its
        last boot (`osd metadata`), not cephadm's cache nor the mgr's
        daemon state, which can miss a boot epoch."""
        key = (osd_id, up_from)
        if key in self._versions:
            return self._versions[key]
        ret, out, err = self.mgr.mon_command({'prefix': 'osd metadata', 'id': osd_id, 'format': 'json'})
        if ret != 0:
            return ''
        try:
            md = json.loads(out or '{}')
        except ValueError:
            return ''
        v = str(md.get('ceph_version_short') or '')
        if not v and str(md.get('ceph_version', '')).startswith('ceph version '):
            v = str(md['ceph_version']).split(' ')[2]
        if v:
            self._versions[key] = v
        return v

    # --------------------------------------------------------------- policy
    def _levels(self, tree: CrushTree) -> List[str]:
        level = self._level()
        if level == OSD_CRUSH_LEVEL_AUTO:
            levels = tree.bucket_types_top_down()
            if not levels:
                raise OrchestratorError('the CRUSH map has no bucket below a root')
            return levels
        if level == 'osd':
            raise OrchestratorError(
                "mgr/cephadm/upgrade_staged_switch_osd_crush_level 'osd' makes no sense "
                "for a staged switch (one OSD at a time is the regular upgrade path)")
        present = {str(n.get('type')) for n in tree.nodes.values()}
        if level not in present:
            # a map without that level (hosts straight under the root with
            # level 'rack', say): nothing to group by, let the regular path
            # do its job rather than stall the upgrade
            logger.warning('Upgrade: mgr/cephadm/upgrade_staged_switch_osd_crush_level %r is not a '
                           'bucket type of the CRUSH map (found: %s); OSDs are upgraded the '
                           'regular way', level, ', '.join(sorted(present)) or 'none')
            return []
        if all(int(n['id']) in tree.roots for n in tree.nodes.values() if n.get('type') == level):
            raise OrchestratorError(
                f'mgr/cephadm/upgrade_staged_switch_osd_crush_level {level!r} is only used by '
                f'root buckets; a whole hierarchy cannot be switched at once')
        return [level]

    def _pending(self, need_upgrade: List[DaemonDescription], tree: CrushTree,
                 osds: Dict[int, Dict[str, Any]]) -> Dict[int, DaemonDescription]:
        """OSDs of need_upgrade this policy will handle: in the upgrade's
        CRUSH scope, on an online host, up, and placed in the CRUSH map. The
        others are left to the regular path, which has its own handling."""
        state = self.upgrade.upgrade_state
        assert state is not None
        pending: Dict[int, DaemonDescription] = {}
        for d in need_upgrade:
            if d.daemon_type == 'osd' and str(d.daemon_id).isdigit():
                pending[int(str(d.daemon_id))] = d
        if not pending:
            return {}
        if state.crush_bucket_name:
            node = tree.by_name.get(state.crush_bucket_name)
            if node is None:
                raise OrchestratorError(
                    f'CRUSH bucket {state.crush_bucket_name!r} (--crush_bucket_name) not found')
            scope = set(tree.osds_under(int(node['id'])))
            pending = {i: d for i, d in pending.items() if i in scope}
        skipped: List[str] = []
        for i in sorted(pending):
            why = None
            if pending[i].hostname in self.mgr.offline_hosts:
                why = 'host offline'
            elif not osds.get(i, {}).get('up'):
                why = 'not up'
            elif i not in tree.osd_ids:
                why = 'not in the CRUSH map'
            if why:
                skipped.append(f'osd.{i} ({why})')
                del pending[i]
        if skipped:
            logger.info('Upgrade: staged switch leaves %s to the regular upgrade path',
                        ', '.join(skipped))
        return pending

    def _pick(self, tree: CrushTree, levels: List[str], pending: Dict[int, DaemonDescription],
              limit: Optional[int]) -> Tuple[Optional[StagedGroup], List[str], bool]:
        """First bucket, highest level first, whose pending OSDs are
        ok-to-stop as a set. Returns (group, reasons it skipped the others,
        whether any bucket of these levels holds a pending OSD at all)."""
        reasons: List[str] = []
        any_bucket = False
        for btype in levels:
            for bucket in tree.buckets_of_type(btype):
                ids = [i for i in tree.osds_under(int(bucket['id'])) if i in pending]
                if not ids:
                    continue
                any_bucket = True
                if limit is not None and len(ids) > limit:
                    ids = ids[:limit]
                if self._max_group() and len(ids) > self._max_group():
                    reasons.append(f'{btype} {bucket["name"]}: {len(ids)} OSDs, more than '
                                   f'upgrade_staged_switch_osd_max_group ({self._max_group()})')
                    continue
                ok, why = self._ok_to_stop(ids)
                if ok:
                    label = f'{btype} {bucket["name"]}'
                    group = StagedGroup(str(bucket['id']), label, [pending[i] for i in ids], {
                        'bucket': bucket['name'], 'type': btype, 'osd_ids': ids,
                        'noout': False, 'committed': False,
                        'up_fingerprint': self._up_fingerprint(self._osds(), ids)})
                    return group, reasons, True
                reasons.append(f'{btype} {bucket["name"]} ({len(ids)} OSDs): {why}')
            if reasons and self._level() == OSD_CRUSH_LEVEL_AUTO:
                logger.info('Upgrade: no %s can be switched as a whole right now (%s); '
                            'trying the next CRUSH level down', btype, '; '.join(reasons[-3:]))
        return None, reasons, any_bucket

    def groups(self, need_upgrade: List[DaemonDescription]) -> List[StagedGroup]:
        state = self.upgrade.upgrade_state
        assert state is not None
        tree = self._tree()
        pending = self._pending(need_upgrade, tree, self._osds())
        if not pending:
            return []
        levels = self._levels(tree)
        if not levels:
            return []
        limit = state.remaining_count if state.remaining_count is not None and state.remaining_count > 0 else None
        reasons: List[str] = []
        for attempt in range(OSD_OK_TO_STOP_TRIES):
            if attempt:
                if self._paused():
                    raise StagedSwitchNotReady('upgrade paused')
                time.sleep(OSD_OK_TO_STOP_RETRY_SECONDS)
            group, reasons, any_bucket = self._pick(tree, levels, pending, limit)
            if group is not None:
                logger.info('Upgrade: staged switch picked %s: %d OSD(s) %s', group.label,
                            len(group.daemons), ', '.join(group.names))
                return [group]
            if not any_bucket:
                # e.g. level 'rack' on a map whose hosts hang off the root
                logger.info('Upgrade: no %s bucket holds an OSD still to upgrade; '
                            'the regular upgrade path takes over', '/'.join(levels))
                return []
        summary = (f'no {"/".join(levels)} bucket can be switched as a whole right now, every PG '
                   f'must stay active ({"; ".join(reasons[:3])}{"; ..." if len(reasons) > 3 else ""})')
        if self._some_osd_ok_to_stop_alone(tree, levels, pending):
            # Not the PGs of the previous group recovering (then no OSD
            # sharing a PG with them passes either) but buckets that cannot
            # go as a whole: a pool with an `osd` failure domain, two copies
            # of a PG on one host... Rather than wait for a verdict that
            # will not change, let the regular path upgrade OSDs one by one
            # (`osd ok-to-stop` batches) this pass; the buckets are tried
            # again next pass with fewer OSDs left in them.
            logger.info('Upgrade: %s; single OSDs are, so the regular upgrade path '
                        'handles this pass', summary)
            return []
        raise StagedSwitchNotReady(summary)

    def _some_osd_ok_to_stop_alone(self, tree: CrushTree, levels: List[str],
                                   pending: Dict[int, DaemonDescription], probes: int = 8) -> bool:
        """Whether one OSD, alone, of some bucket that was refused as a whole
        is ok-to-stop. A few probes at most, lowest level, so a large map
        does not turn into hundreds of PG scans."""
        tried = 0
        for bucket in tree.buckets_of_type(levels[-1]):
            ids = [i for i in tree.osds_under(int(bucket['id'])) if i in pending]
            if not ids:
                continue
            ok, _ = self._ok_to_stop(ids[:1])
            if ok:
                return True
            tried += 1
            if tried >= probes:
                break
        return False

    def take_down(self, group: StagedGroup) -> None:
        ids = [int(i) for i in group.data['osd_ids']]
        if self._noout():
            # Keep the monitors from marking the group out should the
            # restart outlast mon_osd_down_out_interval. Idempotent.
            self.mgr.check_mon_command({
                'prefix': 'osd set-group', 'flags': 'noout', 'who': self._names(ids)})
            group.data['noout'] = True
        group.data['committed'] = True

    def before_switch(self, group: StagedGroup) -> None:
        # Staging took a while - or a mgr failover brought us back here long
        # after take_down: make sure the window can still be opened. Not
        # called once a switch has started (PGs are degraded by then).
        ids = [int(i) for i in group.data['osd_ids']]
        # Any OSD outside the group down (or back) since the group was
        # chosen means the ok-to-stop verdict was given for another
        # cluster: start over, whatever a fresh verdict would say. ok-to-stop
        # works from PG stats, which trail an OSD failure by the heartbeat
        # grace and a stats report; the osdmap's up set is the earliest
        # the monitors can tell us about one.
        fp = group.data.get('up_fingerprint')
        if fp and self._up_fingerprint(self._osds(), ids) != fp:
            raise StagedSwitchNotReady(
                f'the set of up OSDs changed since {group.label} was chosen')
        for attempt in range(OSD_OK_TO_STOP_TRIES):
            ok, why = self._ok_to_stop(ids)
            if ok:
                return
            if attempt == OSD_OK_TO_STOP_TRIES - 1 or self._paused():
                raise StagedSwitchNotReady(f'{group.label} is no longer ok-to-stop: {why}')
            time.sleep(OSD_OK_TO_STOP_RETRY_SECONDS)

    def forget(self, state: Dict[str, Any], names: List[str]) -> None:
        gone = {int(n.split('.', 1)[1]) for n in names if n.split('.', 1)[1].isdigit()}
        data = state.get('data') or {}
        data['osd_ids'] = [i for i in data.get('osd_ids', []) if int(i) not in gone]
        snap = state.get('snapshot') or {}
        if 'up_from' in snap:
            snap['up_from'] = {k: v for k, v in snap['up_from'].items() if int(k) not in gone}

    def is_down(self, group: StagedGroup) -> bool:
        if not group.data.get('committed'):
            return False
        if not group.data.get('noout'):
            return True
        osds = self._osds()
        return all('noout' in (osds.get(int(i), {}).get('state') or []) for i in group.data['osd_ids'])

    def snapshot(self, group: StagedGroup) -> Dict[str, Any]:
        osdmap = self._osdmap()
        osds = self._osds(osdmap)
        return {'epoch': int(osdmap.get('epoch', 0)),
                'up_from': {str(i): int(osds.get(int(i), {}).get('up_from', 0)) for i in group.data['osd_ids']}}

    def verify(self, group: StagedGroup, snapshot: Dict[str, Any],
               target_version: Optional[str]) -> Tuple[bool, str]:
        ids = [int(i) for i in group.data['osd_ids']]
        snapshot_up_from = snapshot.get('up_from') or {}
        pre = {int(k): int(v) for k, v in snapshot_up_from.items()}
        osds = self._osds()
        # the osdmap first (cheap, and `osd metadata` only means something
        # once the OSD has booted again)
        for i in ids:
            o = osds.get(i)
            if not o or not o.get('up'):
                return False, f'osd.{i} is not up yet'
            if int(o.get('up_from', 0)) <= pre.get(i, 0):
                return False, f'osd.{i} has not re-registered with the monitors yet (up_from {o.get("up_from")})'
        if target_version:
            for i in ids:
                v = self._version(i, int(osds[i].get('up_from', 0)))
                if v != target_version:
                    return False, f'osd.{i} reports version {v!r}, want {target_version!r}'
        # then the PGs: the switch is done only once every PG an OSD of the
        # group holds has peered again since the OSD booted
        waiting = self._pgs_waiting(ids, osds, caught_up=False)
        if waiting:
            return False, ('not every PG of the group has peered again with its OSDs yet ('
                           + self._pgs_summary(waiting) + ')')
        return True, ''

    def settled(self, group: StagedGroup) -> Tuple[bool, str]:
        # ... and the next group is chosen only once they have caught up
        waiting = self._pgs_waiting([int(i) for i in group.data['osd_ids']], self._osds(),
                                    caught_up=True)
        if waiting:
            return False, ('its OSDs are recovering what was written while they were down ('
                           + self._pgs_summary(waiting) + ')')
        return True, ''

    def _pgs_waiting(self, ids: List[int], osds: Dict[int, Dict[str, Any]],
                     caught_up: bool) -> Dict[int, List[str]]:
        """The PGs, per OSD of the group, that do not count the OSD yet the
        way `ok-to-stop` counts OSDs: the acting set of a PG, or, for a
        degraded PG, only the OSDs of it missing no object
        (avail_no_missing). A PG holds an OSD when the OSD is in its up set.

        caught_up=False (verify, bounded by the timeout; a failure past
        it): the PG has peered again since the OSD booted - it is active,
        not peering, activating, ..., as reported since then (stats from
        before the restart say nothing about it).

        caught_up=True (settle, unbounded; never a failure): the OSD, still
        up, is in the acting set of the PG - not left out of it while it is
        backfilled - and, if the PG is degraded, misses no object of it:
        the recovery of what was written while it was down is done. An OSD
        that went down again, or a PG that is not active, is left to
        `ok-to-stop`.

        Up and version only tell that the daemons booted; until their PGs
        have peered and caught up, `ok-to-stop` counts them out and refuses
        every bucket sharing a PG with the group - with `auto`, the policy
        would descend a level, or hand the pass to the regular path, on a
        verdict that only reflects the group not being back yet. Waiting
        for both means the next group is chosen on a cluster where the OSDs
        of this one count again: the highest bucket that can go as a whole
        is the one taken."""
        up_from = {i: int(osds.get(i, {}).get('up_from', 0)) for i in ids}
        pg_stats = self.mgr.get('pg_stats') or {}
        stats = pg_stats.get('pg_stats') or []
        waiting: Dict[int, List[str]] = {}
        for pg in stats:
            mine = [int(i) for i in pg.get('up') or [] if int(i) in up_from]
            if not mine:
                continue
            states = set(str(pg.get('state') or 'unknown').split('+'))
            acting = {int(i) for i in pg.get('acting') or []}
            active = 'active' in states and not states & OSD_UNSETTLED_PG_STATES
            for i in mine:
                if not caught_up:
                    back = active and int(pg.get('reported_epoch') or 0) >= up_from[i]
                elif active and osds.get(i, {}).get('up'):
                    # shards, e.g. '3' or '3(0)' for an EC pool
                    back = i in acting and ('degraded' not in states or i in {
                        int(str(s).split('(', 1)[0]) for s in pg.get('avail_no_missing') or []})
                else:
                    back = True
                if not back:
                    waiting.setdefault(i, []).append(str(pg.get('pgid')))
        return waiting

    @staticmethod
    def _pgs_summary(waiting: Dict[int, List[str]]) -> str:
        return '; '.join(f'osd.{i}: {len(pgids)} PG(s), e.g. {", ".join(sorted(pgids)[:3])}'
                         for i, pgids in sorted(waiting.items()))

    def restore(self, group: StagedGroup) -> None:
        if group.data.get('noout'):
            ids = [int(i) for i in group.data['osd_ids']]
            try:
                self.mgr.check_mon_command({
                    'prefix': 'osd unset-group', 'flags': 'noout', 'who': self._names(ids)})
                group.data['noout'] = False
            except Exception as e:
                # The group is switched either way; a leftover per-OSD noout
                # shows up as OSD_FLAGS in `ceph health detail`.
                logger.warning('Upgrade: could not unset noout on %s (%s); run '
                               '`ceph osd unset-group noout %s` to clear it', group.label, e,
                               ' '.join(self._names(ids)))


POLICIES: Dict[str, Type[StagedSwitchPolicy]] = {
    MdsStagedSwitchPolicy.daemon_type: MdsStagedSwitchPolicy,
    OsdStagedSwitchPolicy.daemon_type: OsdStagedSwitchPolicy,
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
