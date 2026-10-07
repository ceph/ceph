"""Tests for the staged switch: the daemon-type agnostic runner (driven with
a fake policy) and the MDS policy (driven with a small monitor simulator)."""

import json
from typing import Dict, List, Tuple
from unittest import mock

from cephadm import CephadmOrchestrator
from cephadm.staged_switch import (
    CrushTree,
    MdsStagedSwitchPolicy,
    OsdStagedSwitchPolicy,
    StagedGroup,
    StagedSwitchNotReady,
    StagedSwitchPolicy,
    StagedSwitchRunner,
    policy_for,
)
from cephadm.upgrade import CephadmUpgrade, UpgradeState
from orchestrator import DaemonDescription, OrchestratorError

from .fixtures import with_host


TARGET = 'quay.io/ceph/ceph@sha256:' + 'ab' * 32
NEW, OLD = '19.2.9', '19.2.8'


class _Clock:
    # sleeping advances the clock so timeouts elapse without waiting
    now = 1000.0

    def time(self):
        return self.now

    def sleep(self, secs):
        self.now += secs


def _names_in(args):
    """the --name values of a switch-staged call (one call per host names
    every daemon of the group on that host)"""
    return [args[i + 1] for i, a in enumerate(args) if a == '--name']


def _dd(daemon_type, daemon_id, host='host1', service_name=None):
    return DaemonDescription(daemon_type=daemon_type, daemon_id=daemon_id, hostname=host,
                             service_name=service_name or f'{daemon_type}.svc',
                             container_image_name='old_image')


def _add_daemons(cephadm_module, dds):
    by_host: Dict[str, Dict[str, DaemonDescription]] = {}
    for d in dds:
        by_host.setdefault(d.hostname, {})[d.name()] = d
    for host, dm in by_host.items():
        cephadm_module.cache.update_host_daemons(host, dm)


# ---------------------------------------------------------------------------
# Generic runner, with a fake policy
# ---------------------------------------------------------------------------

class _FakeWorld:
    """What a fake daemon type looks like to its policy: each daemon has a
    'generation' that changes when it restarts, and a version."""

    def __init__(self, names, old=OLD):
        self.gen = {n: 1 for n in names}
        self.version = {n: old for n in names}
        self.down = False
        self.switched: set = set()
        self.log: List[str] = []


class _FakePolicy(StagedSwitchPolicy):
    daemon_type = 'osd'   # any type with a cephadm service will do

    def __init__(self, upgrade, world, refuse=None):
        super().__init__(upgrade)
        self.world = world
        self.refuse = refuse

    def groups(self, need_upgrade):
        if not need_upgrade:
            return []
        return [StagedGroup('g1', 'group one', list(need_upgrade), {'note': 'x'})]

    def preconditions(self, group):
        return self.refuse

    def take_down(self, group):
        self.world.log.append('take_down')
        self.world.down = True

    def is_down(self, group):
        return self.world.down

    def snapshot(self, group):
        return {'gen': dict(self.world.gen)}

    def verify(self, group, snapshot, target_version):
        for n in group.names:
            if self.world.gen[n] == snapshot['gen'].get(n):
                return False, f'{n} not restarted'
            if target_version and self.world.version[n] != target_version:
                return False, f'{n} on {self.world.version[n]}'
        return True, ''

    def restore(self, group):
        self.world.log.append('restore')
        self.world.down = False


def _runner_setup(cephadm_module, names, switch_fails=(), stage_fails=(), verify_fails=()):
    world = _FakeWorld(names)
    calls: List[Tuple[str, str, list]] = []
    staged: List[str] = []

    async def fake_run_cephadm(host, entity, command, args, **kw):
        calls.append((host, command, list(args)))
        if command == 'switch-staged':
            names = _names_in(args)
            # like the real command: a daemon that cannot be switched fails
            # the call before anything on the host is stopped
            bad = [n for n in names if n in switch_fails and '--rollback' not in args]
            if bad:
                return ([], [f'{bad[0]}: boom'], 1)
            for name in names:
                if '--rollback' in args:
                    # like the real command: nothing to put back for a daemon
                    # that was never switched, it is only made sure to run
                    if name in world.switched:
                        world.gen[name] += 1
                        world.version[name] = OLD
                        world.switched.discard(name)
                else:
                    world.gen[name] += 1
                    world.switched.add(name)
                    world.version[name] = NEW if name not in verify_fails else OLD
            return ([json.dumps([{'name': n} for n in names])], [], 0)
        return (['{}'], [], 0)

    async def fake_create_daemon(daemon_spec, reconfig=False, osd_uuid_map=None,
                                 skip_restart_for_reconfig=False, send_signal_to_daemon=None,
                                 stage=False):
        assert stage is True
        staged.append(daemon_spec.name())
        if daemon_spec.name() in stage_fails:
            raise OrchestratorError('no such image')
        return 'ok'

    async def no_refresh(hosts):
        return None

    def mon_command(cmd, inbuf=None):
        # only the image pins matter here; staging an OSD also generates
        # its config (auth get, config generate-minimal-conf)
        if cmd.get('prefix') in ('config set', 'config rm'):
            world.log.append(f"mon:{cmd.get('prefix')}:{cmd.get('who', '')}")
        return (0, '', '')

    cephadm_module.upgrade_staged_switch = True
    cephadm_module.upgrade_staged_switch_timeout = 20
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target_image', 0, target_digests=[TARGET], target_version=NEW, fail_fs=True)
    patches = [
        mock.patch("cephadm.serve.CephadmServe._run_cephadm", side_effect=fake_run_cephadm),
        mock.patch("cephadm.serve.CephadmServe._create_daemon", side_effect=fake_create_daemon),
        mock.patch.object(StagedSwitchRunner, '_refresh_hosts', side_effect=no_refresh),
        mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command", side_effect=mon_command),
        mock.patch("cephadm.staged_switch.time", _Clock()),
    ]
    return world, calls, staged, patches


def _run_fake(cephadm_module, names=('a', 'b', 'c'), refuse=None, state=None, **kw):
    dds = [_dd('osd', n) for n in names]
    world, calls, staged, patches = _runner_setup(cephadm_module, [d.name() for d in dds], **kw)
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            if state:
                cephadm_module.upgrade.upgrade_state.staged_switch = state
            policy = _FakePolicy(cephadm_module.upgrade, world, refuse)
            handled = StagedSwitchRunner(cephadm_module.upgrade, policy).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    return world, calls, staged, handled


def _switches(calls, rollback=False):
    """one entry per daemon switched (host, command, args, name)"""
    return [(c[0], c[1], c[2], n) for c in calls if c[1] == 'switch-staged'
            and (('--rollback' in c[2]) == rollback) for n in _names_in(c[2])]


def _all_switches(calls):
    return [c for c in calls if c[1] == 'switch-staged']


def test_runner_happy_path(cephadm_module: CephadmOrchestrator):
    world, calls, staged, handled = _run_fake(cephadm_module)
    assert handled is True
    # every daemon staged, then the group taken down, then every daemon switched
    assert sorted(staged) == ['osd.a', 'osd.b', 'osd.c']
    assert [e for e in world.log if not e.startswith('mon:')] == ['take_down', 'restore']
    fwd = _switches(calls)
    assert len(fwd) == 3
    assert all('--expected-image' in c[2] and TARGET in c[2] for c in fwd)
    assert _switches(calls, rollback=True) == []
    assert all(v == NEW for v in world.version.values())
    st = cephadm_module.upgrade.upgrade_state
    assert st.staged_switch == {}
    assert not st.paused


def test_runner_nothing_to_handle_falls_through(cephadm_module: CephadmOrchestrator):
    world, calls, staged, handled = _run_fake(cephadm_module, names=())
    assert handled is False
    assert staged == [] and _all_switches(calls) == []


def test_runner_precondition_pauses_before_staging(cephadm_module: CephadmOrchestrator):
    world, calls, staged, handled = _run_fake(cephadm_module, refuse='not today')
    assert handled is True
    assert staged == [] and _all_switches(calls) == []
    st = cephadm_module.upgrade.upgrade_state
    assert st.paused and st.staged_switch == {}
    assert 'not today' in cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']


def test_runner_offline_host_pauses_before_staging(cephadm_module: CephadmOrchestrator):
    dds = [_dd('osd', 'a')]
    world, calls, staged, patches = _runner_setup(cephadm_module, ['osd.a'])
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            cephadm_module.offline_hosts.add('host1')
            try:
                handled = StagedSwitchRunner(cephadm_module.upgrade,
                                             _FakePolicy(cephadm_module.upgrade, world)).run(dds, TARGET)
            finally:
                cephadm_module.offline_hosts.discard('host1')
    finally:
        for p in reversed(patches):
            p.stop()
    assert handled is True and staged == []
    assert 'offline' in cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']


def test_runner_stage_failure_touches_nothing(cephadm_module: CephadmOrchestrator):
    world, calls, staged, handled = _run_fake(cephadm_module, stage_fails=('osd.b',))
    assert 'take_down' not in world.log       # never taken down
    assert _switches(calls) == []
    assert all(g == 1 for g in world.gen.values())
    st = cephadm_module.upgrade.upgrade_state
    assert st.paused and st.staged_switch == {}
    assert 'osd.b' in ' '.join(cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['detail'])


def test_runner_take_down_failure_restores_and_pauses(cephadm_module: CephadmOrchestrator):
    dds = [_dd('osd', 'a')]
    world, calls, staged, patches = _runner_setup(cephadm_module, ['osd.a'])

    class _Refusing(_FakePolicy):
        def take_down(self, group):
            self.world.log.append('take_down')
            raise OrchestratorError('fs fail refused')

    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            StagedSwitchRunner(cephadm_module.upgrade, _Refusing(cephadm_module.upgrade, world)).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    assert staged == ['osd.a']
    assert [e for e in world.log if not e.startswith('mon:')] == ['take_down', 'restore']
    assert _switches(calls) == []
    st = cephadm_module.upgrade.upgrade_state
    assert st.paused and st.staged_switch == {}
    assert 'fs fail refused' in cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']


def test_runner_switch_failure_rolls_back(cephadm_module: CephadmOrchestrator):
    world, calls, staged, handled = _run_fake(cephadm_module, switch_fails=('osd.c',))
    assert len(_switches(calls)) == 3
    assert len(_switches(calls, rollback=True)) == 3
    assert all(v == OLD for v in world.version.values())
    # restored on the old release, after the per-daemon image pins were dropped
    assert [e for e in world.log if not e.startswith('mon:config set')] == [
        'take_down',
        'mon:config rm:osd.a', 'mon:config rm:osd.b', 'mon:config rm:osd.c',
        'restore']
    st = cephadm_module.upgrade.upgrade_state
    assert st.paused and st.staged_switch == {}
    assert 'UPGRADE_SWITCH_FAILED' in cephadm_module.health_checks


def test_runner_rollback_after_partial_switch_across_hosts(cephadm_module: CephadmOrchestrator):
    # host1 switched, host2's call failed before stopping anything: the
    # rollback restarts host1's daemon only; host2's never re-registers and
    # must not be waited for - the group is restored, not "left out of service"
    dds = [_dd('osd', 'a', host='host1'), _dd('osd', 'b', host='host2')]
    world, calls, staged, patches = _runner_setup(cephadm_module, ['osd.a', 'osd.b'], switch_fails=('osd.b',))
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'), with_host(cephadm_module, 'host2'):
            _add_daemons(cephadm_module, dds)
            StagedSwitchRunner(cephadm_module.upgrade, _FakePolicy(cephadm_module.upgrade, world)).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    assert sorted(n for _, _, _, n in _switches(calls, rollback=True)) == ['osd.a', 'osd.b']
    assert world.gen == {'osd.a': 3, 'osd.b': 1} and all(v == OLD for v in world.version.values())
    assert [e for e in world.log if not e.startswith('mon:')] == ['take_down', 'restore']
    hc = cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']
    assert 'rolled back and restored' in hc['summary'] and 'after rollback' not in ' '.join(hc['detail'])


def test_runner_verify_timeout_rolls_back(cephadm_module: CephadmOrchestrator):
    # every switch "succeeds" but osd.b comes back on the old version
    world, calls, staged, handled = _run_fake(cephadm_module, verify_fails=('osd.b',))
    assert len(_switches(calls, rollback=True)) == 3
    assert [e for e in world.log if not e.startswith('mon:')] == ['take_down', 'restore']
    assert 'osd.b' in cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']['summary']


def test_runner_resumes_after_switch(cephadm_module: CephadmOrchestrator):
    # mgr failover right after the switch: the persisted state says
    # 'switched'; nothing must be staged, taken down or switched again,
    # only verified and restored.
    names = ['osd.a', 'osd.b']
    state = {'type': 'osd', 'key': 'g1', 'label': 'group one', 'daemons': names,
             'hosts': ['host1'], 'image': TARGET, 'data': {'note': 'x'},
             'snapshot': {'gen': {n: 1 for n in names}}, 'phase': 'switched'}
    dds = [_dd('osd', 'a'), _dd('osd', 'b')]
    world, calls, staged, patches = _runner_setup(cephadm_module, names)
    world.down = True
    for n in names:          # already restarted on the new version
        world.gen[n] = 2
        world.version[n] = NEW
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            cephadm_module.upgrade.upgrade_state.staged_switch = state
            StagedSwitchRunner(cephadm_module.upgrade, _FakePolicy(cephadm_module.upgrade, world)).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    assert staged == [] and _all_switches(calls) == []
    assert [e for e in world.log if not e.startswith('mon:')] == ['restore']
    st = cephadm_module.upgrade.upgrade_state
    assert st.staged_switch == {} and not st.paused


def test_runner_resume_takes_down_again_if_needed(cephadm_module: CephadmOrchestrator):
    # failover between recording 'down' and the actual take-down: the
    # group is found up again and must be taken down before any switch
    names = ['osd.a']
    state = {'type': 'osd', 'key': 'g1', 'label': 'group one', 'daemons': names,
             'hosts': ['host1'], 'image': TARGET, 'data': {},
             'snapshot': {'gen': {'osd.a': 1}}, 'phase': 'down'}
    world, calls, staged, handled = _run_fake(cephadm_module, names=('a',), state=state)
    assert staged == []
    assert [e for e in world.log if not e.startswith('mon:')] == ['take_down', 'restore']
    assert len(_switches(calls)) == 1
    assert not cephadm_module.upgrade.upgrade_state.paused


def test_runner_not_ready_from_groups_waits_without_pausing(cephadm_module: CephadmOrchestrator):
    dds = [_dd('osd', 'a')]
    world, calls, staged, patches = _runner_setup(cephadm_module, ['osd.a'])

    class _Waiting(_FakePolicy):
        def groups(self, need_upgrade):
            raise StagedSwitchNotReady('PGs recovering')

    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            handled = StagedSwitchRunner(cephadm_module.upgrade, _Waiting(cephadm_module.upgrade, world)).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    # handled (the regular path must not take over), nothing staged, not paused
    assert handled is True and staged == [] and _all_switches(calls) == []
    st = cephadm_module.upgrade.upgrade_state
    assert not st.paused and st.staged_switch == {}
    assert 'UPGRADE_STAGE_FAILED' not in cephadm_module.health_checks
    assert 'PGs recovering' in cephadm_module.upgrade.upgrade_info_str


def test_runner_not_ready_at_take_down_starts_over(cephadm_module: CephadmOrchestrator):
    # staged, but the policy cannot take the group down any more: the
    # staged files are left behind, the image pins dropped, the state
    # cleared, and the upgrade keeps running (next pass picks again)
    dds = [_dd('osd', 'a'), _dd('osd', 'b')]
    world, calls, staged, patches = _runner_setup(cephadm_module, ['osd.a', 'osd.b'])

    class _Changed(_FakePolicy):
        def take_down(self, group):
            self.world.log.append('take_down')
            raise StagedSwitchNotReady('no longer ok-to-stop')

    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            handled = StagedSwitchRunner(cephadm_module.upgrade, _Changed(cephadm_module.upgrade, world)).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    assert handled is True
    assert sorted(staged) == ['osd.a', 'osd.b'] and _all_switches(calls) == []
    assert [e for e in world.log if not e.startswith('mon:config set')] == [
        'take_down', 'mon:config rm:osd.a', 'mon:config rm:osd.b']   # no restore: nothing was taken down
    st = cephadm_module.upgrade.upgrade_state
    assert not st.paused and st.staged_switch == {}
    assert 'no longer ok-to-stop' in cephadm_module.upgrade.upgrade_info_str


def test_runner_config_error_pauses(cephadm_module: CephadmOrchestrator):
    dds = [_dd('osd', 'a')]
    world, calls, staged, patches = _runner_setup(cephadm_module, ['osd.a'])

    class _Misconfigured(_FakePolicy):
        def groups(self, need_upgrade):
            raise OrchestratorError('no such CRUSH bucket type')

    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            handled = StagedSwitchRunner(cephadm_module.upgrade, _Misconfigured(cephadm_module.upgrade, world)).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    assert handled is True and staged == []
    st = cephadm_module.upgrade.upgrade_state
    assert st.paused
    assert 'no such CRUSH bucket type' in cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']


def test_runner_counts_switched_daemons_against_limit(cephadm_module: CephadmOrchestrator):
    dds = [_dd('osd', 'a'), _dd('osd', 'b'), _dd('osd', 'c')]
    world, calls, staged, patches = _runner_setup(cephadm_module, [d.name() for d in dds])
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            st = cephadm_module.upgrade.upgrade_state
            st.total_count, st.remaining_count = 10, 10
            # osd.c is already on the target image and only redeployed: not counted
            StagedSwitchRunner(cephadm_module.upgrade, _FakePolicy(cephadm_module.upgrade, world)).run(
                dds, TARGET, redeploy_only=['osd.c'])
            assert st.remaining_count == 8
            # --limit reached: the runner steps aside
            st.remaining_count = 0
            handled = StagedSwitchRunner(cephadm_module.upgrade, _FakePolicy(cephadm_module.upgrade, world)).run(dds, TARGET)
            assert handled is False
    finally:
        for p in reversed(patches):
            p.stop()


class _Settling(_FakePolicy):
    """Settles once world.settle_left calls have answered "not yet" (None:
    not before the test says so)."""

    def settled(self, group):
        left = getattr(self.world, 'settle_left', 0)
        if left is None:
            return False, 'catching up'
        if left > 0:
            self.world.settle_left = left - 1
            return False, 'catching up'
        return True, ''


def test_runner_settling_waits_without_pausing(cephadm_module: CephadmOrchestrator):
    # switched, verified and restored, but not settled: the runner keeps the
    # group, pass after pass, without pausing and without a timeout, and lets
    # go of it once it has settled - counted against --limit once
    dds = [_dd('osd', 'a'), _dd('osd', 'b'), _dd('osd', 'c')]
    world, calls, staged, patches = _runner_setup(cephadm_module, [d.name() for d in dds])
    world.settle_left = None
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            st = cephadm_module.upgrade.upgrade_state
            st.total_count, st.remaining_count = 10, 10

            def one_pass():
                policy = _Settling(cephadm_module.upgrade, world)
                return StagedSwitchRunner(cephadm_module.upgrade, policy).run(dds, TARGET)

            assert one_pass() is True
            assert st.staged_switch.get('phase') == 'settling' and not st.paused
            assert [e for e in world.log if not e.startswith('mon:')] == ['take_down', 'restore']
            assert 'to settle: catching up' in cephadm_module.upgrade.upgrade_info_str
            assert st.remaining_count == 7 and len(_switches(calls)) == 3
            # a pass later (a mgr failover between the two, say): still
            # settling, nothing staged or switched again, nothing counted again
            assert one_pass() is True
            assert st.staged_switch.get('phase') == 'settling' and not st.paused
            assert len(staged) == 3 and len(_switches(calls)) == 3 and st.remaining_count == 7
            world.settle_left = 2                # settles within the next pass
            assert one_pass() is True
            assert st.staged_switch == {} and not st.paused and st.remaining_count == 7
            assert 'UPGRADE_SWITCH_FAILED' not in cephadm_module.health_checks
    finally:
        for p in reversed(patches):
            p.stop()


def test_do_upgrade_hands_a_settling_group_to_the_runner(cephadm_module: CephadmOrchestrator):
    # The hook in _do_upgrade: once the last group is switched nothing is
    # left to upgrade, but while that group settles the runner still gets
    # the pass (and the phase of the type is not completed); with no group
    # of the type in the state, it does not.
    dds = [_dd('mds', 'a')]

    def detect(daemons, *args, **kwargs):
        return (False, [], [], len(daemons))

    for state, expect in (({'type': 'mds', 'phase': 'settling', 'daemons': ['mds.a']}, 1),
                          ({}, 0)):
        cephadm_module.upgrade.upgrade_state = UpgradeState(
            'target_image', 'pid', target_id='image_id', target_digests=[TARGET],
            target_version=NEW, daemon_types=['mds'], fail_fs=True, staged_switch=dict(state))
        cephadm_module.upgrade_staged_switch = True
        cephadm_module.upgrade_staged_switch_types = 'mds'
        run = mock.MagicMock(return_value=True)
        set_images = mock.MagicMock()
        patches = [
            mock.patch.object(CephadmUpgrade, '_detect_need_upgrade', side_effect=detect),
            mock.patch.object(CephadmUpgrade, '_upgrade_daemons'),
            mock.patch.object(CephadmUpgrade, '_update_upgrade_progress'),
            mock.patch.object(CephadmUpgrade, '_set_container_images', set_images),
            mock.patch.object(CephadmUpgrade, '_complete_mds_upgrade'),
            mock.patch.object(CephadmUpgrade, '_mark_upgrade_complete'),
            mock.patch.object(CephadmUpgrade, 'get_distinct_container_image_settings', return_value={}),
            mock.patch.object(StagedSwitchRunner, 'run', run),
            mock.patch("cephadm.serve.CephadmServe._run_cephadm",
                       new_callable=mock.AsyncMock, return_value=(['{}'], [], 0)),
            mock.patch("cephadm.module.CephadmOrchestrator.lookup_release_name", return_value='tentacle'),
            mock.patch("cephadm.module.CephadmOrchestrator.get_active_mgr_digests", return_value=[TARGET]),
            mock.patch("cephadm.module.CephadmOrchestrator.version",
                       new_callable=mock.PropertyMock, return_value=f'ceph version {NEW} (hash)'),
            mock.patch("cephadm.module.CephadmOrchestrator.set_container_image"),
            mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command",
                       return_value=(0, '{}', '')),
            mock.patch("cephadm.CephadmOrchestrator.get", side_effect=lambda what: {
                'min_mon_release': 19, 'require_osd_release': 'tentacle', 'have_local_config_map': True,
                'filesystems': []}),
        ]
        for p in patches:
            p.start()
        try:
            with with_host(cephadm_module, 'host1'):
                with mock.patch("cephadm.module.HostCache.get_daemons", return_value=dds):
                    cephadm_module.upgrade._do_upgrade()
        finally:
            for p in reversed(patches):
                p.stop()
        assert run.call_count == expect
        if expect:
            assert run.call_args.args[0] == []
            assert not any(c.args[0] == 'mds' for c in set_images.call_args_list)


class _OneByOne(_FakePolicy):
    """One daemon per group; names every daemon still to upgrade for
    staging ahead."""

    def groups(self, need_upgrade):
        return [StagedGroup(f'g-{need_upgrade[0].name()}', need_upgrade[0].name(),
                            [need_upgrade[0]], {})] if need_upgrade else []

    def stage_ahead(self, need_upgrade):
        return list(need_upgrade)


def _ahead_run(cephadm_module, dds, world, passes, policy_cls=_OneByOne):
    """Run `passes` passes, each handing the runner the daemons not
    switched yet."""
    for _ in range(passes):
        left = [d for d in dds if d.name() not in world.switched]
        StagedSwitchRunner(cephadm_module.upgrade, policy_cls(cephadm_module.upgrade, world)).run(left, TARGET)


def test_runner_stages_ahead_once_then_groups_skip_it(cephadm_module: CephadmOrchestrator):
    # all three staged at the first pass, hosts in parallel, before the first
    # group; then each group switches without staging again
    dds = [_dd('osd', 'a'), _dd('osd', 'b', host='host2'), _dd('osd', 'c')]
    world, calls, staged, patches = _runner_setup(cephadm_module, [d.name() for d in dds])
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'), with_host(cephadm_module, 'host2'):
            _add_daemons(cephadm_module, dds)
            _ahead_run(cephadm_module, dds, world, 1)
            assert sorted(staged) == ['osd.a', 'osd.b', 'osd.c']
            assert world.switched == {'osd.a'}
            st = cephadm_module.upgrade.upgrade_state
            assert st.staged_ahead['osd']['image'] == TARGET
            assert sorted(st.staged_ahead['osd']['daemons']) == ['osd.a', 'osd.b', 'osd.c']
            _ahead_run(cephadm_module, dds, world, 2)
            assert world.switched == {'osd.a', 'osd.b', 'osd.c'}
            assert sorted(staged) == ['osd.a', 'osd.b', 'osd.c']          # never staged twice
            assert not st.paused
            # the state survives a mgr failover (json round trip)
            restored = UpgradeState.from_json(json.loads(json.dumps(st.to_json())))
            assert restored and restored.staged_ahead == st.staged_ahead
    finally:
        for p in reversed(patches):
            p.stop()


def test_runner_restages_what_changed_since_staged_ahead(cephadm_module: CephadmOrchestrator):
    # osd.b's generated configuration changed after it was staged ahead:
    # its group stages it again, the others are not
    dds = [_dd('osd', 'a'), _dd('osd', 'b'), _dd('osd', 'c')]
    world, calls, staged, patches = _runner_setup(cephadm_module, [d.name() for d in dds])
    fps = {n: 'v1' for n in ('osd.a', 'osd.b', 'osd.c')}
    patches.append(mock.patch.object(
        StagedSwitchRunner, '_stage_fingerprint',
        side_effect=lambda spec, image: fps[spec.name()]))
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            _ahead_run(cephadm_module, dds, world, 1)
            fps['osd.b'] = 'v2'
            _ahead_run(cephadm_module, dds, world, 2)
            assert sorted(staged) == ['osd.a', 'osd.b', 'osd.b', 'osd.c']
            assert world.switched == {'osd.a', 'osd.b', 'osd.c'}
    finally:
        for p in reversed(patches):
            p.stop()


def test_runner_stage_ahead_failure_is_left_to_the_group(cephadm_module: CephadmOrchestrator):
    # staging osd.b ahead fails: not recorded, no pause; its group stages it
    # again (and would pause, as usual, if that failed too)
    dds = [_dd('osd', 'a'), _dd('osd', 'b'), _dd('osd', 'c')]
    world, calls, staged, patches = _runner_setup(cephadm_module, [d.name() for d in dds])
    attempts: List[str] = []

    async def flaky(daemon_spec, reconfig=False, osd_uuid_map=None, skip_restart_for_reconfig=False,
                    send_signal_to_daemon=None, stage=False):
        attempts.append(daemon_spec.name())
        if daemon_spec.name() == 'osd.b' and attempts.count('osd.b') == 1:
            raise OrchestratorError('registry hiccup')
        staged.append(daemon_spec.name())
        return 'ok'
    patches = [p for p in patches if getattr(p, 'attribute', None) != '_create_daemon'] + [
        mock.patch("cephadm.serve.CephadmServe._create_daemon", side_effect=flaky)]
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            _ahead_run(cephadm_module, dds, world, 1)
            st = cephadm_module.upgrade.upgrade_state
            assert sorted(st.staged_ahead['osd']['daemons']) == ['osd.a', 'osd.c']
            assert not st.paused
            _ahead_run(cephadm_module, dds, world, 2)
            assert attempts.count('osd.b') == 2 and attempts.count('osd.a') == 1
            assert world.switched == {'osd.a', 'osd.b', 'osd.c'} and not st.paused
    finally:
        for p in reversed(patches):
            p.stop()


def test_runner_stage_ahead_can_be_disabled(cephadm_module: CephadmOrchestrator):
    dds = [_dd('osd', 'a'), _dd('osd', 'b')]
    world, calls, staged, patches = _runner_setup(cephadm_module, [d.name() for d in dds])
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            cephadm_module.upgrade_staged_switch_stage_ahead = False
            try:
                _ahead_run(cephadm_module, dds, world, 1)
                assert staged == ['osd.a']                       # only the group
                assert cephadm_module.upgrade.upgrade_state.staged_ahead == {}
            finally:
                cephadm_module.upgrade_staged_switch_stage_ahead = True
    finally:
        for p in reversed(patches):
            p.stop()


def test_runner_no_stage_ahead_by_default(cephadm_module: CephadmOrchestrator):
    # a policy that does not name daemons to stage ahead (the MDS one) stages
    # each group when it is picked, as before
    world, calls, staged, handled = _run_fake(cephadm_module, names=('a', 'b', 'c'))
    assert handled is True and sorted(staged) == ['osd.a', 'osd.b', 'osd.c']
    assert cephadm_module.upgrade.upgrade_state.staged_ahead == {}


def test_runner_state_survives_json(cephadm_module: CephadmOrchestrator):
    st = UpgradeState('t', 'pid', staged_switch={'type': 'mds', 'phase': 'down', 'data': {'fscids': [1]}},
                      staged_ahead={'osd': {'image': 'i', 'daemons': {'osd.1': 'f'}}})
    restored = UpgradeState.from_json(json.loads(json.dumps(st.to_json())))
    assert restored and restored.staged_switch == {'type': 'mds', 'phase': 'down', 'data': {'fscids': [1]}}
    assert restored.staged_ahead == {'osd': {'image': 'i', 'daemons': {'osd.1': 'f'}}}
    assert UpgradeState('t', 'pid').staged_switch == {} and UpgradeState('t', 'pid').staged_ahead == {}


def test_policy_for_honours_options(cephadm_module: CephadmOrchestrator):
    up = cephadm_module.upgrade
    up.upgrade_state = UpgradeState('t', 'pid', fail_fs=True)
    cephadm_module.upgrade_staged_switch = False
    assert policy_for(up, 'mds') is None
    cephadm_module.upgrade_staged_switch = True
    cephadm_module.upgrade_staged_switch_types = 'mds'
    assert isinstance(policy_for(up, 'mds'), MdsStagedSwitchPolicy)
    assert policy_for(up, 'osd') is None                    # not listed
    cephadm_module.upgrade_staged_switch_types = 'mds, osd'
    assert isinstance(policy_for(up, 'osd'), OsdStagedSwitchPolicy)
    assert isinstance(policy_for(up, 'mds'), MdsStagedSwitchPolicy)
    cephadm_module.upgrade_staged_switch_types = 'mds, rgw'
    assert policy_for(up, 'rgw') is None                    # listed, no policy
    up.upgrade_state = UpgradeState('t', 'pid', fail_fs=False)
    assert policy_for(up, 'mds') is None                    # needs fail_fs


# ---------------------------------------------------------------------------
# MDS policy, with a monitor simulator
# ---------------------------------------------------------------------------

class _FakeMons:
    """Just enough of the monitors for the MDS policy: an fsmap with ranks
    and standbys, `fs fail` / `fs set joinable`, `mds metadata`, and
    daemon restarts that come back as standbys with a new gid."""

    def __init__(self, names=('a', 'b', 'c'), ranks=2, old=OLD, new=NEW, extra_standbys=()):
        self.fs_name, self.fscid, self.ranks = 'cephfs', 1, ranks
        self.old, self.new = old, new
        self.names = list(names)
        self.version = {n: old for n in names}
        self.gid = {n: i + 1 for i, n in enumerate(names)}
        for n, v in extra_standbys:          # standbys cephadm does not manage
            self.version[n] = v
            self.gid[n] = len(self.gid) + 1
        self.rank = {self.names[i]: i for i in range(ranks)}
        self.flags = 0
        self.commands: List[dict] = []
        self.tells: List[Tuple[str, str, dict]] = []
        self.restarts: List[Tuple[str, str]] = []

    def fsmap(self):
        info = {f'gid_{self.gid[n]}': {'gid': self.gid[n], 'name': n, 'rank': r, 'state': 'up:active'}
                for n, r in self.rank.items()}
        standbys = [{'gid': self.gid[n], 'name': n, 'join_fscid': self.fscid, 'state': 'up:standby'}
                    for n in self.version if n not in self.rank]
        return {'filesystems': [
            {'id': self.fscid,
             'mdsmap': {'fs_name': self.fs_name, 'max_mds': self.ranks, 'flags': self.flags,
                        'up': {f'mds_{r}': self.gid[n] for n, r in self.rank.items()},
                        'in': list(range(self.ranks)), 'info': info}}],
            'standbys': standbys}

    def get(self, what):
        return self.fsmap() if what == 'fs_map' else None

    def mon_command(self, cmd, inbuf=None):
        self.commands.append(cmd)
        p = cmd.get('prefix')
        if p == 'fs fail':
            self.rank = {}
            self.flags |= 1                      # CEPH_MDSMAP_NOT_JOINABLE
            return (0, '', '')
        if p == 'fs set' and cmd.get('var') == 'joinable':
            self.flags &= ~1
            # standbys are picked by gid order, like the real monitors
            for r, n in enumerate(sorted((n for n in self.version if n not in self.rank),
                                         key=lambda n: self.gid[n])[:self.ranks]):
                self.rank[n] = r
            return (0, '', '')
        if p == 'mds metadata':
            return (0, json.dumps([{'name': n, 'ceph_version_short': v}
                                   for n, v in self.version.items()]), '')
        if p == 'versions':
            return (0, '{}', '')
        return (0, '', '')

    def tell(self, daemon_type, daemon_id, cmd, inbuf=None):
        self.tells.append((daemon_type, daemon_id, cmd))
        return (0, '', '')

    def restart(self, name, version):
        self.restarts.append((name, version))
        self.gid[name] += 100
        self.version[name] = version
        self.rank.pop(name, None)

    def cmds(self, prefix, **kw):
        return [c for c in self.commands if c.get('prefix') == prefix
                and all(c.get(k) == v for k, v in kw.items())]


def _mds_setup(cephadm_module, mons, switch_fails=()):
    calls: List[Tuple[str, str, list]] = []
    staged: List[str] = []

    async def fake_run_cephadm(host, entity, command, args, **kw):
        calls.append((host, command, list(args)))
        if command == 'switch-staged':
            names = [n.split('.', 1)[1] for n in _names_in(args)]
            bad = [n for n in names if n in switch_fails and '--rollback' not in args]
            if bad:
                return ([], [f'mds.{bad[0]}: boom'], 1)
            for name in names:
                mons.restart(name, mons.old if '--rollback' in args else mons.new)
            return ([json.dumps([{'name': f'mds.{n}'} for n in names])], [], 0)
        return (['{}'], [], 0)

    async def fake_create_daemon(daemon_spec, reconfig=False, osd_uuid_map=None,
                                 skip_restart_for_reconfig=False, send_signal_to_daemon=None,
                                 stage=False):
        assert stage is True
        staged.append(daemon_spec.name())
        return 'ok'

    async def no_refresh(hosts):
        return None

    cephadm_module.upgrade_staged_switch = True
    cephadm_module.upgrade_staged_switch_timeout = 20
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target_image', 0, target_digests=[TARGET], target_version=mons.new, fail_fs=True)
    patches = [
        mock.patch("cephadm.serve.CephadmServe._run_cephadm", side_effect=fake_run_cephadm),
        mock.patch("cephadm.serve.CephadmServe._create_daemon", side_effect=fake_create_daemon),
        mock.patch.object(StagedSwitchRunner, '_refresh_hosts', side_effect=no_refresh),
        mock.patch("cephadm.CephadmOrchestrator.get", side_effect=mons.get),
        mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command", side_effect=mons.mon_command),
        mock.patch("cephadm.module.CephadmOrchestrator.mon_command", side_effect=mons.mon_command),
        mock.patch("cephadm.module.CephadmOrchestrator.tell_command", side_effect=mons.tell),
        mock.patch("cephadm.staged_switch.time", _Clock()),
        mock.patch("cephadm.upgrade.time", _Clock()),
    ]
    return calls, staged, patches


def _mds_dds(mons):
    return [_dd('mds', n, service_name='mds.cephfs') for n in mons.names]


def _run_mds(cephadm_module, mons, state=None, **kw):
    calls, staged, patches = _mds_setup(cephadm_module, mons, **kw)
    dds = _mds_dds(mons)
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            if state:
                cephadm_module.upgrade.upgrade_state.staged_switch = state
            policy = policy_for(cephadm_module.upgrade, 'mds')
            assert policy is not None
            handled = StagedSwitchRunner(cephadm_module.upgrade, policy).run(dds, TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    return calls, staged, handled


def test_mds_stages_then_fails_then_switches(cephadm_module: CephadmOrchestrator):
    mons = _FakeMons()
    calls, staged, handled = _run_mds(cephadm_module, mons)
    assert handled is True
    # every MDS of the service was staged ...
    assert sorted(staged) == ['mds.a', 'mds.b', 'mds.c']
    # ... before the filesystem was failed, once; no journal flush by default
    assert len(mons.cmds('fs fail')) == 1
    # then every MDS was switched with the expected image, in one pass
    assert len(_switches(calls)) == 3
    assert _switches(calls, rollback=True) == []
    # every daemon came back on the new version, then the fs was re-joined
    assert all(v == mons.new for v in mons.version.values())
    assert len(mons.cmds('fs set', var='joinable', val='true')) == 1
    assert len(mons.rank) == 2 and all(mons.version[n] == mons.new for n in mons.rank)
    st = cephadm_module.upgrade.upgrade_state
    assert st.staged_switch == {}
    assert st.fs_failed_for_upgrade == []
    assert not st.paused


def test_mds_flush_journal_by_default(cephadm_module: CephadmOrchestrator):
    # By default every active rank is flushed, one at a time, before
    # `fs fail`; standbys are not.
    mons = _FakeMons()
    calls, staged, handled = _run_mds(cephadm_module, mons)
    flushed = [t[1] for t in mons.tells if t[2].get('prefix') == 'flush journal']
    assert sorted(flushed) == ['a', 'b']
    fail_at = mons.commands.index(mons.cmds('fs fail')[0])
    assert fail_at >= 0 and len(mons.cmds('fs fail')) == 1
    assert not cephadm_module.upgrade.upgrade_state.paused


def test_mds_flush_journal_can_be_disabled(cephadm_module: CephadmOrchestrator):
    mons = _FakeMons()
    cephadm_module.upgrade_staged_switch_flush_mds_journal = False
    calls, staged, handled = _run_mds(cephadm_module, mons)
    assert not [t for t in mons.tells if t[2].get('prefix') == 'flush journal']
    assert len(mons.cmds('fs fail')) == 1
    assert not cephadm_module.upgrade.upgrade_state.paused


def test_mds_switch_failure_rolls_back_and_rejoins(cephadm_module: CephadmOrchestrator):
    mons = _FakeMons()
    calls, staged, handled = _run_mds(cephadm_module, mons, switch_fails=('c',))
    assert len(_switches(calls)) == 3 and len(_switches(calls, rollback=True)) == 3
    assert all(v == mons.old for v in mons.version.values())
    assert len(mons.cmds('fs set', var='joinable', val='true')) == 1
    assert len(mons.rank) == 2
    st = cephadm_module.upgrade.upgrade_state
    assert st.paused and st.staged_switch == {} and st.fs_failed_for_upgrade == []
    assert 'UPGRADE_SWITCH_FAILED' in cephadm_module.health_checks


def test_mds_old_pinned_standby_blocks_rejoin(cephadm_module: CephadmOrchestrator):
    # A standby pinned to the filesystem that cephadm does not manage still
    # runs the old version: it would take rank 0 and the monitors would then
    # refuse every new-version standby. The policy must not re-join on that.
    mons = _FakeMons(extra_standbys=[('legacy', OLD)])
    calls, staged, handled = _run_mds(cephadm_module, mons)
    assert len(_switches(calls, rollback=True)) == 3
    assert cephadm_module.upgrade.upgrade_state.paused
    assert 'legacy' in cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']['summary']


def test_mds_fail_fs_refused_rejoins_and_pauses(cephadm_module: CephadmOrchestrator):
    mons = _FakeMons()
    real = mons.mon_command

    def refuse_fail(cmd, inbuf=None):
        if cmd.get('prefix') == 'fs fail':
            mons.commands.append(cmd)
            return (1, '', 'refused')
        return real(cmd, inbuf)

    mons.mon_command = refuse_fail                     # type: ignore[method-assign]
    calls, staged, handled = _run_mds(cephadm_module, mons)
    assert len(staged) == 3
    assert _switches(calls) == [] and mons.restarts == []
    st = cephadm_module.upgrade.upgrade_state
    assert st.paused and st.staged_switch == {}
    assert 'UPGRADE_STAGE_FAILED' in cephadm_module.health_checks


def test_mds_resume_refails_joinable_fs(cephadm_module: CephadmOrchestrator):
    # mgr failover between recording 'down' and the actual `fs fail`: the
    # filesystem is still joinable and must be failed before any switch.
    mons = _FakeMons()
    state = {'type': 'mds', 'key': '1', 'label': 'filesystem cephfs',
             'daemons': ['mds.a', 'mds.b', 'mds.c'], 'hosts': ['host1'], 'image': TARGET,
             'data': {'fscids': [1], 'fs_names': ['cephfs']},
             'snapshot': {'gids': {'a': 1, 'b': 2, 'c': 3}}, 'phase': 'down'}
    calls, staged, handled = _run_mds(cephadm_module, mons, state=state)
    assert staged == []
    assert len(mons.cmds('fs fail')) == 1
    fail_at = mons.commands.index(mons.cmds('fs fail')[0])
    assert all(v == mons.new for v in mons.version.values())
    assert fail_at < len(mons.commands)
    assert not cephadm_module.upgrade.upgrade_state.paused


def test_mds_precondition_fewer_daemons_than_ranks(cephadm_module: CephadmOrchestrator):
    # The service has 2 MDS but the filesystem has 3 ranks; the third rank is
    # held by a daemon cephadm does not manage for this service. After the
    # switch it would be handed back to a standby outside the group (not
    # switched, still on the old release): refuse before taking anything down.
    mons = _FakeMons(names=('a', 'b'), ranks=2, extra_standbys=[('outsider', OLD)])
    mons.rank['outsider'] = 2
    mons.ranks = 3
    calls, staged, handled = _run_mds(cephadm_module, mons)
    assert staged == []
    assert mons.cmds('fs fail') == []
    assert _all_switches(calls) == []
    assert cephadm_module.upgrade.upgrade_state.paused
    assert '2 MDS daemon(s) for 3 rank(s)' in \
        cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']


def test_mds_precondition_rank_in_other_fs(cephadm_module: CephadmOrchestrator):
    # mds.c belongs to service mds.cephfs (so it is part of the group) but
    # is not in need_upgrade and currently holds a rank in another
    # filesystem: switching it would restart an active MDS of a live fs.
    mons = _FakeMons()
    fsmap = mons.fsmap()
    fsmap['standbys'] = [s for s in fsmap['standbys'] if s['name'] != 'c']
    fsmap['filesystems'].append({'id': 2, 'mdsmap': {
        'fs_name': 'other', 'max_mds': 1, 'flags': 0, 'up': {'mds_0': 3}, 'in': [0],
        'info': {'gid_3': {'gid': 3, 'name': 'c', 'rank': 0, 'state': 'up:active'}}}})
    calls, staged, patches = _mds_setup(cephadm_module, mons)
    patches = [p for p in patches if p.attribute != 'get'] + [
        mock.patch("cephadm.CephadmOrchestrator.get", side_effect=lambda w: fsmap if w == 'fs_map' else None)]
    dds = _mds_dds(mons)
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            policy = policy_for(cephadm_module.upgrade, 'mds')
            StagedSwitchRunner(cephadm_module.upgrade, policy).run(
                [d for d in dds if d.daemon_id != 'c'], TARGET)
    finally:
        for p in reversed(patches):
            p.stop()
    assert staged == []
    assert 'mds.c currently serves filesystem other' in \
        cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']


def test_do_upgrade_uses_staged_switch_for_mds(cephadm_module: CephadmOrchestrator):
    # The hook in _do_upgrade: with the option on, the MDS phase goes through
    # the runner and the regular prepare/redeploy path is not entered.
    mons = _FakeMons()
    calls, staged, patches = _mds_setup(cephadm_module, mons)
    dds = _mds_dds(mons)
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target_image', 'pid', target_id='image_id', target_digests=[TARGET],
        target_version=mons.new, daemon_types=['mds'], fail_fs=True)

    def detect(daemons, *args, **kwargs):
        return (False, [(d, False) for d in daemons if d.daemon_type == 'mds'
                        and mons.version[d.daemon_id] != mons.new], [], 0)

    prepare = mock.MagicMock(return_value=True)
    upgrade_daemons = mock.MagicMock()
    patches += [
        mock.patch.object(CephadmUpgrade, '_detect_need_upgrade', side_effect=detect),
        mock.patch.object(CephadmUpgrade, '_prepare_for_mds_upgrade', prepare),
        mock.patch.object(CephadmUpgrade, '_upgrade_daemons', upgrade_daemons),
        mock.patch.object(CephadmUpgrade, '_update_upgrade_progress'),
        mock.patch.object(CephadmUpgrade, 'get_distinct_container_image_settings', return_value={}),
        mock.patch("cephadm.module.CephadmOrchestrator.lookup_release_name", return_value='tentacle'),
        mock.patch("cephadm.module.CephadmOrchestrator.get_active_mgr_digests", return_value=[TARGET]),
        mock.patch("cephadm.module.CephadmOrchestrator.version",
                   new_callable=mock.PropertyMock, return_value=f'ceph version {mons.new} (hash)'),
        mock.patch("cephadm.module.CephadmOrchestrator.set_container_image"),
    ]
    real_get = mons.get

    def get(what):
        if what == 'fs_map':
            return real_get(what)
        return {'min_mon_release': 19, 'require_osd_release': 'tentacle', 'have_local_config_map': True}

    patches = [p for p in patches if p.attribute != 'get'] + [
        mock.patch("cephadm.CephadmOrchestrator.get", side_effect=get)]
    for p in patches:
        p.start()
    try:
        with with_host(cephadm_module, 'host1'):
            _add_daemons(cephadm_module, dds)
            with mock.patch("cephadm.module.HostCache.get_daemons", return_value=dds):
                cephadm_module.upgrade._do_upgrade()
    finally:
        for p in reversed(patches):
            p.stop()
    prepare.assert_not_called()
    # the regular redeploy path saw nothing to do for any type
    assert all(c.args[0] == [] for c in upgrade_daemons.call_args_list)
    assert sorted(staged) == ['mds.a', 'mds.b', 'mds.c']
    assert len(mons.cmds('fs fail')) == 1
    assert all(v == mons.new for v in mons.version.values())


# ---------------------------------------------------------------------------
# OSD policy, with a monitor simulator (CRUSH tree, osdmap, PGs)
# ---------------------------------------------------------------------------

class _FakeOsdMons:
    """Just enough of the monitors for the OSD policy: a CRUSH tree, an
    osdmap with up / up_from / per-OSD flags, `osd ok-to-stop` evaluated
    against a small PG model (acting sets and min_size; an OSD that just
    restarted has no complete copy until recover() is called), `osd
    set-group` / `unset-group`, `osd metadata`, and the mgr's `pg_stats`.

    A restarted OSD rejoins the PGs it holds (is back in their acting sets)
    `join_secs` of simulated time after its restart, and has caught up with
    the writes it missed `catch_up_secs` later (None: only once recover()
    is called). Until it has rejoined, the PGs it is in the up set of report
    `peering` without it; until it has caught up, they are degraded and
    leave it out of avail_no_missing. ok-to-stop counts, like the real one,
    the acting set of a PG, or only its avail_no_missing OSDs if it is
    degraded. `never_join` keeps some OSDs out, `backfilling` keeps some
    out of the acting set of active PGs, `stats_epoch` freezes the epoch
    the PG stats were reported at.

    Default topology: root default -> racks r1, r2, r3 -> two hosts each
    (h1..h6) -> two OSDs each (0..11).
    """

    def __init__(self, old=OLD, new=NEW, pgs=None, join_secs=4, catch_up_secs=4):
        self.old, self.new = old, new
        self.clock = _Clock()
        self.join_secs, self.catch_up_secs = join_secs, catch_up_secs
        self.joins_at: Dict[int, float] = {}
        self.caught_up_at: Dict[int, float] = {}
        self.never_join: set = set()
        self.backfilling: set = set()
        self.stats_epoch = None
        self.unjoined_at_unset: List[List[int]] = []
        self.racks = {'r1': ['h1', 'h2'], 'r2': ['h3', 'h4'], 'r3': ['h5', 'h6']}
        self.hosts = {h: [2 * i, 2 * i + 1] for i, h in enumerate(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])}
        self.host_of = {o: h for h, osds in self.hosts.items() for o in osds}
        self.epoch = 100
        self.up = {o: True for o in range(12)}
        self.up_from = {o: 10 for o in range(12)}
        self.flags = {o: set() for o in range(12)}
        self.version = {o: old for o in range(12)}
        self.recovering = set()
        # (min_size, acting): one replica per rack for most PGs, plus a
        # "host failure domain" pool whose PGs put two replicas in r1
        self.pgs = pgs if pgs is not None else (
            [(2, [a, b, c]) for a in (0, 1, 2, 3) for b in (4, 5, 6, 7) for c in (8, 9, 10, 11)][:16]
            + [(2, [0, 2, 4]), (2, [1, 3, 8])])
        self.commands: List[dict] = []
        self.restarts: List[Tuple[int, str]] = []

    # --- maps
    def tree(self):
        nodes = [{'id': -1, 'name': 'default', 'type': 'root', 'type_id': 11, 'children': [-2, -3, -4]}]
        rid, hid = -2, -10
        for r, hosts in self.racks.items():
            hids = []
            for h in hosts:
                hids.append(hid)
                nodes.append({'id': hid, 'name': h, 'type': 'host', 'type_id': 1,
                              'children': list(self.hosts[h])})
                hid -= 1
            nodes.append({'id': rid, 'name': r, 'type': 'rack', 'type_id': 3, 'children': hids})
            rid -= 1
        for o in range(12):
            nodes.append({'id': o, 'name': f'osd.{o}', 'type': 'osd', 'type_id': 0, 'status': 'up'})
        return {'nodes': nodes, 'stray': []}

    def osdmap(self):
        return {'epoch': self.epoch, 'osds': [
            {'osd': o, 'uuid': f'uuid-{o}', 'up': 1 if self.up[o] else 0, 'in': 1,
             'up_from': self.up_from[o], 'state': ['exists', 'up'] + sorted(self.flags[o])}
            for o in range(12)]}

    def get(self, what):
        if what == 'osd_map':
            return self.osdmap()
        if what == 'osd_map_tree':
            return self.tree()
        if what == 'pg_stats':
            return self.pg_stats()
        return None

    # --- the PG model behind ok-to-stop and pg_stats
    def unjoined(self):
        return {o for o, t in self.joins_at.items() if o in self.never_join or self.clock.now < t}

    def missing(self):
        """OSDs with objects missing, i.e. not caught up yet."""
        return set(self.recovering) | {
            o for o, t in self.caught_up_at.items() if t is None or self.clock.now < t}

    def _pg(self, up):
        unjoined, missing = self.unjoined(), self.missing()
        acting = [o for o in up if self.up[o] and o not in unjoined and o not in self.backfilling]
        degraded = len(acting) < len(up) or any(o in missing for o in acting)
        complete = [o for o in acting if o not in missing] if degraded else []
        return acting, degraded, complete

    def pg_stats(self):
        out = []
        unjoined = self.unjoined()
        for n, (min_size, up) in enumerate(self.pgs):
            acting, degraded, complete = self._pg(up)
            if any(self.up[o] and o in unjoined for o in up):
                state = 'peering'
            elif len(acting) < min_size:
                state = 'undersized+degraded+peered'
            elif any(o in self.backfilling for o in up):
                state = 'active+remapped+backfilling'
            elif degraded:
                state = 'active+recovering+degraded'
            else:
                state = 'active+clean'
            out.append({'pgid': f'1.{n:x}', 'state': state, 'up': list(up), 'acting': acting,
                        'avail_no_missing': [str(o) for o in complete],
                        'reported_epoch': self.epoch if self.stats_epoch is None else self.stats_epoch})
        return {'pg_stats': out}

    def ok_to_stop(self, ids):
        stopped = set(ids)
        bad = []
        for n, (min_size, up) in enumerate(self.pgs):
            acting, degraded, complete = self._pg(up)
            counted = complete if degraded else acting
            if not stopped & set(up):
                continue
            left = [o for o in counted if o not in stopped]
            if len(left) < min_size:
                bad.append(n)
        return bad

    def mon_command(self, cmd, inbuf=None):
        self.commands.append(cmd)
        p = cmd.get('prefix')
        if p == 'osd ok-to-stop':
            ids = [int(i) for i in cmd['ids']]
            bad = self.ok_to_stop(ids)
            report = {'ok_to_stop': not bad, 'osds': ids, 'bad_become_inactive': [f'1.{b}' for b in bad]}
            if bad:
                return (-16, json.dumps({'ok_to_stop': report}),
                        f'unsafe to stop osd(s) at this time ({len(bad)} PGs are or would become offline)')
            return (0, json.dumps({'ok_to_stop': report}), '')
        if p == 'osd unset-group':
            self.unjoined_at_unset.append(sorted(self.unjoined()))
        if p in ('osd set-group', 'osd unset-group'):
            for who in cmd['who']:
                o = int(who.split('.', 1)[1])
                for fl in cmd['flags'].split(','):
                    (self.flags[o].add if p == 'osd set-group' else self.flags[o].discard)(fl)
            return (0, '', '')
        if p == 'osd metadata':
            o = int(cmd['id'])
            return (0, json.dumps({'id': o, 'ceph_version_short': self.version[o],
                                   'ceph_version': f'ceph version {self.version[o]} (hash) x (stable)'}), '')
        if p == 'versions':
            return (0, '{}', '')
        return (0, '', '')

    # --- what cephadm switch-staged does to the world
    def restart(self, osd_id, version, comes_back=True):
        self.restarts.append((osd_id, version))
        self.epoch += 1
        self.version[osd_id] = version
        if comes_back:
            self.up[osd_id] = True
            self.up_from[osd_id] = self.epoch
            self.joins_at[osd_id] = self.clock.now + self.join_secs
            self.caught_up_at[osd_id] = (None if self.catch_up_secs is None
                                         else self.joins_at[osd_id] + self.catch_up_secs)
        else:
            self.up[osd_id] = False

    def recover(self):
        self.recovering.clear()
        self.caught_up_at.clear()

    def cmds(self, prefix, **kw):
        return [c for c in self.commands if c.get('prefix') == prefix
                and all(c.get(k) == v for k, v in kw.items())]


def _osd_dds(mons, ids=None):
    return [_dd('osd', str(o), host=mons.host_of[o], service_name='osd.all')
            for o in (ids if ids is not None else range(12))]


def _osd_setup(cephadm_module, mons, switch_fails=(), never_up=(), level='host', noout=True):
    calls: List[Tuple[str, str, list]] = []
    staged: List[str] = []

    async def fake_run_cephadm(host, entity, command, args, **kw):
        calls.append((host, command, list(args)))
        if command == 'switch-staged':
            ids = [int(n.split('.', 1)[1]) for n in _names_in(args)]
            bad = [i for i in ids if i in switch_fails and '--rollback' not in args]
            if bad:
                return ([], [f'osd.{bad[0]}: boom'], 1)
            for osd_id in ids:
                if '--rollback' in args:
                    mons.restart(osd_id, mons.old)
                else:
                    mons.restart(osd_id, mons.new, comes_back=osd_id not in never_up)
            return ([json.dumps([{'name': f'osd.{i}'} for i in ids])], [], 0)
        return (['{}'], [], 0)

    async def fake_create_daemon(daemon_spec, reconfig=False, osd_uuid_map=None,
                                 skip_restart_for_reconfig=False, send_signal_to_daemon=None,
                                 stage=False):
        assert stage is True
        assert osd_uuid_map and osd_uuid_map[daemon_spec.daemon_id] == f'uuid-{daemon_spec.daemon_id}'
        staged.append(daemon_spec.name())
        return 'ok'

    async def no_refresh(hosts):
        return None

    cephadm_module.upgrade_staged_switch = True
    cephadm_module.upgrade_staged_switch_types = 'mds,osd'
    cephadm_module.upgrade_staged_switch_timeout = 20
    cephadm_module.upgrade_staged_switch_osd_crush_level = level
    cephadm_module.upgrade_staged_switch_osd_noout = noout
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target_image', 0, target_digests=[TARGET], target_version=mons.new)
    patches = [
        mock.patch("cephadm.serve.CephadmServe._run_cephadm", side_effect=fake_run_cephadm),
        mock.patch("cephadm.serve.CephadmServe._create_daemon", side_effect=fake_create_daemon),
        mock.patch.object(StagedSwitchRunner, '_refresh_hosts', side_effect=no_refresh),
        mock.patch("cephadm.CephadmOrchestrator.get", side_effect=mons.get),
        mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command", side_effect=mons.mon_command),
        mock.patch("cephadm.module.CephadmOrchestrator.mon_command", side_effect=mons.mon_command),
        mock.patch("cephadm.staged_switch.time", mons.clock),
    ]
    return calls, staged, patches


class _OsdRun:
    """Drives passes of the OSD policy against the simulator: each pass
    hands the runner the OSDs still on the old version, like _do_upgrade."""

    def __init__(self, cephadm_module, mons, dds=None, **kw):
        self.m, self.mons = cephadm_module, mons
        self.dds = dds if dds is not None else _osd_dds(mons)
        self.calls, self.staged, self.patches = _osd_setup(cephadm_module, mons, **kw)
        self.groups: List[str] = []

    def __enter__(self):
        for p in self.patches:
            p.start()
        self.hosts = [with_host(self.m, h) for h in self.mons.hosts]
        for h in self.hosts:
            h.__enter__()
        _add_daemons(self.m, self.dds)
        return self

    def __exit__(self, *exc):
        for h in reversed(self.hosts):
            h.__exit__(*exc)
        for p in reversed(self.patches):
            p.stop()

    def pending(self):
        return [d for d in self.dds if self.mons.version[int(d.daemon_id)] != self.mons.new]

    def one_pass(self, dds=None):
        policy = policy_for(self.m.upgrade, 'osd')
        assert isinstance(policy, OsdStagedSwitchPolicy)
        before = len(self.mons.restarts)
        handled = StagedSwitchRunner(self.m.upgrade, policy).run(
            dds if dds is not None else self.pending(), TARGET)
        self.groups.append(sorted(o for o, _ in self.mons.restarts[before:]))
        return handled

    def switched(self, rollback=False):
        return _switches(self.calls, rollback)


def test_crush_tree_view():
    tree = CrushTree(_FakeOsdMons().tree())
    assert tree.roots == [-1]
    assert tree.osds_under(-1) == list(range(12))
    assert tree.osds_under(-2) == [0, 1, 2, 3]            # r1
    assert tree.osds_under(-10) == [0, 1]                 # h1
    assert [b['name'] for b in tree.buckets_of_type('rack')] == ['r1', 'r2', 'r3']
    assert tree.bucket_types_top_down() == ['rack', 'host']   # not 'root'
    assert tree.by_name['h3']['id'] == -12
    # natural order: host2 before host10
    t2 = CrushTree({'nodes': [{'id': -1, 'name': 'root', 'type': 'root', 'type_id': 11, 'children': [-2, -3]},
                              {'id': -2, 'name': 'host10', 'type': 'host', 'type_id': 1, 'children': [0]},
                              {'id': -3, 'name': 'host2', 'type': 'host', 'type_id': 1, 'children': [1]},
                              {'id': 0, 'name': 'osd.0', 'type': 'osd', 'type_id': 0},
                              {'id': 1, 'name': 'osd.1', 'type': 'osd', 'type_id': 0}]})
    assert [b['name'] for b in t2.buckets_of_type('host')] == ['host2', 'host10']


def test_osd_host_level_one_host_per_pass(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons) as run:
        assert run.one_pass() is True
        # the whole of h1 staged, noout set on exactly those OSDs, both
        # switched at once, noout cleared, versions checked with the monitors
        assert sorted(run.staged) == ['osd.0', 'osd.1']
        assert run.groups[-1] == [0, 1]
        assert mons.cmds('osd set-group') == [{'prefix': 'osd set-group', 'flags': 'noout', 'who': ['osd.0', 'osd.1']}]
        assert mons.cmds('osd unset-group') == [{'prefix': 'osd unset-group', 'flags': 'noout', 'who': ['osd.0', 'osd.1']}]
        assert mons.flags[0] == set() and mons.flags[1] == set()
        assert sorted(int(c['id']) for c in mons.cmds('osd metadata')) == [0, 1]
        assert mons.version[0] == mons.version[1] == NEW
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch == {} and not st.paused
        # the ok-to-stop calls asked for exactly the set, with max = len
        oks = mons.cmds('osd ok-to-stop')
        assert all(c['max'] == len(c['ids']) for c in oks)
        # the pass waited for h1 to be back in its PGs with a complete copy,
        # so h2 could go next; but osd.0 restarts on its own and misses
        # writes: every other host shares a PG with it ([0,2,4], [0,4,8]...)
        # and so does every single OSD probed -> wait, do not pause
        mons.recovering.add(0)
        assert run.one_pass() is True
        assert run.groups[-1] == []
        assert not cephadm_module.upgrade.upgrade_state.paused
        assert 'Waiting to stage' in cephadm_module.upgrade.upgrade_info_str
        mons.recover()
        assert run.one_pass() is True
        assert run.groups[-1] == [2, 3]
        # all the way through: six hosts, in order, one right after the other
        for _ in range(4):
            run.one_pass()
        assert run.groups == [[0, 1], [], [2, 3], [4, 5], [6, 7], [8, 9], [10, 11]]
        assert all(v == NEW for v in mons.version.values())
        # nothing left: the policy steps aside
        assert run.one_pass() is False


def test_osd_auto_level_takes_racks_and_falls_back_to_hosts(cephadm_module: CephadmOrchestrator):
    # r1 can never go as a whole (PGs [0,2,4] and [1,3,8] have two copies
    # in it), r2 and r3 can: auto does r2, r3, then r1 host by host.
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons, level='auto') as run:
        run.one_pass()
        assert run.groups[-1] == [4, 5, 6, 7]                  # rack r2 (r1 refused)
        mons.recover()
        run.one_pass()
        assert run.groups[-1] == [8, 9, 10, 11]                # rack r3
        mons.recover()
        run.one_pass()
        assert run.groups[-1] == [0, 1]                        # r1 still refused -> host h1
        mons.recover()
        run.one_pass()
        assert run.groups[-1] == [2, 3]                        # h2
        assert all(v == NEW for v in mons.version.values())
        # labels say what was switched
        assert [c for c in mons.cmds('osd set-group')][0]['who'] == ['osd.4', 'osd.5', 'osd.6', 'osd.7']


def test_osd_explicit_level_never_descends_but_does_not_stall(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons, level='rack') as run:
        run.one_pass()
        mons.recover()
        run.one_pass()
        mons.recover()
        assert run.groups == [[4, 5, 6, 7], [8, 9, 10, 11]]
        # only r1 is left and it can never be stopped as a whole, while its
        # OSDs can one by one: no host-level group (the level is explicit),
        # the regular path gets this pass instead of waiting forever
        assert run.one_pass() is False
        assert run.groups[-1] == [] and run.staged.count('osd.0') == 0
        assert not cephadm_module.upgrade.upgrade_state.paused
        # ... whereas while PGs recover (osd.4 restarted on its own and missed
        # writes), it waits
        mons2 = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons2, level='rack') as run2:
        run2.one_pass()
        assert run2.groups[-1] == [4, 5, 6, 7]
        mons2.recovering.add(4)
        assert run2.one_pass() is True                       # recovering: wait
        assert run2.groups[-1] == []
        info = cephadm_module.upgrade.upgrade_info_str
        assert 'Waiting to stage' in info and 'rack r1' in info and 'PG(s) would become inactive' in info


def test_osd_pg_check_covers_only_the_pending_osds(cephadm_module: CephadmOrchestrator):
    # OSDs of the bucket already on the target are not restarted again
    mons = _FakeOsdMons()
    mons.version[0] = NEW
    with _OsdRun(cephadm_module, mons) as run:
        run.one_pass()
        assert run.groups[-1] == [1]
        assert run.staged == ['osd.1']
        assert mons.cmds('osd ok-to-stop')[0]['ids'] == ['1']


def test_osd_scope_of_crush_bucket_name(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons) as run:
        st = cephadm_module.upgrade.upgrade_state
        st.crush_bucket_type, st.crush_bucket_name = 'rack', 'r2'
        run.one_pass()
        mons.recover()
        run.one_pass()
        mons.recover()
        assert run.groups == [[4, 5], [6, 7]]
        # nothing left in scope: the regular path (which filters too) takes over
        assert run.one_pass() is False
        assert run.groups[-1] == []
        st.crush_bucket_name = 'nowhere'
        assert run.one_pass() is True
        assert st.paused and 'nowhere' in cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']


def test_osd_leaves_down_osds_and_offline_hosts_to_the_regular_path(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    mons.pgs = [pg for pg in mons.pgs if 1 not in pg[1]]   # osd.1 holds no PG
    mons.up[1] = False                 # osd.1 is down: it has no window to miss
    with _OsdRun(cephadm_module, mons) as run:
        cephadm_module.offline_hosts.add('h2')
        try:
            run.one_pass()
            assert run.groups[-1] == [0]
            mons.recover()
            run.one_pass()
            assert run.groups[-1] == [4, 5]      # h2 skipped entirely
            mons.recover()
            for _ in range(3):
                run.one_pass()
                mons.recover()
            # only osd.1 and h2 left: nothing for the policy
            assert run.one_pass() is False
            assert run.groups[-1] == []
        finally:
            cephadm_module.offline_hosts.discard('h2')
        assert mons.version[1] == OLD and mons.version[2] == OLD and mons.version[3] == OLD
        assert all(mons.version[o] == NEW for o in (0, 4, 5, 6, 7, 8, 9, 10, 11))


def test_osd_recheck_before_the_window(cephadm_module: CephadmOrchestrator):
    # ok-to-stop passes when the group is picked, but by the time staging
    # is done another OSD is gone: nothing is restarted, the state is
    # cleared and the pass ends without pausing
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons) as run:
        orig = mons.mon_command

        def flaky(cmd, inbuf=None):
            if cmd.get('prefix') == 'osd ok-to-stop' and len(mons.cmds('osd ok-to-stop')) == 1:
                mons.up[2] = False     # a host dies after the first check
            return orig(cmd, inbuf)

        with mock.patch("cephadm.module.CephadmOrchestrator.mon_command", side_effect=flaky), \
                mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command", side_effect=flaky):
            assert run.one_pass() is True
        assert sorted(run.staged) == ['osd.0', 'osd.1']
        assert run.switched() == [] and run.groups[-1] == []
        # noout was set at take_down and cleared again when the switch was called off
        assert len(mons.cmds('osd set-group')) == 1 and len(mons.cmds('osd unset-group')) == 1
        assert mons.flags[0] == set() and mons.flags[1] == set()
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch == {} and not st.paused
        assert 'no longer ok-to-stop' in cephadm_module.upgrade.upgrade_info_str


def test_osd_one_osd_not_back_pauses_without_rollback_and_resumes(cephadm_module: CephadmOrchestrator):
    # osd.1 does not come back: the group is NOT switched back (a store the
    # new ceph-osd opened is not for the old one), the upgrade pauses with
    # the state kept, noout stays on the group; once osd.1 is up again,
    # `upgrade resume` re-verifies, restores and moves on
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons, never_up=(1,)) as run:
        run.one_pass()
        assert len(run.switched()) == 2 and run.switched(rollback=True) == []
        assert mons.version[0] == NEW and mons.version[1] == NEW
        assert mons.flags[0] == {'noout'} and mons.flags[1] == {'noout'}
        st = cephadm_module.upgrade.upgrade_state
        assert st.paused and st.staged_switch.get('phase') == 'switched'
        hc = cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']
        assert 'osd.1 is not up' in hc['summary'] and 'upgrade resume' in hc['summary']
        # the admin fixes osd.1, resumes
        mons.up[1] = True
        mons.epoch += 1
        mons.up_from[1] = mons.epoch
        st.paused = False
        assert run.one_pass(dds=_osd_dds(mons, [])) is True
        assert run.switched() == run.switched()[:2]          # nothing restarted again
        assert mons.flags[0] == set() and mons.flags[1] == set()
        assert st.staged_switch == {} and not st.paused


def test_osd_old_version_after_switch_pauses(cephadm_module: CephadmOrchestrator):
    # the daemon restarts but the monitors still see the old version
    # (wrong image behind the tag): pause, no rollback
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons) as run:
        orig = mons.restart

        def restart(osd_id, version, comes_back=True):
            orig(osd_id, OLD if osd_id == 0 and version == NEW else version, comes_back)
        mons.restart = restart
        run.one_pass()
        assert run.switched(rollback=True) == []
        st = cephadm_module.upgrade.upgrade_state
        assert st.paused and st.staged_switch.get('phase') == 'switched'
        assert "osd.0 reports version '19.2.8'" in cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']['summary']


def test_osd_switch_command_failure_pauses_and_resume_retries(cephadm_module: CephadmOrchestrator):
    # switch-staged fails on h1 (nothing restarted there, the command
    # checks before stopping): pause at 'switching'; on resume the
    # idempotent switch is retried and goes through
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons, switch_fails=(1,)) as run:
        run.one_pass()
        assert run.groups[-1] == [] and mons.version[0] == OLD
        st = cephadm_module.upgrade.upgrade_state
        assert st.paused and st.staged_switch.get('phase') == 'switching'
        assert 'osd.1: boom' in ' '.join(cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']['detail'])
        assert mons.flags[0] == {'noout'}
    # the image is fixed on the host: resume
    with _OsdRun(cephadm_module, mons) as run:
        cephadm_module.upgrade.upgrade_state = st
        st.paused = False
        mons.commands.clear()
        assert run.one_pass() is True
        assert run.groups[-1] == [0, 1] and run.staged == []     # not staged again
        assert mons.cmds('osd ok-to-stop') == []                  # resumed at 'switching': no re-check
        assert mons.version[0] == mons.version[1] == NEW
        assert mons.flags[0] == set() and st.staged_switch == {} and not st.paused


def test_osd_resume_at_down_rechecks_ok_to_stop(cephadm_module: CephadmOrchestrator):
    # mgr failover between take_down and the switch, and the cluster is no
    # longer in a state to open the window: call it off, nothing restarted
    mons = _FakeOsdMons()
    mons.up[2] = False                                   # [0,2,4] has one copy left without 0
    mons.flags[0].add('noout')
    mons.flags[1].add('noout')
    state = {'type': 'osd', 'key': '-10', 'label': 'host h1', 'daemons': ['osd.0', 'osd.1'],
             'hosts': ['h1'], 'image': TARGET,
             'data': {'bucket': 'h1', 'type': 'host', 'osd_ids': [0, 1], 'noout': True, 'committed': True},
             'snapshot': {'epoch': 100, 'up_from': {'0': 10, '1': 10}}, 'phase': 'down'}
    with _OsdRun(cephadm_module, mons) as run:
        cephadm_module.upgrade.upgrade_state.staged_switch = state
        assert run.one_pass(dds=_osd_dds(mons, [0, 1])) is True
        assert len(mons.cmds('osd ok-to-stop')) >= 1 and run.switched() == []
        assert mons.flags[0] == set() and mons.flags[1] == set()      # restored
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch == {} and not st.paused
        assert 'no longer ok-to-stop' in cephadm_module.upgrade.upgrade_info_str


def test_osd_any_osd_failure_since_the_choice_calls_the_switch_off(cephadm_module: CephadmOrchestrator):
    # osd.11 shares no PG with h1, so ok-to-stop would still clear h1 - but
    # an OSD went down since the group was chosen: the verdict was given
    # for another cluster, start over
    mons = _FakeOsdMons()
    mons.pgs = [pg for pg in mons.pgs if 11 not in pg[1]]
    with _OsdRun(cephadm_module, mons) as run:
        orig = mons.mon_command

        def flaky(cmd, inbuf=None):
            if cmd.get('prefix') == 'osd set-group':
                mons.up[11] = False            # dies while h1 is being staged
            return orig(cmd, inbuf)

        with mock.patch("cephadm.module.CephadmOrchestrator.mon_command", side_effect=flaky), \
                mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command", side_effect=flaky):
            assert run.one_pass() is True
        assert sorted(run.staged) == ['osd.0', 'osd.1'] and run.switched() == []
        assert mons.flags[0] == set() and mons.flags[1] == set()
        assert len(mons.cmds('osd ok-to-stop')) == 1        # the fingerprint spoke first
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch == {} and not st.paused
        assert 'set of up OSDs changed' in cephadm_module.upgrade.upgrade_info_str


def test_osd_resume_without_a_purged_daemon(cephadm_module: CephadmOrchestrator):
    # osd.1 never came back after the switch, the admin purged it and
    # resumed: the group goes on without it instead of wedging
    mons = _FakeOsdMons()
    mons.restart(0, NEW)
    mons.flags[0].add('noout')
    state = {'type': 'osd', 'key': '-10', 'label': 'host h1', 'daemons': ['osd.0', 'osd.1'],
             'hosts': ['h1'], 'image': TARGET,
             'data': {'bucket': 'h1', 'type': 'host', 'osd_ids': [0, 1], 'noout': True, 'committed': True},
             'snapshot': {'epoch': 100, 'up_from': {'0': 10, '1': 10}}, 'phase': 'switched'}
    with _OsdRun(cephadm_module, mons, dds=_osd_dds(mons, [0] + list(range(2, 12)))) as run:
        cephadm_module.upgrade.upgrade_state.staged_switch = state
        assert run.one_pass(dds=[]) is True
        assert run.switched() == []
        assert mons.cmds('osd unset-group') == [{'prefix': 'osd unset-group', 'flags': 'noout', 'who': ['osd.0']}]
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch == {} and not st.paused


def test_osd_group_cap_option(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons, level='auto') as run:
        cephadm_module.upgrade_staged_switch_osd_max_group = 3
        run.one_pass()
        # racks hold 4 pending OSDs > 3: skipped for the host level
        assert run.groups[-1] == [0, 1]
        cephadm_module.upgrade_staged_switch_osd_max_group = 0
        mons.recover()
        run.one_pass()
        assert run.groups[-1] == [2, 3]                  # the rest of rack r1, as a rack


def test_osd_verify_timeout_is_its_own_option(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons) as run:
        cephadm_module.upgrade_staged_switch_osd_timeout = 777
        policy = policy_for(cephadm_module.upgrade, 'osd')
        assert policy is not None and policy.verify_timeout() == 777
        assert policy.rollback_on_failure is False
        mds = MdsStagedSwitchPolicy(cephadm_module.upgrade)
        assert mds.verify_timeout() == 20 and mds.rollback_on_failure is True
        del run


def test_osd_noout_can_be_disabled(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons, noout=False) as run:
        run.one_pass()
        assert run.groups[-1] == [0, 1]
        assert mons.cmds('osd set-group') == [] and mons.cmds('osd unset-group') == []


def test_osd_bad_level_pauses(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    for level, text in (('osd', 'makes no sense'), ('root', 'only used by root buckets')):
        with _OsdRun(cephadm_module, mons, level=level) as run:
            assert run.one_pass() is True
            assert run.staged == []
            st = cephadm_module.upgrade.upgrade_state
            assert st.paused
            assert text in cephadm_module.health_checks['UPGRADE_STAGE_FAILED']['summary']
    # a type the map does not have: nothing to group by, regular path
    with _OsdRun(cephadm_module, mons, level='rak') as run:
        assert run.one_pass() is False
        assert run.staged == [] and not cephadm_module.upgrade.upgrade_state.paused


def test_osd_limit_caps_the_group(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons) as run:
        st = cephadm_module.upgrade.upgrade_state
        st.total_count, st.remaining_count = 3, 3
        run.one_pass()
        assert run.groups[-1] == [0, 1] and st.remaining_count == 1
        mons.recover()
        run.one_pass()
        assert run.groups[-1] == [2] and st.remaining_count == 0
        mons.recover()
        assert run.one_pass() is False


def test_osd_resume_mid_switch_does_not_ask_ok_to_stop_again(cephadm_module: CephadmOrchestrator):
    # mgr failover while switching h1: osd.0 was restarted, osd.1 not yet.
    # PGs are degraded so ok-to-stop would refuse, but the group is
    # committed: finish the switch, verify, restore.
    mons = _FakeOsdMons()
    mons.restart(0, NEW)
    mons.flags[0].add('noout')
    mons.flags[1].add('noout')
    state = {'type': 'osd', 'key': '-10', 'label': 'host h1', 'daemons': ['osd.0', 'osd.1'],
             'hosts': ['h1'], 'image': TARGET,
             'data': {'bucket': 'h1', 'type': 'host', 'osd_ids': [0, 1], 'noout': True, 'committed': True},
             'snapshot': {'epoch': 100, 'up_from': {'0': 10, '1': 10}}, 'phase': 'switching'}
    with _OsdRun(cephadm_module, mons) as run:
        cephadm_module.upgrade.upgrade_state.staged_switch = state
        assert run.one_pass(dds=_osd_dds(mons, [1])) is True
        assert mons.cmds('osd ok-to-stop') == []
        assert run.staged == []
        assert len(run.switched()) == 2                     # idempotent switch-staged on both
        assert mons.version[1] == NEW
        assert mons.flags[0] == set() and mons.flags[1] == set()
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch == {} and not st.paused


def test_osd_staging_generates_config_and_passes_the_uuid_map(cephadm_module: CephadmOrchestrator):
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons) as run:
        with mock.patch.object(cephadm_module.osd_service, 'generate_config',
                               return_value=({'config': '', 'keyring': ''}, [])) as gen:
            run.one_pass()
        assert sorted(c.args[0].daemon_spec.name() for c in gen.call_args_list) == ['osd.0', 'osd.1']
        # the fake _create_daemon asserted the uuid map was handed over
        assert sorted(run.staged) == ['osd.0', 'osd.1']


def test_osd_group_is_done_only_once_back_in_its_pgs(cephadm_module: CephadmOrchestrator):
    # the OSDs boot on the new version at once but take 10s to rejoin their
    # PGs and 10s more to catch up: noout stays on and the next group is not
    # chosen until they have
    mons = _FakeOsdMons(join_secs=10, catch_up_secs=10)
    with _OsdRun(cephadm_module, mons) as run:
        assert run.one_pass() is True
        assert run.groups[-1] == [0, 1]
        assert mons.unjoined_at_unset == [[]] and mons.missing() == set()
        assert mons.flags[0] == set() and mons.flags[1] == set()
        assert cephadm_module.upgrade.upgrade_state.staged_switch == {}


def test_osd_auto_takes_the_next_rack_once_the_previous_one_has_rejoined(cephadm_module: CephadmOrchestrator):
    # size 2 / min_size 1 PGs over two racks out of three: once r1 is
    # switched, r2 and r3 hold PGs of r1's OSDs, but h4 (6, 7) does not.
    # While r1 is still peering, or catching up, ok-to-stop counts it out:
    # r2 is refused and h4 passes - the group would be a host on a verdict
    # that only reflects r1 not being back yet. Waiting for r1 to be back
    # in its PGs with a complete copy gets the whole of r2.
    pgs = [(1, [0, 4]), (1, [1, 8]), (1, [2, 5]), (1, [3, 9]), (1, [6, 10]), (1, [7, 11])]
    mons = _FakeOsdMons(pgs=pgs, join_secs=6, catch_up_secs=6)
    with _OsdRun(cephadm_module, mons, level='auto') as run:
        for _ in range(3):
            assert run.one_pass() is True
        assert run.groups == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]
        assert all(u == [] for u in mons.unjoined_at_unset)
        assert all(v == NEW for v in mons.version.values())


def test_osd_never_rejoined_pauses_and_resume_finishes(cephadm_module: CephadmOrchestrator):
    # osd.1 boots on the new version but never gets back into its PGs:
    # the upgrade pauses at the verification, noout kept, no rollback; once
    # it has, `upgrade resume` restores the group without restarting it
    mons = _FakeOsdMons()
    mons.never_join.add(1)
    with _OsdRun(cephadm_module, mons) as run:
        run.one_pass()
        st = cephadm_module.upgrade.upgrade_state
        assert st.paused and st.staged_switch.get('phase') == 'switched'
        assert run.switched(rollback=True) == []
        assert mons.flags[0] == {'noout'} and mons.flags[1] == {'noout'}
        summary = cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']['summary']
        assert 'peered again' in summary and 'osd.1:' in summary and 'osd.0:' not in summary
        mons.never_join.clear()
        st.paused = False
        assert run.one_pass(dds=_osd_dds(mons, [])) is True
        assert len(run.switched()) == 2
        assert mons.flags[0] == set() and mons.flags[1] == set()
        assert st.staged_switch == {} and not st.paused


def test_osd_pg_stats_from_before_the_restart_do_not_count(cephadm_module: CephadmOrchestrator):
    # PG stats not reported since the OSDs booted say nothing about them
    mons = _FakeOsdMons()
    mons.stats_epoch = mons.epoch
    with _OsdRun(cephadm_module, mons) as run:
        run.one_pass()
        st = cephadm_module.upgrade.upgrade_state
        assert st.paused and st.staged_switch.get('phase') == 'switched'
        assert 'peered again' in cephadm_module.health_checks['UPGRADE_SWITCH_FAILED']['summary']
        mons.stats_epoch = None
        st.paused = False
        run.one_pass(dds=_osd_dds(mons, []))
        assert st.staged_switch == {} and not st.paused


def test_osd_backfilling_osd_settles_without_pausing(cephadm_module: CephadmOrchestrator):
    # osd.0 is backfilled (left out of the acting set of active PGs, pg_temp):
    # its PGs have peered again, so the switch is done, but it is not back
    # in their acting sets yet - the group settles, without a timeout, and
    # the next group waits for the backfill
    mons = _FakeOsdMons()
    mons.backfilling.add(0)
    with _OsdRun(cephadm_module, mons) as run:
        cephadm_module.upgrade_staged_switch_osd_timeout = 30
        run.one_pass()
        st = cephadm_module.upgrade.upgrade_state
        assert not st.paused and 'UPGRADE_SWITCH_FAILED' not in cephadm_module.health_checks
        assert st.staged_switch.get('phase') == 'settling'
        info = cephadm_module.upgrade.upgrade_info_str
        assert 'osd.0:' in info and 'osd.1:' not in info
        mons.clock.sleep(3600)
        run.one_pass()
        assert st.staged_switch.get('phase') == 'settling' and not st.paused
        mons.backfilling.clear()
        run.one_pass()
        assert st.staged_switch == {} and not st.paused


def test_osd_catching_up_waits_without_pausing(cephadm_module: CephadmOrchestrator):
    # osd.0 and osd.1 are back in the acting sets of their PGs, active, but
    # still miss the objects written while they were down: the switch is
    # done (noout cleared, --limit counted) but the next group waits for
    # them, pass after pass, without pausing the upgrade and without a
    # timeout; once they have caught up, the group ends and h2 goes next
    mons = _FakeOsdMons(catch_up_secs=None)
    with _OsdRun(cephadm_module, mons) as run:
        st = cephadm_module.upgrade.upgrade_state
        st.remaining_count = 10
        assert run.one_pass() is True
        assert run.groups[-1] == [0, 1]
        assert not st.paused and 'UPGRADE_SWITCH_FAILED' not in cephadm_module.health_checks
        assert st.staged_switch.get('phase') == 'settling'
        assert mons.flags[0] == set() and mons.flags[1] == set()
        assert st.remaining_count == 8
        assert mons.unjoined() == set() and mons.missing() == {0, 1}
        stats = mons.pg_stats()['pg_stats']
        assert all('active' in pg['state'].split('+') and 0 in pg['acting']
                   for pg in stats if 0 in pg['up'])
        info = cephadm_module.upgrade.upgrade_info_str
        assert 'to settle' in info and 'recovering what was written' in info and 'osd.0:' in info
        # hours later (well past any timeout), still waiting, still not paused
        mons.clock.sleep(10 * 3600)
        staged = len(run.staged)
        assert run.one_pass() is True
        assert run.groups[-1] == [] and len(run.staged) == staged
        assert not st.paused and st.staged_switch.get('phase') == 'settling'
        mons.recover()
        assert run.one_pass() is True                        # the group ends
        assert run.groups[-1] == [] and st.staged_switch == {}
        assert st.remaining_count == 8                       # counted once
        assert run.one_pass() is True
        assert run.groups[-1] == [2, 3]


def test_osd_last_group_settles_too(cephadm_module: CephadmOrchestrator):
    # the last group is switched: nothing left to upgrade, but the group
    # still settles before the runner lets go of it
    mons = _FakeOsdMons(catch_up_secs=None)
    with _OsdRun(cephadm_module, mons, dds=_osd_dds(mons, [0, 1])) as run:
        run.one_pass()
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch.get('phase') == 'settling'
        assert run.pending() == []
        assert run.one_pass(dds=[]) is True                  # still settling
        mons.recover()
        assert run.one_pass(dds=[]) is True                  # done now
        assert st.staged_switch == {}
        assert run.one_pass(dds=[]) is False                 # nothing left


def test_osd_settling_ignores_an_osd_that_went_down_again(cephadm_module: CephadmOrchestrator):
    # an OSD of the group that dies after the switch is not waited for:
    # ok-to-stop sees it down when the next group is chosen
    mons = _FakeOsdMons(catch_up_secs=None)
    with _OsdRun(cephadm_module, mons) as run:
        run.one_pass()
        st = cephadm_module.upgrade.upgrade_state
        assert st.staged_switch.get('phase') == 'settling'
        mons.up[0] = False
        mons.epoch += 1
        mons.caught_up_at.pop(1)                            # osd.1 caught up
        assert run.one_pass() is True
        assert st.staged_switch == {} and not st.paused


def test_osd_verify_timeout_does_not_cover_catching_up(cephadm_module: CephadmOrchestrator):
    # catching up takes longer than upgrade_staged_switch_osd_timeout: no
    # UPGRADE_SWITCH_FAILED, the group just settles for longer
    mons = _FakeOsdMons(catch_up_secs=900)
    with _OsdRun(cephadm_module, mons) as run:
        cephadm_module.upgrade_staged_switch_osd_timeout = 60
        for _ in range(40):
            run.one_pass()
            if cephadm_module.upgrade.upgrade_state.staged_switch == {}:
                break
        st = cephadm_module.upgrade.upgrade_state
        assert not st.paused and 'UPGRADE_SWITCH_FAILED' not in cephadm_module.health_checks
        assert st.staged_switch == {} and mons.missing() == set()
        assert run.one_pass() is True
        assert run.groups[-1] == [2, 3]


def test_osd_ec_shards_in_avail_no_missing(cephadm_module: CephadmOrchestrator):
    # EC pools report shards as 'osd(shard)'
    mons = _FakeOsdMons()
    with _OsdRun(cephadm_module, mons):
        policy = policy_for(cephadm_module.upgrade, 'osd')
        assert isinstance(policy, OsdStagedSwitchPolicy)
        osds = {0: {'up': 1, 'up_from': 10}, 1: {'up': 1, 'up_from': 10}}
        pg = {'pgid': '2.0', 'state': 'active+recovering+degraded', 'up': [0, 4, 8],
              'acting': [0, 4, 8], 'avail_no_missing': ['0(0)', '4(1)'], 'reported_epoch': 20}
        with mock.patch("cephadm.CephadmOrchestrator.get", side_effect=lambda w: {'pg_stats': [pg]}):
            assert policy._pgs_waiting([0, 1], osds, caught_up=False) == {}
            assert policy._pgs_waiting([0, 1], osds, caught_up=True) == {}
            pg['avail_no_missing'] = ['4(1)', '8(2)']
            assert policy._pgs_waiting([0, 1], osds, caught_up=True) == {0: ['2.0']}
            assert policy._pgs_summary({0: ['2.0']}) == 'osd.0: 1 PG(s), e.g. 2.0'
