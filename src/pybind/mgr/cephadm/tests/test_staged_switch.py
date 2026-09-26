"""Tests for the staged switch: the daemon-type agnostic runner (driven with
a fake policy) and the MDS policy (driven with a small monitor simulator)."""

import json
from typing import Dict, List, Tuple
from unittest import mock

from cephadm import CephadmOrchestrator
from cephadm.staged_switch import (
    MdsStagedSwitchPolicy,
    StagedGroup,
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
            name = args[args.index('--name') + 1]
            if '--rollback' in args:
                world.gen[name] += 1
                world.version[name] = OLD
            elif name in switch_fails:
                return ([], [f'{name}: boom'], 1)
            else:
                world.gen[name] += 1
                world.version[name] = NEW if name not in verify_fails else OLD
            return ([json.dumps({'name': name})], [], 0)
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
    return [c for c in calls if c[1] == 'switch-staged' and (('--rollback' in c[2]) == rollback)]


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


def test_runner_state_survives_json(cephadm_module: CephadmOrchestrator):
    st = UpgradeState('t', 'pid', staged_switch={'type': 'mds', 'phase': 'down', 'data': {'fscids': [1]}})
    restored = UpgradeState.from_json(json.loads(json.dumps(st.to_json())))
    assert restored and restored.staged_switch == {'type': 'mds', 'phase': 'down', 'data': {'fscids': [1]}}
    assert UpgradeState('t', 'pid').staged_switch == {}


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
    assert policy_for(up, 'osd') is None                    # listed, no policy
    assert isinstance(policy_for(up, 'mds'), MdsStagedSwitchPolicy)
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
            name = args[args.index('--name') + 1].split('.', 1)[1]
            if '--rollback' in args:
                mons.restart(name, mons.old)
            elif name in switch_fails:
                return ([], [f'mds.{name}: boom'], 1)
            else:
                mons.restart(name, mons.new)
            return ([json.dumps({'name': f'mds.{name}'})], [], 0)
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
