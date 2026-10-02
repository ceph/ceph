# Tests for staged redeploys: `deploy --stage` writes unit.*.staged siblings
# without touching systemd, and `switch-staged` swaps them in (or rolls them
# back) around a single stop/start of the daemon.

import json
import os
import pathlib

from unittest import mock
import pytest

from .fixtures import (  # noqa: F401 -- cephadm_fs / funkypatch are pytest fixtures
    cephadm_fs,
    funkypatch,
    import_cephadm,
    mock_podman,
    with_cephadm_ctx,
)

_cephadm = import_cephadm()

FSID = '9b9d7609-f4d5-4aba-94c8-effa764d96c9'
DATA = f'/var/lib/ceph/{FSID}/mds.a'
OLD_IMAGE = 'quay.io/ceph/ceph:old'
NEW_IMAGE = 'quay.io/ceph/ceph:new'
UNIT = f'ceph-{FSID}@mds.a'


def _write(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)


def _read(path):
    with open(path) as f:
        return f.read()


def _live_daemon(image=OLD_IMAGE):
    for n in _cephadm.UNIT_FILES:
        _write(f'{DATA}/{n}', f'{n} for {image}\n' if n != 'unit.image' else f'{image}\n')


def _staged_daemon(image=NEW_IMAGE, files=None):
    for n in (files or _cephadm.UNIT_FILES):
        _write(f'{DATA}/{n}.staged',
               f'{n} for {image}\n' if n != 'unit.image' else f'{image}\n')


def _ident():
    return _cephadm.DaemonIdentity(FSID, 'mds', 'a')


def _systemctl_calls(call_mock):
    return [c.args[1] for c in call_mock.call_args_list
            if c.args[1][0] == 'systemctl']


def _deploy_patches(funkypatch):
    _call = funkypatch.patch('cephadmlib.container_types.call')
    _call.return_value = ('', '', 0)
    _call_throws = funkypatch.patch('cephadmlib.container_types.call_throws')
    _call_throws.return_value = ('ceph version 99.0.0 (hash) reef (stable)', '', 0)
    firewalld = funkypatch.patch('cephadm.Firewalld')
    firewalld().external_ports.get.return_value = []
    uid_gid = funkypatch.patch('cephadm.extract_uid_gid', force=True)
    uid_gid.return_value = (os.getuid(), os.getgid())
    funkypatch.patch('cephadm.install_sysctl')
    funkypatch.patch('cephadmlib.file_utils.make_run_dir')
    return _call, _call_throws


class TestDeployStage:
    def test_stage_writes_siblings_and_leaves_systemd_alone(self, cephadm_fs, funkypatch):
        # Full `_orch deploy` path with params.stage=true: config and
        # keyring are refreshed, unit.*.staged are written for the new image,
        # the live unit files and the running unit are not touched.
        _call, _call_throws = _deploy_patches(funkypatch)
        with with_cephadm_ctx([]) as ctx:
            ctx.container_engine = mock_podman()
            _cephadm.apply_deploy_config_to_ctx(
                {'name': 'mds.a', 'fsid': FSID, 'image': NEW_IMAGE,
                 'config_blobs': {'config': 'CONF', 'keyring': 'KEY'},
                 'params': {'stage': True}}, ctx)
            assert ctx.stage is True
            _live_daemon(OLD_IMAGE)
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)) as call, \
                    mock.patch('cephadm.check_unit', return_value=(True, 'running', True)), \
                    mock.patch('cephadm.is_container_running', return_value=True):
                _cephadm._common_deploy(ctx)

        base = pathlib.Path(DATA)
        # staged siblings exist and point at the new image...
        for n in _cephadm.UNIT_FILES:
            assert (base / f'{n}.staged').exists(), n
        assert _read(f'{DATA}/unit.image.staged').strip() == NEW_IMAGE
        assert NEW_IMAGE in _read(f'{DATA}/unit.run.staged')
        assert '--entrypoint /usr/bin/ceph-mds' in _read(f'{DATA}/unit.run.staged')
        # ...while the live files are untouched
        assert _read(f'{DATA}/unit.image').strip() == OLD_IMAGE
        assert _read(f'{DATA}/unit.run') == f'unit.run for {OLD_IMAGE}\n'
        # config/keyring were refreshed (they are only read at start)
        assert _read(f'{DATA}/config') == 'CONF'
        assert _read(f'{DATA}/keyring') == 'KEY'
        # the target image was executed once with --version, before writing
        version_runs = [c for c in _call_throws.call_args_list
                        if '--version' in c.args[1] and NEW_IMAGE in c.args[1]]
        assert len(version_runs) == 1
        # systemd: daemon-reload only - no stop, start, enable, restart
        assert _systemctl_calls(call_throws) == [['systemctl', 'daemon-reload']]
        assert not any(c.args[1][:2] in (['systemctl', 'stop'], ['systemctl', 'start'],
                                         ['systemctl', 'restart'])
                       for c in call.call_args_list)

    def test_stage_refuses_a_broken_image(self, cephadm_fs, funkypatch):
        _call, _call_throws = _deploy_patches(funkypatch)
        _call_throws.side_effect = RuntimeError('exec format error')
        with with_cephadm_ctx([]) as ctx:
            ctx.container_engine = mock_podman()
            _cephadm.apply_deploy_config_to_ctx(
                {'name': 'mds.a', 'fsid': FSID, 'image': NEW_IMAGE,
                 'config_blobs': {'config': 'CONF', 'keyring': 'KEY'},
                 'params': {'stage': True}}, ctx)
            _live_daemon(OLD_IMAGE)
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.check_unit', return_value=(True, 'running', True)), \
                    mock.patch('cephadm.is_container_running', return_value=True):
                with pytest.raises(_cephadm.Error, match='failed to run'):
                    _cephadm._common_deploy(ctx)
        assert not os.path.exists(f'{DATA}/unit.run.staged')
        assert _systemctl_calls(call_throws) == []

    def test_get_deployment_type_stage(self, cephadm_fs):
        with with_cephadm_ctx(['--image', NEW_IMAGE, 'deploy', '--name', 'mds.a',
                               '--fsid', FSID, '--stage']) as ctx:
            with mock.patch('cephadm.check_unit', return_value=(True, 'running', True)), \
                    mock.patch('cephadm.is_container_running', return_value=True):
                with pytest.raises(_cephadm.Error, match='has not been deployed'):
                    _cephadm.get_deployment_type(ctx, _ident())
                _live_daemon()
                assert _cephadm.get_deployment_type(ctx, _ident()) is _cephadm.DeploymentType.STAGE

    def test_stage_and_reconfig_are_exclusive(self, cephadm_fs):
        with with_cephadm_ctx(['--image', NEW_IMAGE, 'deploy', '--name', 'mds.a',
                               '--fsid', FSID, '--stage', '--reconfig']) as ctx:
            _live_daemon()
            with pytest.raises(_cephadm.Error, match='mutually exclusive'):
                _cephadm.get_deployment_type(ctx, _ident())

    def test_stage_refuses_agent_and_sidecars(self, cephadm_fs):
        with with_cephadm_ctx([f'--image={NEW_IMAGE}']) as ctx:
            ctx.fsid = FSID
            _live_daemon()
            with pytest.raises(_cephadm.Error, match='containerized'):
                _cephadm.deploy_daemon(ctx, _ident(), None, 0, 0,
                                       deployment_type=_cephadm.DeploymentType.STAGE)
            c = _cephadm.CephContainer(ctx, image=NEW_IMAGE, entrypoint='/usr/bin/ceph-mds')
            sc = mock.MagicMock()
            with pytest.raises(_cephadm.Error, match='sidecar'):
                _cephadm.deploy_daemon(ctx, _ident(), c, 0, 0,
                                       deployment_type=_cephadm.DeploymentType.STAGE,
                                       sidecars=[sc])

    def test_orch_deploy_params_accept_stage(self):
        with with_cephadm_ctx(['_orch', 'deploy']) as ctx:
            _cephadm.apply_deploy_config_to_ctx(
                {'name': 'mds.a', 'fsid': FSID, 'image': NEW_IMAGE,
                 'params': {'stage': True}}, ctx)
            assert ctx.stage is True
            assert not getattr(ctx, 'reconfig', False)


class TestSwitchStaged:
    def _ctx(self):
        cm = with_cephadm_ctx([f'--image={NEW_IMAGE}'], list_networks={})
        ctx = cm.__enter__()
        ctx.fsid = FSID
        ctx.container_engine = mock_podman()
        return cm, ctx

    def test_switch_swaps_files_around_one_stop_start(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)) as call, \
                    mock.patch('cephadm.clean_cgroup') as clean_cgroup:
                res = _cephadm.switch_staged_unit_files(ctx, _ident(), expected_image=NEW_IMAGE)
            assert res['image'] == NEW_IMAGE
            assert set(res['switched']) == set(_cephadm.UNIT_FILES)
            assert not res.get('already_switched')
            # live files are the new ones, previous ones kept as .prev, no .staged left
            assert _read(f'{DATA}/unit.image').strip() == NEW_IMAGE
            assert _read(f'{DATA}/unit.run') == f'unit.run for {NEW_IMAGE}\n'
            assert _read(f'{DATA}/unit.image.prev').strip() == OLD_IMAGE
            assert not os.path.exists(f'{DATA}/unit.run.staged')
            # image presence was checked before stopping anything
            assert any(c.args[1][1:3] == ['image', 'inspect'] for c in call.call_args_list)
            # exactly: stop, (reset-failed via call), enable, start
            assert _systemctl_calls(call_throws) == [
                ['systemctl', 'stop', UNIT],
                ['systemctl', 'enable', UNIT],
                ['systemctl', 'start', UNIT],
            ]
            assert ['systemctl', 'reset-failed', UNIT] in _systemctl_calls(call)
            clean_cgroup.assert_called_once()
        finally:
            cm.__exit__(None, None, None)

    def test_switch_refuses_wrong_expected_image(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)):
                with pytest.raises(_cephadm.Error, match='not the expected'):
                    _cephadm.switch_staged_unit_files(
                        ctx, _ident(), expected_image='quay.io/ceph/ceph:other')
            # nothing was stopped, nothing moved
            assert _systemctl_calls(call_throws) == []
            assert _read(f'{DATA}/unit.image').strip() == OLD_IMAGE
            assert os.path.exists(f'{DATA}/unit.run.staged')
        finally:
            cm.__exit__(None, None, None)

    def test_switch_refuses_when_image_is_gone(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)

            def fake_call(ctx, cmd, **kw):
                if cmd[1:3] == ['image', 'inspect']:
                    return ('', 'no such image', 125)
                return ('', '', 0)

            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', side_effect=fake_call):
                with pytest.raises(_cephadm.Error, match='not present on this host'):
                    _cephadm.switch_staged_unit_files(ctx, _ident())
            assert _systemctl_calls(call_throws) == []
            assert _read(f'{DATA}/unit.image').strip() == OLD_IMAGE
        finally:
            cm.__exit__(None, None, None)

    def test_switch_refuses_partial_staging(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE, files=['unit.image', 'unit.meta'])  # no unit.run.staged
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)):
                with pytest.raises(_cephadm.Error, match='unit.run.staged is missing'):
                    _cephadm.switch_staged_unit_files(ctx, _ident())
            assert _systemctl_calls(call_throws) == []
        finally:
            cm.__exit__(None, None, None)

    def test_switch_is_idempotent(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            _live_daemon(NEW_IMAGE)                      # already switched...
            _write(f'{DATA}/unit.image.prev', OLD_IMAGE + '\n')
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.check_unit', return_value=(True, 'running', True)):
                res = _cephadm.switch_staged_unit_files(ctx, _ident(), expected_image=NEW_IMAGE)
            assert res['already_switched'] is True
            assert res['switched'] == []
            assert _systemctl_calls(call_throws) == []  # ...and running: nothing to do

            # already switched but the unit is down: only start it
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.check_unit', return_value=(True, 'stopped', True)):
                res = _cephadm.switch_staged_unit_files(ctx, _ident(), expected_image=NEW_IMAGE)
            assert res.get('started') is True
            assert _systemctl_calls(call_throws) == [['systemctl', 'start', UNIT]]

            # nothing staged and the live image is not the expected one: error
            with mock.patch('cephadm.call_throws'), \
                    mock.patch('cephadm.call', return_value=('', '', 0)):
                with pytest.raises(_cephadm.Error, match='nothing staged'):
                    _cephadm.switch_staged_unit_files(
                        ctx, _ident(), expected_image='quay.io/ceph/ceph:other')
        finally:
            cm.__exit__(None, None, None)

    def test_rollback_restores_prev_and_keeps_staged_for_retry(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)
            with mock.patch('cephadm.call_throws'), \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.clean_cgroup'):
                _cephadm.switch_staged_unit_files(ctx, _ident())
                assert _read(f'{DATA}/unit.image').strip() == NEW_IMAGE

                with mock.patch('cephadm.call_throws') as call_throws:
                    res = _cephadm.switch_staged_unit_files(ctx, _ident(), rollback=True)
            assert res['rollback'] is True
            assert res['image'] == OLD_IMAGE
            assert _read(f'{DATA}/unit.run') == f'unit.run for {OLD_IMAGE}\n'
            # the new files are parked as .staged again so the switch can be retried
            assert _read(f'{DATA}/unit.image.staged').strip() == NEW_IMAGE
            assert not os.path.exists(f'{DATA}/unit.run.prev')
            assert _systemctl_calls(call_throws)[0] == ['systemctl', 'stop', UNIT]
            assert _systemctl_calls(call_throws)[-1] == ['systemctl', 'start', UNIT]
        finally:
            cm.__exit__(None, None, None)

    def test_switch_requires_data_dir(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            with pytest.raises(_cephadm.Error, match='does not exist'):
                _cephadm.switch_staged_unit_files(ctx, _cephadm.DaemonIdentity(FSID, 'mds', 'nope'))
        finally:
            cm.__exit__(None, None, None)


class TestSwitchStagedCommand:
    def test_command_prints_json(self, cephadm_fs, capsys):
        with with_cephadm_ctx(['switch-staged', '--fsid', FSID, '--name', 'mds.a',
                               '--expected-image', NEW_IMAGE]) as ctx:
            ctx.container_engine = mock_podman()
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)
            with mock.patch('cephadm.call_throws'), \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.clean_cgroup'):
                rc = _cephadm.command_switch_staged(ctx)
            assert rc == 0
            out = json.loads(capsys.readouterr().out)
            assert out['name'] == 'mds.a'
            assert out['image'] == NEW_IMAGE
            assert 'unit.run' in out['switched']

    def test_parser_rejects_unknown_daemon_type(self, cephadm_fs):
        # --name goes through CustomValidation like `deploy` and `unit`
        with pytest.raises(SystemExit):
            with with_cephadm_ctx(['switch-staged', '--fsid', FSID, '--name', 'bogus.a']):
                pass

    def test_command_requires_fsid(self, cephadm_fs):
        with with_cephadm_ctx(['switch-staged', '--name', 'mds.a']) as ctx:
            ctx.fsid = None
            with pytest.raises(_cephadm.Error, match='must pass --fsid'):
                _cephadm.command_switch_staged(ctx)


DATA_B = f'/var/lib/ceph/{FSID}/mds.b'
UNIT_B = f'ceph-{FSID}@mds.b'


def _daemon_b(live=OLD_IMAGE, staged=NEW_IMAGE):
    for n in _cephadm.UNIT_FILES:
        _write(f'{DATA_B}/{n}', f'{n} for {live}\n' if n != 'unit.image' else f'{live}\n')
        if staged:
            _write(f'{DATA_B}/{n}.staged', f'{n} for {staged}\n' if n != 'unit.image' else f'{staged}\n')


class TestSwitchStagedSeveral:
    """switch-staged with several --name: one stop of all the units, the
    swaps, one start - the downtime of a host's daemons is one restart."""

    def _ctx(self):
        cm = with_cephadm_ctx([f'--image={NEW_IMAGE}'], list_networks={})
        ctx = cm.__enter__()
        ctx.fsid = FSID
        ctx.container_engine = mock_podman()
        return cm, ctx

    def test_two_daemons_one_stop_one_start(self, cephadm_fs):
        cm, ctx = self._ctx()
        try:
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)
            _daemon_b()
            idents = [_ident(), _cephadm.DaemonIdentity(FSID, 'mds', 'b')]
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)) as call, \
                    mock.patch('cephadm.clean_cgroup') as clean_cgroup:
                res = _cephadm.switch_staged_units(ctx, idents, expected_image=NEW_IMAGE)
            assert [r['name'] for r in res] == ['mds.a', 'mds.b']
            assert all(r['image'] == NEW_IMAGE and set(r['switched']) == set(_cephadm.UNIT_FILES) for r in res)
            assert _read(f'{DATA}/unit.image').strip() == NEW_IMAGE
            assert _read(f'{DATA_B}/unit.image').strip() == NEW_IMAGE
            assert _systemctl_calls(call_throws) == [
                ['systemctl', 'stop', UNIT, UNIT_B],
                ['systemctl', 'enable', UNIT, UNIT_B],
                ['systemctl', 'start', UNIT, UNIT_B],
            ]
            assert ['systemctl', 'reset-failed', UNIT, UNIT_B] in _systemctl_calls(call)
            assert clean_cgroup.call_count == 2
        finally:
            cm.__exit__(None, None, None)

    def test_a_bad_daemon_stops_nothing(self, cephadm_fs):
        # mds.b was staged with another image: the whole call is refused
        # before mds.a is stopped
        cm, ctx = self._ctx()
        try:
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)
            _daemon_b(staged='quay.io/ceph/ceph:other')
            idents = [_ident(), _cephadm.DaemonIdentity(FSID, 'mds', 'b')]
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.clean_cgroup'):
                with pytest.raises(_cephadm.Error, match='mds.b: staged image'):
                    _cephadm.switch_staged_units(ctx, idents, expected_image=NEW_IMAGE)
            assert _systemctl_calls(call_throws) == []
            assert _read(f'{DATA}/unit.image').strip() == OLD_IMAGE
            assert os.path.exists(f'{DATA}/unit.run.staged')
        finally:
            cm.__exit__(None, None, None)

    def test_mixed_already_switched_and_pending(self, cephadm_fs):
        # a retry after a lost reply: mds.a is already switched (only made
        # sure to run), mds.b still pending
        cm, ctx = self._ctx()
        try:
            _live_daemon(NEW_IMAGE)
            _daemon_b()
            idents = [_ident(), _cephadm.DaemonIdentity(FSID, 'mds', 'b')]
            with mock.patch('cephadm.call_throws') as call_throws, \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.clean_cgroup'), \
                    mock.patch('cephadm.check_unit', return_value=(True, 'running', True)):
                res = _cephadm.switch_staged_units(ctx, idents, expected_image=NEW_IMAGE)
            assert res[0]['already_switched'] and res[0]['switched'] == []
            assert set(res[1]['switched']) == set(_cephadm.UNIT_FILES)
            assert _systemctl_calls(call_throws) == [
                ['systemctl', 'stop', UNIT_B],
                ['systemctl', 'enable', UNIT_B],
                ['systemctl', 'start', UNIT_B],
            ]
        finally:
            cm.__exit__(None, None, None)

    def test_command_accepts_several_names(self, cephadm_fs, capsys):
        with with_cephadm_ctx(['switch-staged', '--fsid', FSID, '--name', 'mds.a', '--name', 'mds.b',
                               '--expected-image', NEW_IMAGE]) as ctx:
            assert ctx.name == ['mds.a', 'mds.b']
            assert _cephadm._ctx_daemon_name(ctx) is None
            ctx.container_engine = mock_podman()
            _live_daemon(OLD_IMAGE)
            _staged_daemon(NEW_IMAGE)
            _daemon_b()
            with mock.patch('cephadm.call_throws'), \
                    mock.patch('cephadm.call', return_value=('', '', 0)), \
                    mock.patch('cephadm.clean_cgroup'):
                rc = _cephadm.command_switch_staged(ctx)
            assert rc == 0
            out = json.loads(capsys.readouterr().out)
            assert [o['name'] for o in out] == ['mds.a', 'mds.b']
        with with_cephadm_ctx(['switch-staged', '--fsid', FSID, '--name', 'mds.a']) as ctx:
            assert ctx.name == ['mds.a']
            assert _cephadm._ctx_daemon_name(ctx) == 'mds.a'
        with pytest.raises(SystemExit):
            with with_cephadm_ctx(['switch-staged', '--fsid', FSID, '--name', 'mds.a', '--name', 'bogus.b']):
                pass
