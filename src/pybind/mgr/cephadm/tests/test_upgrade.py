import json
from unittest import mock

import pytest

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec
from cephadm import CephadmOrchestrator
from cephadm.upgrade import CephadmUpgrade, UpgradeState
from cephadm.ssh import HostConnectionError
from cephadm.utils import ContainerInspectInfo
from orchestrator import OrchestratorError, DaemonDescription
from .fixtures import _run_cephadm, wait, with_host, with_service, \
    receive_agent_metadata, async_side_effect

from typing import List, Tuple, Optional


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_start(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        with with_host(cephadm_module, 'test2'):
            with with_service(cephadm_module, ServiceSpec('mgr', placement=PlacementSpec(count=2)), status_running=True):
                assert wait(cephadm_module, cephadm_module.upgrade_start(
                    'image_id', None)) == 'Initiating upgrade to image_id'

                assert wait(cephadm_module, cephadm_module.upgrade_status()
                            ).target_image == 'image_id'

                assert wait(cephadm_module, cephadm_module.upgrade_pause()
                            ) == 'Paused upgrade to image_id'

                assert wait(cephadm_module, cephadm_module.upgrade_resume()
                            ) == 'Resumed upgrade to image_id'

                assert wait(cephadm_module, cephadm_module.upgrade_stop()
                            ) == 'Stopped upgrade to image_id'


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_start_offline_hosts(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        with with_host(cephadm_module, 'test2'):
            cephadm_module.offline_hosts = set(['test2'])
            with pytest.raises(OrchestratorError, match=r"Upgrade aborted - Some host\(s\) are currently offline: {'test2'}"):
                cephadm_module.upgrade_start('image_id', None)
            cephadm_module.offline_hosts = set([])  # so remove_host doesn't fail when leaving the with_host block


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_daemons_offline_hosts(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        with with_host(cephadm_module, 'test2'):
            cephadm_module.upgrade.upgrade_state = UpgradeState('target_image', 0)
            with mock.patch("cephadm.serve.CephadmServe._run_cephadm", side_effect=HostConnectionError('connection failure reason', 'test2', '192.168.122.1')):
                _to_upgrade = [(DaemonDescription(daemon_type='crash', daemon_id='test2', hostname='test2'), True)]
                with pytest.raises(HostConnectionError, match=r"connection failure reason"):
                    cephadm_module.upgrade._upgrade_daemons(_to_upgrade, 'target_image', ['digest1'])


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_do_upgrade_offline_hosts(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        with with_host(cephadm_module, 'test2'):
            cephadm_module.upgrade.upgrade_state = UpgradeState('target_image', 0)
            cephadm_module.offline_hosts = set(['test2'])
            with pytest.raises(HostConnectionError, match=r"Host\(s\) were marked offline: {'test2'}"):
                cephadm_module.upgrade._do_upgrade()
            cephadm_module.offline_hosts = set([])  # so remove_host doesn't fail when leaving the with_host block


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
@mock.patch("cephadm.module.CephadmOrchestrator.remove_health_warning")
def test_upgrade_resume_clear_health_warnings(_rm_health_warning, cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        with with_host(cephadm_module, 'test2'):
            cephadm_module.upgrade.upgrade_state = UpgradeState('target_image', 0, paused=True)
            _rm_health_warning.return_value = None
            assert wait(cephadm_module, cephadm_module.upgrade_resume()
                        ) == 'Resumed upgrade to target_image'
            calls_list = [mock.call(alert_id) for alert_id in cephadm_module.upgrade.UPGRADE_ERRORS]
            _rm_health_warning.assert_has_calls(calls_list, any_order=True)


@mock.patch('cephadm.upgrade.CephadmUpgrade._get_current_version', lambda _: (17, 2, 6))
@mock.patch("cephadm.serve.CephadmServe._get_container_image_info")
def test_upgrade_check_with_ceph_version(_get_img_info, cephadm_module: CephadmOrchestrator):
    # This test was added to avoid screwing up the image base so that
    # when the version was added to it it made an incorrect image
    # The issue caused the image to come out as
    # quay.io/ceph/ceph:v18:v18.2.0
    # see https://tracker.ceph.com/issues/63150
    _img = ''

    def _fake_get_img_info(img_name):
        nonlocal _img
        _img = img_name
        return ContainerInspectInfo(
            'image_id',
            '18.2.0',
            'digest'
        )

    _get_img_info.side_effect = _fake_get_img_info
    cephadm_module.upgrade_check('', '18.2.0')
    assert _img == 'quay.io/ceph/ceph:v18.2.0'


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
@pytest.mark.parametrize("use_repo_digest",
                         [
                             False,
                             True
                         ])
def test_upgrade_run(use_repo_digest, cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        with with_host(cephadm_module, 'host2'):
            cephadm_module.set_container_image('global', 'from_image')
            cephadm_module.use_repo_digest = use_repo_digest
            with with_service(cephadm_module, ServiceSpec('mgr', placement=PlacementSpec(host_pattern='*', count=2)),
                              CephadmOrchestrator.apply_mgr, '', status_running=True), \
                mock.patch("cephadm.module.CephadmOrchestrator.lookup_release_name",
                           return_value='foo'), \
                mock.patch("cephadm.module.CephadmOrchestrator.version",
                           new_callable=mock.PropertyMock) as version_mock, \
                mock.patch("cephadm.module.CephadmOrchestrator.get",
                           return_value={
                               # capture fields in both mon and osd maps
                               "require_osd_release": "pacific",
                               "min_mon_release": 16,
                           }):
                version_mock.return_value = 'ceph version 18.2.1 (somehash)'
                assert wait(cephadm_module, cephadm_module.upgrade_start(
                    'to_image', None)) == 'Initiating upgrade to to_image'

                assert wait(cephadm_module, cephadm_module.upgrade_status()
                            ).target_image == 'to_image'

                def _versions_mock(cmd):
                    return json.dumps({
                        'mgr': {
                            'ceph version 1.2.3 (asdf) blah': 1
                        }
                    })

                cephadm_module._mon_command_mock_versions = _versions_mock

                with mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm(json.dumps({
                    'image_id': 'image_id',
                    'repo_digests': ['to_image@repo_digest'],
                    'ceph_version': 'ceph version 18.2.3 (hash)',
                }))):

                    cephadm_module.upgrade._do_upgrade()

                assert cephadm_module.upgrade_status is not None

                with mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm(
                    json.dumps([
                        dict(
                            name=list(cephadm_module.cache.daemons['host1'].keys())[0],
                            style='cephadm',
                            fsid='fsid',
                            container_id='container_id',
                            container_image_name='to_image',
                            container_image_id='image_id',
                            container_image_digests=['to_image@repo_digest'],
                            deployed_by=['to_image@repo_digest'],
                            version='version',
                            state='running',
                        )
                    ])
                )):
                    receive_agent_metadata(cephadm_module, 'host1', ['ls'])
                    receive_agent_metadata(cephadm_module, 'host2', ['ls'])

                with mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm(json.dumps({
                    'image_id': 'image_id',
                    'repo_digests': ['to_image@repo_digest'],
                    'ceph_version': 'ceph version 18.2.3 (hash)',
                }))):
                    cephadm_module.upgrade._do_upgrade()

                _, image, _ = cephadm_module.check_mon_command({
                    'prefix': 'config get',
                    'who': 'global',
                    'key': 'container_image',
                })
                if use_repo_digest:
                    assert image == 'to_image@repo_digest'
                else:
                    assert image == 'to_image'


def test_upgrade_state_null(cephadm_module: CephadmOrchestrator):
    # This test validates https://tracker.ceph.com/issues/47580
    cephadm_module.set_store('upgrade_state', 'null')
    CephadmUpgrade(cephadm_module)
    assert CephadmUpgrade(cephadm_module).upgrade_state is None


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_not_enough_mgrs(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'host1'):
        with with_service(cephadm_module, ServiceSpec('mgr', placement=PlacementSpec(count=1)), CephadmOrchestrator.apply_mgr, ''):
            with pytest.raises(OrchestratorError):
                wait(cephadm_module, cephadm_module.upgrade_start('image_id', None))


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
@mock.patch("cephadm.CephadmOrchestrator.check_mon_command")
def test_enough_mons_for_ok_to_stop(check_mon_command, cephadm_module: CephadmOrchestrator):
    # only 2 monitors, not enough for ok-to-stop to ever pass
    check_mon_command.return_value = (
        0, '{"monmap": {"mons": [{"name": "mon.1"}, {"name": "mon.2"}]}}', '')
    assert not cephadm_module.upgrade._enough_mons_for_ok_to_stop()

    # 3 monitors, ok-to-stop should work fine
    check_mon_command.return_value = (
        0, '{"monmap": {"mons": [{"name": "mon.1"}, {"name": "mon.2"}, {"name": "mon.3"}]}}', '')
    assert cephadm_module.upgrade._enough_mons_for_ok_to_stop()


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
@mock.patch("cephadm.module.HostCache.get_daemons_by_service")
@mock.patch("cephadm.CephadmOrchestrator.get")
def test_enough_mds_for_ok_to_stop(get, get_daemons_by_service, cephadm_module: CephadmOrchestrator):
    get.side_effect = [{'filesystems': [{'mdsmap': {'fs_name': 'test', 'max_mds': 1}}]}]
    get_daemons_by_service.side_effect = [[DaemonDescription()]]
    assert not cephadm_module.upgrade._enough_mds_for_ok_to_stop(
        DaemonDescription(daemon_type='mds', daemon_id='test.host1.gfknd', service_name='mds.test'))

    get.side_effect = [{'filesystems': [{'mdsmap': {'fs_name': 'myfs.test', 'max_mds': 2}}]}]
    get_daemons_by_service.side_effect = [[DaemonDescription(), DaemonDescription()]]
    assert not cephadm_module.upgrade._enough_mds_for_ok_to_stop(
        DaemonDescription(daemon_type='mds', daemon_id='myfs.test.host1.gfknd', service_name='mds.myfs.test'))

    get.side_effect = [{'filesystems': [{'mdsmap': {'fs_name': 'myfs.test', 'max_mds': 1}}]}]
    get_daemons_by_service.side_effect = [[DaemonDescription(), DaemonDescription()]]
    assert cephadm_module.upgrade._enough_mds_for_ok_to_stop(
        DaemonDescription(daemon_type='mds', daemon_id='myfs.test.host1.gfknd', service_name='mds.myfs.test'))


@pytest.mark.parametrize("current_version, use_tags, show_all_versions, tags, result",
                         [
                             # several candidate versions (from different major versions)
                             (
                                 (16, 1, '16.1.0'),
                                 False,  # use_tags
                                 False,  # show_all_versions
                                 [
                                     'v17.1.0',
                                     'v16.2.7',
                                     'v16.2.6',
                                     'v16.2.5',
                                     'v16.1.4',
                                     'v16.1.3',
                                     'v15.2.0',
                                 ],
                                 ['17.1.0', '16.2.7', '16.2.6', '16.2.5', '16.1.4', '16.1.3']
                             ),
                             # candidate minor versions are available
                             (
                                 (16, 1, '16.1.0'),
                                 False,  # use_tags
                                 False,  # show_all_versions
                                 [
                                     'v16.2.2',
                                     'v16.2.1',
                                     'v16.1.6',
                                 ],
                                 ['16.2.2', '16.2.1', '16.1.6']
                             ),
                             # all versions are less than the current version
                             (
                                 (17, 2, '17.2.0'),
                                 False,  # use_tags
                                 False,  # show_all_versions
                                 [
                                     'v17.1.0',
                                     'v16.2.7',
                                     'v16.2.6',
                                 ],
                                 []
                             ),
                             # show all versions (regardless of the current version)
                             (
                                 (16, 1, '16.1.0'),
                                 False,  # use_tags
                                 True,   # show_all_versions
                                 [
                                     'v17.1.0',
                                     'v16.2.7',
                                     'v16.2.6',
                                     'v15.1.0',
                                     'v14.2.0',
                                 ],
                                 ['17.1.0', '16.2.7', '16.2.6', '15.1.0', '14.2.0']
                             ),
                             # show all tags (regardless of the current version and show_all_versions flag)
                             (
                                 (16, 1, '16.1.0'),
                                 True,   # use_tags
                                 False,  # show_all_versions
                                 [
                                     'v17.1.0',
                                     'v16.2.7',
                                     'v16.2.6',
                                     'v16.2.5',
                                     'v16.1.4',
                                     'v16.1.3',
                                     'v15.2.0',
                                 ],
                                 ['v15.2.0', 'v16.1.3', 'v16.1.4', 'v16.2.5',
                                     'v16.2.6', 'v16.2.7', 'v17.1.0']
                             ),
                         ])
@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_ls(current_version, use_tags, show_all_versions, tags, result, cephadm_module: CephadmOrchestrator):
    with mock.patch('cephadm.upgrade.Registry.get_tags', return_value=tags):
        with mock.patch('cephadm.upgrade.CephadmUpgrade._get_current_version', return_value=current_version):
            out = cephadm_module.upgrade.upgrade_ls(None, use_tags, show_all_versions)
            if use_tags:
                assert out['tags'] == result
            else:
                assert out['versions'] == result


@pytest.mark.parametrize(
    "upgraded, not_upgraded, daemon_types, hosts, services, should_block",
    # [ ([(type, host, id), ... ], [...], [daemon types], [hosts], [services], True/False), ... ]
    [
        (  # valid, upgrade mgr daemons
            [],
            [('mgr', 'a', 'a.x'), ('mon', 'a', 'a')],
            ['mgr'],
            None,
            None,
            False
        ),
        (  # invalid, can't upgrade mons until mgr is upgraded
            [],
            [('mgr', 'a', 'a.x'), ('mon', 'a', 'a')],
            ['mon'],
            None,
            None,
            True
        ),
        (  # invalid, can't upgrade mon service until all mgr daemons are upgraded
            [],
            [('mgr', 'a', 'a.x'), ('mon', 'a', 'a')],
            None,
            None,
            ['mon'],
            True
        ),
        (  # valid, upgrade mgr service
            [],
            [('mgr', 'a', 'a.x'), ('mon', 'a', 'a')],
            None,
            None,
            ['mgr'],
            False
        ),
        (  # valid, mgr is already upgraded so can upgrade mons
            [('mgr', 'a', 'a.x')],
            [('mon', 'a', 'a')],
            ['mon'],
            None,
            None,
            False
        ),
        (  # invalid, can't upgrade all daemons on b b/c un-upgraded mgr on a
            [],
            [('mgr', 'b', 'b.y'), ('mon', 'a', 'a')],
            None,
            ['a'],
            None,
            True
        ),
        (  # valid, only daemon on b is a mgr
            [],
            [('mgr', 'a', 'a.x'), ('mgr', 'b', 'b.y'), ('mon', 'a', 'a')],
            None,
            ['b'],
            None,
            False
        ),
        (  # invalid, can't upgrade mon on a while mgr on b is un-upgraded
            [],
            [('mgr', 'a', 'a.x'), ('mgr', 'b', 'b.y'), ('mon', 'a', 'a')],
            None,
            ['a'],
            None,
            True
        ),
        (  # valid, only upgrading the mgr on a
            [],
            [('mgr', 'a', 'a.x'), ('mgr', 'b', 'b.y'), ('mon', 'a', 'a')],
            ['mgr'],
            ['a'],
            None,
            False
        ),
        (  # valid, mgr daemon not on b are upgraded
            [('mgr', 'a', 'a.x')],
            [('mgr', 'b', 'b.y'), ('mon', 'a', 'a')],
            None,
            ['b'],
            None,
            False
        ),
        (  # valid, all the necessary hosts are covered, mgr on c is already upgraded
            [('mgr', 'c', 'c.z')],
            [('mgr', 'a', 'a.x'), ('mgr', 'b', 'b.y'), ('mon', 'a', 'a'), ('osd', 'c', '0')],
            None,
            ['a', 'b'],
            None,
            False
        ),
        (  # invalid, can't upgrade mon on a while mgr on b is un-upgraded
            [],
            [('mgr', 'a', 'a.x'), ('mgr', 'b', 'b.y'), ('mon', 'a', 'a')],
            ['mgr', 'mon'],
            ['a'],
            None,
            True
        ),
        (  # valid, only mon not on "b" is upgraded already. Case hit while making teuthology test
            [('mon', 'a', 'a')],
            [('mon', 'b', 'x'), ('mon', 'b', 'y'), ('osd', 'a', '1'), ('osd', 'b', '2')],
            ['mon', 'osd'],
            ['b'],
            None,
            False
        ),
    ]
)
@mock.patch("cephadm.module.HostCache.get_daemons")
@mock.patch("cephadm.serve.CephadmServe._get_container_image_info")
@mock.patch('cephadm.module.SpecStore.__getitem__')
def test_staggered_upgrade_validation(
        get_spec,
        get_image_info,
        get_daemons,
        upgraded: List[Tuple[str, str, str]],
        not_upgraded: List[Tuple[str, str, str, str]],
        daemon_types: Optional[str],
        hosts: Optional[str],
        services: Optional[str],
        should_block: bool,
        cephadm_module: CephadmOrchestrator,
):
    def to_dds(ts: List[Tuple[str, str]], upgraded: bool) -> List[DaemonDescription]:
        dds = []
        digest = 'new_image@repo_digest' if upgraded else 'old_image@repo_digest'
        for t in ts:
            dds.append(DaemonDescription(daemon_type=t[0],
                                         hostname=t[1],
                                         daemon_id=t[2],
                                         container_image_digests=[digest],
                                         deployed_by=[digest],))
        return dds
    get_daemons.return_value = to_dds(upgraded, True) + to_dds(not_upgraded, False)
    get_image_info.side_effect = async_side_effect(
        ('new_id', 'ceph version 99.99.99 (hash)', ['new_image@repo_digest']))

    class FakeSpecDesc():
        def __init__(self, spec):
            self.spec = spec

    def _get_spec(s):
        return FakeSpecDesc(ServiceSpec(s))

    get_spec.side_effect = _get_spec
    if should_block:
        with pytest.raises(OrchestratorError):
            cephadm_module.upgrade._validate_upgrade_filters(
                'new_image_name', daemon_types, hosts, services)
    else:
        cephadm_module.upgrade._validate_upgrade_filters(
            'new_image_name', daemon_types, hosts, services)
