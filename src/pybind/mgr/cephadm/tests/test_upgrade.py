import json
import logging
from unittest import mock

import pytest

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec
from cephadm import CephadmOrchestrator
from cephadm.upgrade import (
    CephadmUpgrade,
    OkToUpgradeMonReport,
    UpgradeState,
    parse_ok_to_upgrade_mon_json,
    request_osd_ok_to_upgrade_report,
)
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
def test_upgrade_start_hosts_mutually_exclusive_with_bucket(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        with with_host(cephadm_module, 'test2'):
            with with_service(cephadm_module, ServiceSpec('mgr', placement=PlacementSpec(count=2)), status_running=True):
                with pytest.raises(OrchestratorError) as err:
                    cephadm_module.upgrade_start(
                        'image_id', None,
                        daemon_types=['osd'],
                        host_placement='test',
                        bucket_type='rack',
                        bucket_name='rack-a',
                    )
                assert str(err.value) == '--hosts cannot be combined with --crush_bucket_type or --crush_bucket_name'


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_start_services_mutually_exclusive_with_bucket(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        with with_host(cephadm_module, 'test2'):
            with with_service(cephadm_module, ServiceSpec('mgr', placement=PlacementSpec(count=2)), status_running=True):
                with pytest.raises(OrchestratorError) as err:
                    cephadm_module.upgrade_start(
                        'image_id', None,
                        services=['mgr'],
                        bucket_type='rack',
                        bucket_name='rack-a',
                    )
                assert str(err.value) == (
                    '--services cannot be combined with --crush_bucket_type or '
                    '--crush_bucket_name')


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


def test_upgrade_state_crush_roundtrip():
    u = UpgradeState(
        'target', 'pid', crush_bucket_type='rack', crush_bucket_name='rack1')
    restored = UpgradeState.from_json(u.to_json())
    assert restored
    assert restored.crush_bucket_type == 'rack'
    assert restored.crush_bucket_name == 'rack1'


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_status_which_crush_osd_only(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target', 'pid',
        target_digests=['digest1'],
        daemon_types=['osd'],
        crush_bucket_type='rack',
        crush_bucket_name='rack1',
    )
    with mock.patch.object(cephadm_module.upgrade, '_get_upgrade_info', return_value=('0/0', [])):
        status = wait(cephadm_module, cephadm_module.upgrade_status())
    assert status.which == 'Upgrading daemons of type(s) osd (OSDs in bucket scope)'


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_status_which_crush_osd_only_uppercase(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target', 'pid',
        target_digests=['digest1'],
        daemon_types=['OSD'],
        crush_bucket_type='rack',
        crush_bucket_name='rack1',
    )
    with mock.patch.object(cephadm_module.upgrade, '_get_upgrade_info', return_value=('0/0', [])):
        status = wait(cephadm_module, cephadm_module.upgrade_status())
    assert status.which == 'Upgrading daemons of type(s) OSD (OSDs in bucket scope)'


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_status_which_crush_mixed_daemon_types(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target', 'pid',
        target_digests=['digest1'],
        daemon_types=['mon', 'osd'],
        crush_bucket_type='rack',
        crush_bucket_name='rack1',
    )
    with mock.patch.object(cephadm_module.upgrade, '_get_upgrade_info', return_value=('0/0', [])):
        status = wait(cephadm_module, cephadm_module.upgrade_status())
    assert status.which == (
        'Upgrading daemons of type(s) mon,osd (OSDs in bucket scope)')


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_upgrade_status_which_full_cluster_with_crush_bucket(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target', 'pid',
        target_digests=['digest1'],
        crush_bucket_type='rack',
        crush_bucket_name='rack1',
    )
    with mock.patch.object(cephadm_module.upgrade, '_get_upgrade_info', return_value=('0/0', [])):
        status = wait(cephadm_module, cephadm_module.upgrade_status())
    assert status.which == 'Upgrading all daemon types on all hosts'


def test_parse_ok_to_upgrade_mon_json_nested_and_flat():
    nested = '{"ok_to_upgrade": {"ok_to_upgrade": true, "all_osds_upgraded": false}}'
    inner = parse_ok_to_upgrade_mon_json(nested)
    assert inner['ok_to_upgrade'] is True
    flat = '{"ok_to_upgrade": true}'
    d = parse_ok_to_upgrade_mon_json(flat)
    assert d['ok_to_upgrade'] is True


def test_ok_to_upgrade_mon_report_from_parsed_body():
    rep = OkToUpgradeMonReport.from_parsed_body({
        'ok_to_upgrade': True,
        'all_osds_upgraded': False,
        'osds_ok_to_upgrade': [0, 1],
        'osds_in_crush_bucket': [2, 3],
        'osds_upgraded': [4],
        'bad_no_version': [5],
    })
    assert rep.ok_to_upgrade is True
    assert rep.all_osds_upgraded is False
    assert rep.osds_ok_to_upgrade == [0, 1]
    assert rep.osds_in_crush_bucket == [2, 3]
    assert rep.osds_upgraded == [4]
    assert rep.bad_no_version == [5]
    assert rep.mon_resp_as_dict()['bad_no_version'] == [5]


def test_ok_to_upgrade_mon_report_non_list_osd_array_becomes_empty(caplog):
    caplog.set_level(logging.WARNING, logger='cephadm.upgrade')
    rep = OkToUpgradeMonReport.from_parsed_body({
        'ok_to_upgrade': True,
        'all_osds_upgraded': False,
        'osds_ok_to_upgrade': 'not-a-list',
    })
    assert rep.osds_ok_to_upgrade == []
    assert any(
        'expected list of osd ids' in r.getMessage()
        for r in caplog.records
    )


def test_ok_to_upgrade_mon_report_matches_mgr_json_formatter_shape():
    """
    Shape produced by upgrade_osd_report::dump + JSONFormatter (DaemonServer):
    array sections are bare int lists, not objects per element.
    """
    mon_stdout = (
        '{"ok_to_upgrade":{'
        '"ok_to_upgrade":true,'
        '"all_osds_upgraded":false,'
        '"osds_in_crush_bucket":[10,11],'
        '"osds_ok_to_upgrade":[10],'
        '"osds_upgraded":[],'
        '"bad_no_version":[]'
        '}}'
    )
    body = parse_ok_to_upgrade_mon_json(mon_stdout)
    rep = OkToUpgradeMonReport.from_parsed_body(body)
    assert rep.ok_to_upgrade is True
    assert rep.all_osds_upgraded is False
    assert rep.osds_in_crush_bucket == [10, 11]
    assert rep.osds_ok_to_upgrade == [10]
    assert rep.osds_upgraded == []
    assert rep.bad_no_version == []


def test_ok_to_upgrade_mon_report_from_parsed_body_rejects_non_mapping():
    with pytest.raises(ValueError, match='expected JSON object'):
        OkToUpgradeMonReport.from_parsed_body([])


def test_ok_to_upgrade_mon_report_warns_on_non_boolean_flags(caplog):
    caplog.set_level(logging.WARNING)
    rep = OkToUpgradeMonReport.from_parsed_body({
        'ok_to_upgrade': 'unexpected-string',
        'all_osds_upgraded': False,
    })
    assert rep.ok_to_upgrade is None
    assert rep.all_osds_upgraded is False
    assert any('expected boolean' in r.message for r in caplog.records)


def test_request_osd_ok_to_upgrade_report(cephadm_module: CephadmOrchestrator):
    cephadm_module.check_mon_command = mock.MagicMock(
        return_value=(0, '{"ok_to_upgrade": {"ok_to_upgrade": true}}', ''))
    rep = request_osd_ok_to_upgrade_report(
        cephadm_module, 'mybucket', '20.1.0-144.el9cp', max_osds=3)
    assert rep.ok_to_upgrade is True
    cephadm_module.check_mon_command.assert_called_once()
    cmd = cephadm_module.check_mon_command.call_args[0][0]
    assert cmd['prefix'] == 'osd ok-to-upgrade'
    assert cmd['crush_bucket'] == 'mybucket'
    assert cmd['ceph_version'] == '20.1.0-144.el9cp'
    assert cmd['max'] == 3


def test_parse_ok_to_upgrade_mon_json_invalid_raises():
    with pytest.raises(json.JSONDecodeError):
        parse_ok_to_upgrade_mon_json('not-json{')


def test_request_osd_ok_to_upgrade_report_invalid_json(cephadm_module: CephadmOrchestrator):
    cephadm_module.check_mon_command = mock.MagicMock(
        return_value=(0, 'not-json{', ''))
    with pytest.raises(json.JSONDecodeError):
        request_osd_ok_to_upgrade_report(
            cephadm_module, 'mybucket', '20.1.0', max_osds=3)


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_wait_for_ok_to_upgrade_osd_batch_json_decode_pauses(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'img',
        'pid',
        target_version='20.1.0',
        crush_bucket_type='host',
        crush_bucket_name='host1',
    )
    with mock.patch(
        'cephadm.upgrade.request_osd_ok_to_upgrade_report',
        side_effect=json.JSONDecodeError('msg', 'doc', 0),
    ):
        ok = cephadm_module.upgrade._wait_for_ok_to_upgrade_osd_batch([])
    assert ok is False
    assert cephadm_module.upgrade.upgrade_state.paused is True
    assert 'UPGRADE_EXCEPTION' in cephadm_module.health_checks
    assert 'invalid JSON' in cephadm_module.health_checks['UPGRADE_EXCEPTION']['summary']


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_wait_for_ok_to_upgrade_osd_batch_value_error_pauses(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'img',
        'pid',
        target_version='20.1.0',
        crush_bucket_type='host',
        crush_bucket_name='host1',
    )
    with mock.patch(
        'cephadm.upgrade.request_osd_ok_to_upgrade_report',
        side_effect=ValueError(
            "osd ok-to-upgrade: expected JSON object after unwrap, got <class 'list'>"),
    ):
        ok = cephadm_module.upgrade._wait_for_ok_to_upgrade_osd_batch([])
    assert ok is False
    assert cephadm_module.upgrade.upgrade_state.paused is True
    assert 'UPGRADE_EXCEPTION' in cephadm_module.health_checks
    assert (
        'unexpected JSON shape'
        in cephadm_module.health_checks['UPGRADE_EXCEPTION']['summary']
    )


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
def test_wait_for_ok_to_upgrade_osd_batch_bad_no_version_pauses(
        cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'img',
        'pid',
        target_version='20.1.0',
        crush_bucket_type='host',
        crush_bucket_name='host1',
    )
    bad_rep = OkToUpgradeMonReport(
        ok_to_upgrade=True,
        all_osds_upgraded=False,
        osds_ok_to_upgrade=[],
        osds_in_crush_bucket=[0, 1],
        osds_upgraded=[],
        bad_no_version=[99],
    )
    with mock.patch(
        'cephadm.upgrade.request_osd_ok_to_upgrade_report',
        return_value=bad_rep,
    ):
        ok = cephadm_module.upgrade._wait_for_ok_to_upgrade_osd_batch([])
    assert ok is False
    assert cephadm_module.upgrade.upgrade_state.paused is True
    assert 'UPGRADE_OSD_NO_VERSION' in cephadm_module.health_checks
    detail = cephadm_module.health_checks['UPGRADE_OSD_NO_VERSION']['detail'][0]
    assert 'osd.99' in detail


def test_validate_failure_domain_upgrade_options_ok(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade._validate_failure_domain_upgrade_options(
        'rack', 'rack-a', ['osd'])


def test_validate_failure_domain_upgrade_options_chassis_ok(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade._validate_failure_domain_upgrade_options(
        'chassis', 'c1', ['osd'])


def test_validate_failure_domain_upgrade_options_host_ok(cephadm_module: CephadmOrchestrator):
    cephadm_module.upgrade._validate_failure_domain_upgrade_options(
        'host', 'host1', ['osd'])


def test_validate_failure_domain_upgrade_options_invalid_type(cephadm_module: CephadmOrchestrator):
    with pytest.raises(OrchestratorError) as err:
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'root', 'default', ['osd'])
    assert str(err.value) == (
        "Supported bucket types for OSD upgrade are: chassis, host, rack (specified: 'root')")


def test_validate_failure_domain_upgrade_options_pairing(cephadm_module: CephadmOrchestrator):
    both_msg = 'Both --crush_bucket_type and --crush_bucket_name must be specified together'
    with pytest.raises(OrchestratorError) as err:
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'rack', None, ['osd'])
    assert str(err.value) == both_msg
    with pytest.raises(OrchestratorError) as err:
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            None, 'rack-a', ['osd'])
    assert str(err.value) == both_msg


def test_validate_failure_domain_upgrade_options_comma_in_name(cephadm_module: CephadmOrchestrator):
    with pytest.raises(OrchestratorError) as err:
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'rack', 'a,b', ['osd'])
    assert str(err.value) == (
        'Invalid --crush_bucket_name: use a single name token without commas')


def test_validate_failure_domain_upgrade_options_multi_token_name(cephadm_module: CephadmOrchestrator):
    with pytest.raises(OrchestratorError) as err:
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'rack', 'rack-a rack-b', ['osd'])
    assert str(err.value) == (
        'Invalid --crush_bucket_name: use a single name token without commas')


def test_validate_failure_domain_upgrade_options_daemon_types(cephadm_module: CephadmOrchestrator):
    osd_msg = 'Bucket parameters for OSD upgrade require --daemon-types to be "osd"'
    with pytest.raises(OrchestratorError):
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'rack', 'rack-a', None)
    with pytest.raises(OrchestratorError):
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'rack', 'rack-a', ['osd', 'mds'])
    with pytest.raises(OrchestratorError):
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'rack', 'rack-a', ['mgr', 'mon', 'osd'])
    with pytest.raises(OrchestratorError) as err:
        cephadm_module.upgrade._validate_failure_domain_upgrade_options(
            'rack', 'rack-a', ['mgr', 'mon'])
    assert str(err.value) == osd_msg


def test_validate_failure_domain_upgrade_options_name_not_consulting_crush_map(
        cephadm_module: CephadmOrchestrator):
    """Name existence in the CRUSH map is not validated here."""
    cephadm_module.upgrade._validate_failure_domain_upgrade_options(
        'rack', 'not-in-map', ['osd'])


@mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm('{}'))
@pytest.mark.parametrize(
    "prior_autoscale,pg_autoscale_during_upgrade,autoscale_during_upgrade",
    [
        # Decision table: prior_autoscale, pg_autoscale_during_upgrade -> autoscale_during_upgrade
        (False, False, False),  # prior off, no opt-in -> autoscale off during upgrade
        (False, True, True),   # prior off, opt-in -> autoscale on during upgrade
        (True, False, False),  # prior on, no opt-in -> autoscale off during upgrade
        (True, True, True),    # prior on, opt-in -> autoscale on during upgrade
    ],
)
def test_pg_autoscale_decision_table(
    prior_autoscale,
    pg_autoscale_during_upgrade,
    autoscale_during_upgrade,
    cephadm_module: CephadmOrchestrator,
):
    """Test PG autoscaling decision table at upgrade start.
    Verifies that prior_autoscale and pg_autoscale_during_upgrade produce the
    expected autoscale_during_upgrade decision, and that _set_noautoscale is
    called only when autoscale_during_upgrade is False.
    """
    expect_set_noautoscale = not autoscale_during_upgrade
    with with_host(cephadm_module, 'host1'):
        with with_host(cephadm_module, 'host2'):
            with with_service(
                cephadm_module,
                ServiceSpec('mgr', placement=PlacementSpec(host_pattern='*', count=2)),
                status_running=True,
            ):
                cephadm_module.pg_autoscale_during_upgrade = pg_autoscale_during_upgrade

                with mock.patch.object(
                    cephadm_module.upgrade,
                    '_is_upgrade_autoscaling_allowed',
                    return_value=prior_autoscale,
                ), mock.patch.object(
                    cephadm_module.upgrade,
                    '_set_noautoscale',
                    return_value=True,
                ) as mock_set_noautoscale:
                    result = wait(
                        cephadm_module,
                        cephadm_module.upgrade_start('image_id', None),
                    )
                    assert result == 'Initiating upgrade to image_id'

                    # Decision: autoscale_during_upgrade=False -> set noautoscale
                    if expect_set_noautoscale:
                        mock_set_noautoscale.assert_called_once()
                        assert cephadm_module.upgrade.upgrade_state.noautoscale_set is True
                    else:
                        mock_set_noautoscale.assert_not_called()
                        assert getattr(
                            cephadm_module.upgrade.upgrade_state,
                            'noautoscale_set',
                            False,
                        ) is False


@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
@mock.patch("cephadm.serve.CephadmServe._get_container_image_info")
@pytest.mark.parametrize(
    "daemon_types,expect_set_noautoscale",
    [
        (['mon', 'mgr'], False),        # excludes OSDs -> noautoscale not set
        (['mon', 'mgr', 'osd'], True),  # includes OSDs, prior_autoscale=False -> noautoscale set
    ],
)
def test_pg_autoscale_skipped_when_upgrade_excludes_osds(
    _get_container_image_info, cephadm_module: CephadmOrchestrator,
    daemon_types, expect_set_noautoscale
):
    """When upgrade excludes OSDs, _set_noautoscale should not be called.
    When it includes OSDs and prior_autoscale=False, _set_noautoscale should be called.
    """
    _get_container_image_info.side_effect = async_side_effect(
        ('img_id', 'ceph version 18.2.0 (hash)', ['digest'])
    )
    with with_host(cephadm_module, 'host1'):
        with with_host(cephadm_module, 'host2'):
            with with_service(
                cephadm_module,
                ServiceSpec('mgr', placement=PlacementSpec(host_pattern='*', count=2)),
                status_running=True,
            ):
                cephadm_module.pg_autoscale_during_upgrade = False

                with mock.patch.object(
                    cephadm_module.upgrade,
                    '_is_upgrade_autoscaling_allowed',
                    return_value=False,
                ), mock.patch.object(
                    cephadm_module.upgrade,
                    '_set_noautoscale',
                    return_value=True,
                ) as mock_set_noautoscale:
                    result = wait(
                        cephadm_module,
                        cephadm_module.upgrade_start(
                            'image_id', None,
                            daemon_types=daemon_types,
                        ),
                    )
                    assert result == 'Initiating upgrade to image_id'
                    if expect_set_noautoscale:
                        mock_set_noautoscale.assert_called_once()
                    else:
                        mock_set_noautoscale.assert_not_called()


@pytest.mark.parametrize(
    "prior_autoscale,opt_in,autoscale_after",
    [
        # Decision table: prior_autoscale, opt-in -> autoscale_after
        (False, False, False),  # Case 1: prior off, no opt-in -> autoscale off after
        (False, True, False),   # Case 2: prior off, opt-in -> autoscale off after (revert)
        (True, False, True),   # Case 3: prior on, no opt-in -> autoscale on after (revert)
        (True, True, True),    # Case 4: prior on, opt-in -> autoscale on after
    ],
)
@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
@mock.patch("cephadm.serve.CephadmServe._get_container_image_info")
@mock.patch("cephadm.CephadmOrchestrator.check_mon_command")
def test_pg_autoscale_revert_after_upgrade(
    check_mon_command,
    _get_container_image_info,
    prior_autoscale,
    opt_in,
    autoscale_after,
    cephadm_module: CephadmOrchestrator,
):
    """Test autoscale_after per decision table: prior_autoscale, opt-in -> autoscale_after.
    Cases 1,3: we set noautoscale during upgrade, so we restore to prior on stop.
    Cases 2,4: we never set noautoscale, so no restore; cluster stays as prior.
    """
    _get_container_image_info.side_effect = async_side_effect(
        ('img_id', 'ceph version 18.2.0 (hash)', ['digest'])
    )
    check_mon_command.return_value = (0, '', '')

    with with_host(cephadm_module, 'host1'):
        with with_host(cephadm_module, 'host2'):
            with with_service(
                cephadm_module,
                ServiceSpec('mgr', placement=PlacementSpec(host_pattern='*', count=2)),
                status_running=True,
            ):
                cephadm_module.pg_autoscale_during_upgrade = opt_in

                with mock.patch.object(
                    cephadm_module.upgrade,
                    '_is_upgrade_autoscaling_allowed',
                    return_value=prior_autoscale,
                ):
                    wait(
                        cephadm_module,
                        cephadm_module.upgrade_start(
                            'image_id', None,
                            daemon_types=['mon', 'mgr', 'osd'],
                        ),
                    )

                # upgrade_stop triggers _unset_noautoscale when noautoscale_set
                check_mon_command.reset_mock()
                wait(cephadm_module, cephadm_module.upgrade_stop())

                # Verify autoscale_after: restore path (cases 1,3) vs no restore (cases 2,4)
                config_calls = [
                    c for c in check_mon_command.call_args_list
                    if isinstance(c[0][0], dict)
                    and c[0][0].get('name') == 'osd_pool_default_pg_autoscale_mode'
                ]
                if not opt_in:
                    # Cases 1,3: we set noautoscale, so we restore
                    assert len(config_calls) >= 1
                    expected_value = 'on' if autoscale_after else 'off'
                    assert any(
                        c[0][0].get('value') == expected_value
                        for c in config_calls
                    )
                else:
                    # Cases 2,4: we never set noautoscale, so no restore calls
                    assert len(config_calls) == 0


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
        (  # invalid, can't upgrade crash on a while mon on a is not upgraded
            [('mgr', 'a', 'a.x')],
            [('mon', 'a', 'a'), ('crash', 'a', 'a')],
            ['crash'],
            ['a'],
            None,
            True
        ),
        (  # invalid, can't upgrade crash on a while mgr on a is not upgraded
            [('mon', 'a', 'a')],
            [('mgr', 'a', 'a.x'), ('crash', 'a', 'a')],
            ['crash'],
            ['a'],
            None,
            True
        ),
        (  # invalid, can't upgrade crash service on a while mon on a is not upgraded
            [('mgr', 'a', 'a.x')],
            [('mon', 'a', 'a'), ('crash', 'a', 'a')],
            None,
            ['a'],
            ['crash'],
            True
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
        not_upgraded: List[Tuple[str, str, str]],
        daemon_types: Optional[str],
        hosts: Optional[str],
        services: Optional[str],
        should_block: bool,
        cephadm_module: CephadmOrchestrator,
):
    def to_dds(ts: List[Tuple[str, str, str]], upgraded: bool) -> List[DaemonDescription]:
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


@mock.patch("cephadm.module.HostCache.get_daemons")
def test_filtered_scope_up_to_date(
    get_daemons: mock.MagicMock,
    cephadm_module: CephadmOrchestrator,
) -> None:
    target_digest = 'new_image@repo_digest'
    old_digest = 'old_image@repo_digest'

    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target_image',
        '0',
        target_digests=[target_digest],
        daemon_types=['mon'],
        hosts=['trial031'],
    )

    get_daemons.return_value = [
        DaemonDescription(
            daemon_type='mon',
            daemon_id='a',
            hostname='trial031',
            container_image_digests=[target_digest],
        ),
        DaemonDescription(
            daemon_type='mon',
            daemon_id='c',
            hostname='trial031',
            container_image_digests=[old_digest],
        ),
    ]

    assert not cephadm_module.upgrade._filtered_scope_up_to_date(
        [target_digest], 'target_image',
    )

    get_daemons.return_value[1].container_image_digests = [target_digest]
    assert cephadm_module.upgrade._filtered_scope_up_to_date(
        [target_digest], 'target_image',
    )


@mock.patch.object(CephadmUpgrade, '_mark_upgrade_complete')
@mock.patch.object(CephadmUpgrade, '_filtered_scope_up_to_date', return_value=False)
@mock.patch.object(CephadmUpgrade, '_handle_need_upgrade_self')
@mock.patch.object(CephadmUpgrade, '_to_upgrade', return_value=(True, []))
@mock.patch.object(
    CephadmUpgrade, '_detect_need_upgrade', return_value=(False, [], [], 0),
)
@mock.patch.object(CephadmUpgrade, '_set_container_images')
@mock.patch.object(CephadmUpgrade, '_complete_osd_upgrade')
@mock.patch.object(CephadmUpgrade, '_complete_mds_upgrade')
@mock.patch.object(CephadmUpgrade, '_update_upgrade_progress')
@mock.patch.object(CephadmUpgrade, 'get_distinct_container_image_settings', return_value={})
@mock.patch("cephadm.module.CephadmOrchestrator.lookup_release_name", return_value='tentacle')
@mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command", return_value=(0, '{}', ''))
@mock.patch("cephadm.module.CephadmOrchestrator.set_container_image")
@mock.patch("cephadm.module.CephadmOrchestrator.get_active_mgr_digests")
@mock.patch("cephadm.module.CephadmOrchestrator.get", return_value={
    'min_mon_release': 19,
    'require_osd_release': 'tentacle',
    'have_local_config_map': True,
})
@mock.patch(
    "cephadm.module.CephadmOrchestrator.version",
    new_callable=mock.PropertyMock,
    return_value='ceph version 19.3.0-0 (hash)',
)
@mock.patch("cephadm.module.HostCache.get_daemons")
def test_do_upgrade_limit_exhausted_marks_complete_without_scope_check(
    get_daemons: mock.MagicMock,
    _version: mock.MagicMock,
    _get: mock.MagicMock,
    get_active_mgr_digests: mock.MagicMock,
    _set_container_image: mock.MagicMock,
    _check_mon_command: mock.MagicMock,
    _lookup_release_name: mock.MagicMock,
    _get_distinct_container_image_settings: mock.MagicMock,
    _update_upgrade_progress: mock.MagicMock,
    _complete_mds_upgrade: mock.MagicMock,
    _complete_osd_upgrade: mock.MagicMock,
    _set_container_images: mock.MagicMock,
    _detect_need_upgrade: mock.MagicMock,
    _to_upgrade: mock.MagicMock,
    _handle_need_upgrade_self: mock.MagicMock,
    filtered_scope: mock.MagicMock,
    mark_complete: mock.MagicMock,
    cephadm_module: CephadmOrchestrator,
) -> None:
    target_digest = 'new_image@repo_digest'
    old_digest = 'old_image@repo_digest'
    get_active_mgr_digests.return_value = [target_digest]
    get_daemons.return_value = [
        DaemonDescription(
            daemon_type='osd',
            daemon_id=str(i),
            hostname='host1',
            container_image_digests=[target_digest] if i < 2 else [old_digest],
        )
        for i in range(8)
    ]

    cephadm_module.upgrade.upgrade_state = UpgradeState(
        'target_image',
        '0',
        target_id='image_id',
        target_digests=[target_digest],
        target_version='19.3.0-0',
        daemon_types=['osd'],
        total_count=2,
        remaining_count=0,
    )

    cephadm_module.upgrade._do_upgrade()

    mark_complete.assert_called_once()
    filtered_scope.assert_not_called()
