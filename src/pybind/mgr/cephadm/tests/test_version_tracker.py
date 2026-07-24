import pytest
from tests import mock

import json
import errno

from cephadm.version_tracker import VERSION_HISTORY_KEY_PREFIX, VersionTracker
from ceph.cephadm.version_entry import UpgradeType, UpgradeStatus, CephVersionEntry
from cephadm.module import CephadmOrchestrator
from cephadm.upgrade import UpgradeState


class TestCephVersionEntry(object):

    @pytest.mark.parametrize(
        "upgrade_type, status, command_options, config_dump",
        [
            # testing all possible enum values, command_options as None, and config_dump as None
            (UpgradeType.FULL, UpgradeStatus.STARTED, {'image': 'quay.io/ceph/ceph:18.2.8', 'daemon_types': None}, [{'section': 'global'}, {'section': 'mon'}]),
            (UpgradeType.STAGGERED, UpgradeStatus.STOPPED, {'image': 'quay.io/ceph/ceph:18.2.8', 'daemon_types': None}, None),
            (UpgradeType.BOOTSTRAP, UpgradeStatus.COMPLETE, None, [{'section': 'global'}, {'section': 'mon'}])
        ]
    )
    def test_version_entry_to_json_is_serializable(self, upgrade_type, status, command_options, config_dump):
        entry = CephVersionEntry(
            version='18.2.8',
            upgrade_type=upgrade_type,
            status=status,
            command_options=command_options,
            config_dump=config_dump
        )
        serialized = json.dumps(entry.to_json())
        parsed = json.loads(serialized)
        assert parsed['upgrade_type'] == upgrade_type.value
        assert parsed['status'] == status.value
        assert parsed['command_options'] == command_options
        assert parsed['config_dump'] == config_dump


class TestVersionTracker(object):

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command")
    def test_add_cluster_version_full_upgrade(self, _check_mon_command, _set_store, cephadm_module: CephadmOrchestrator):
        _check_mon_command.return_value = (0, json.dumps([{'section': 'global'}, {'section': 'mon'}]), '')
        vt: VersionTracker = cephadm_module.version_tracker
        params = {'image': 'quay.io/ceph/ceph:v18.2.8', 'version': '18.2.8', 'daemon_types': None}
        vt.add_cluster_version('18.2.8', '2026-07-07 00:00:00', params, UpgradeStatus.STARTED)
        expected_entry = CephVersionEntry(
            version='18.2.8',
            upgrade_type=UpgradeType.FULL,
            command_options=params,
            status=UpgradeStatus.STARTED,
            config_dump=[{'section': 'global'}, {'section': 'mon'}]
        ).to_json()
        _set_store.assert_called_once_with(
            f'{VERSION_HISTORY_KEY_PREFIX}2026-07-07 00:00:00',
            json.dumps(expected_entry)
        )

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command")
    def test_add_cluster_version_staggered_upgrade(self, _check_mon_command, _set_store, cephadm_module: CephadmOrchestrator):
        _check_mon_command.return_value = (0, json.dumps([{'section': 'global'}, {'section': 'mon'}]), '')
        vt: VersionTracker = cephadm_module.version_tracker
        params = {'image': 'quay.io/ceph/ceph:v18.2.8', 'version': '18.2.8', 'daemon_types': ['mgr']}
        vt.add_cluster_version('18.2.8', '2026-07-07 00:00:00', params, UpgradeStatus.STARTED)
        expected_entry = CephVersionEntry(
            version='18.2.8',
            upgrade_type=UpgradeType.STAGGERED,
            command_options=params,
            status=UpgradeStatus.STARTED,
            config_dump=[{'section': 'global'}, {'section': 'mon'}]
        ).to_json()
        _set_store.assert_called_once_with(
            f'{VERSION_HISTORY_KEY_PREFIX}2026-07-07 00:00:00',
            json.dumps(expected_entry)
        )

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command")
    def test_add_cluster_version_check_mon_command_raises(self, _check_mon_command, _set_store, cephadm_module: CephadmOrchestrator):
        _check_mon_command.side_effect = Exception('mon command failure')
        vt: VersionTracker = cephadm_module.version_tracker
        params = {'image': 'quay.io/ceph/ceph:v18.2.8', 'version': '18.2.8', 'daemon_types': ['mgr']}
        with mock.patch.object(cephadm_module.log, 'error') as err_mock:
            vt.add_cluster_version('18.2.8', '2026-07-07 00:00:00', params, UpgradeStatus.STARTED)
            err_mock.assert_called_once()
        expected_entry = CephVersionEntry(
            version='18.2.8',
            upgrade_type=UpgradeType.STAGGERED,
            command_options=params,
            status=UpgradeStatus.STARTED,
            config_dump=None
        ).to_json()
        _set_store.assert_called_once_with(
            f'{VERSION_HISTORY_KEY_PREFIX}2026-07-07 00:00:00',
            json.dumps(expected_entry)
        )

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.check_mon_command")
    def test_add_cluster_version_check_mon_command_empty_output(self, _check_mon_command, _set_store, cephadm_module: CephadmOrchestrator):
        _check_mon_command.return_value = (0, '', '')
        vt: VersionTracker = cephadm_module.version_tracker
        params = {'image': 'quay.io/ceph/ceph:v18.2.8', 'version': '18.2.8', 'daemon_types': ['mgr']}
        vt.add_cluster_version('18.2.8', '2026-07-07 00:00:00', params, UpgradeStatus.STARTED)
        expected_entry = CephVersionEntry(
            version='18.2.8',
            upgrade_type=UpgradeType.STAGGERED,
            command_options=params,
            status=UpgradeStatus.STARTED,
            config_dump=None
        ).to_json()
        _set_store.assert_called_once_with(
            f'{VERSION_HISTORY_KEY_PREFIX}2026-07-07 00:00:00',
            json.dumps(expected_entry)
        )

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_update_cluster_version_status_updates_for_one_entry(self, _get_store_prefix, _set_store, cephadm_module: CephadmOrchestrator):
        entry = {'version': '18.2.8', 'status': UpgradeStatus.STARTED}
        key = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        _get_store_prefix.return_value = {
            key: json.dumps(entry)
        }
        vt: VersionTracker = cephadm_module.version_tracker
        upgrade_state = mock.MagicMock(spec=UpgradeState)
        vt.update_cluster_version_status(upgrade_state, UpgradeStatus.COMPLETE)
        expected = dict(entry, status=UpgradeStatus.COMPLETE)
        _set_store.assert_called_once_with(key, json.dumps(expected))

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_update_cluster_version_status_updates_lastest_entry_for_multiple(self, _get_store_prefix, _set_store, cephadm_module: CephadmOrchestrator):
        entry1 = {'version': '18.2.8', 'status': UpgradeStatus.COMPLETE}
        entry2 = {'version': '19.2.3', 'status': UpgradeStatus.STARTED}
        key1 = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        key2 = f'{VERSION_HISTORY_KEY_PREFIX}2027-01-01 00:00:00'
        _get_store_prefix.return_value = {
            key1: json.dumps(entry1),
            key2: json.dumps(entry2)
        }
        vt: VersionTracker = cephadm_module.version_tracker
        upgrade_state = mock.MagicMock(spec=UpgradeState)
        vt.update_cluster_version_status(upgrade_state, UpgradeStatus.COMPLETE)
        expected = dict(entry2, status=UpgradeStatus.COMPLETE)
        _set_store.assert_called_once_with(key2, json.dumps(expected))

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_update_cluster_version_status_adds_entry_when_no_entry_found(self, _get_store_prefix, _set_store, cephadm_module: CephadmOrchestrator):
        _get_store_prefix.return_value = {}
        vt: VersionTracker = cephadm_module.version_tracker
        upgrade_state = mock.MagicMock(spec=UpgradeState)
        upgrade_state.target_name = '19.2.3'
        upgrade_state.daemon_types = None
        upgrade_state.hosts = None
        upgrade_state.services = None
        upgrade_state.total_count = None
        upgrade_state.crush_bucket_type = None
        upgrade_state.crush_bucket_name = None
        vt.update_cluster_version_status(upgrade_state, UpgradeStatus.COMPLETE)
        _set_store.assert_called_once()
        called_key, called_value = _set_store.call_args.args
        assert called_key.startswith(VERSION_HISTORY_KEY_PREFIX)
        stored_entry = json.loads(called_value)
        assert stored_entry['version'] == '19.2.3'
        assert stored_entry['status'] == UpgradeStatus.COMPLETE
        assert stored_entry['upgrade_type'] == UpgradeType.FULL

    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_get_cluster_version_history_empty(self, _get_store_prefix, cephadm_module: CephadmOrchestrator):
        _get_store_prefix.return_value = {}
        vt: VersionTracker = cephadm_module.version_tracker
        out = vt.get_cluster_version_history()
        assert out == 'No Cluster Version History Stored'

    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_get_cluster_version_history_all_option_false(self, _get_store_prefix, cephadm_module: CephadmOrchestrator):
        entry = {'version': '18.2.8', 'status': UpgradeStatus.STARTED, 'config_dump': [{'section': 'global'}, {'section': 'mon'}]}
        key = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        _get_store_prefix.return_value = {
            key: json.dumps(entry)
        }
        vt: VersionTracker = cephadm_module.version_tracker
        out = vt.get_cluster_version_history(show_config=False)
        parsed = json.loads(out)
        assert 'config_dump' not in parsed[key[16:]]

    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_get_cluster_version_history_all_option_true(self, _get_store_prefix, cephadm_module: CephadmOrchestrator):
        entry = {'version': '18.2.8', 'status': UpgradeStatus.STARTED, 'config_dump': [{'section': 'global'}, {'section': 'mon'}]}
        key = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        _get_store_prefix.return_value = {
            key: json.dumps(entry)
        }
        vt: VersionTracker = cephadm_module.version_tracker
        out = vt.get_cluster_version_history(show_config=True)
        parsed = json.loads(out)
        assert parsed[key[16:]]['config_dump'] == [{'section': 'global'}, {'section': 'mon'}]

    def test_remove_cluster_version_history_no_options(self, cephadm_module: CephadmOrchestrator):
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history()
        assert err == -errno.EINVAL
        assert msg == 'requires at least one of the options "all", "before", "after" to be set'

    def test_remove_cluster_version_history_option_all_and_after_conflict(self, cephadm_module: CephadmOrchestrator):
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(all=True, after='2026-01-01 00:00:00')
        assert err == -errno.EINVAL
        assert msg == 'cannot have option "all" set while option "before" or option "after" are set'

    def test_remove_cluster_version_history_option_all_and_before_conflict(self, cephadm_module: CephadmOrchestrator):
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(all=True, before='2026-01-01 00:00:00')
        assert err == -errno.EINVAL
        assert msg == 'cannot have option "all" set while option "before" or option "after" are set'

    def test_remove_cluster_version_history_invalid_format(self, cephadm_module: CephadmOrchestrator):
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(before='00:00:00 2026-01-01')
        assert err == -errno.EINVAL
        assert msg == 'invalid datetime format for option "before", use "YYYY-MM-DD HH:MM:SS"'

        err, msg = vt.remove_cluster_version_history(after='00:00:00 2026-01-01')
        assert err == -errno.EINVAL
        assert msg == 'invalid datetime format for option "after", use "YYYY-MM-DD HH:MM:SS"'

    def test_remove_cluster_version_history_before_less_than_after_conflict(self, cephadm_module: CephadmOrchestrator):
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(before='2026-10-01 00:00:00', after='2027-10-01 00:00:00')
        assert err == -errno.EINVAL
        assert msg == 'option "before" cannot be a datetime less than option "after", command will remove entries in range (AFTER, BEFORE)'

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_remove_cluster_version_history_all(self, _get_store_prefix, _set_store, cephadm_module: CephadmOrchestrator):
        key1 = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        key2 = f'{VERSION_HISTORY_KEY_PREFIX}2027-01-01 00:00:00'
        key3 = f'{VERSION_HISTORY_KEY_PREFIX}2027-10-01 00:00:00'
        _get_store_prefix.return_value = {
            key1: '{}',
            key2: '{}',
            key3: '{}',
        }
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(all=True)
        assert err == 0
        assert msg == 'Cluster Version History Deletion Successful'
        _set_store.assert_has_calls([
            mock.call(key1, None),
            mock.call(key2, None),
            mock.call(key3, None)
        ])
        assert _set_store.call_count == 3

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_remove_cluster_version_history_before_only(self, _get_store_prefix, _set_store, cephadm_module: CephadmOrchestrator):
        key1 = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        key2 = f'{VERSION_HISTORY_KEY_PREFIX}2027-01-01 00:00:00'
        key3 = f'{VERSION_HISTORY_KEY_PREFIX}2027-10-01 00:00:00'
        _get_store_prefix.return_value = {
            key1: '{}',
            key2: '{}',
            key3: '{}',
        }
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(before='2027-01-01 00:00:00')
        assert err == 0
        assert msg == 'Cluster Version History Deletion Successful'
        _set_store.assert_called_once_with(key1, None)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_remove_cluster_version_history_after_only(self, _get_store_prefix, _set_store, cephadm_module: CephadmOrchestrator):
        key1 = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        key2 = f'{VERSION_HISTORY_KEY_PREFIX}2027-01-01 00:00:00'
        key3 = f'{VERSION_HISTORY_KEY_PREFIX}2027-10-01 00:00:00'
        _get_store_prefix.return_value = {
            key1: '{}',
            key2: '{}',
            key3: '{}',
        }
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(after='2027-01-01 00:00:00')
        assert err == 0
        assert msg == 'Cluster Version History Deletion Successful'
        _set_store.assert_called_once_with(key3, None)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_remove_cluster_version_history_before_and_after_valid_range(self, _get_store_prefix, _set_store, cephadm_module: CephadmOrchestrator):
        key1 = f'{VERSION_HISTORY_KEY_PREFIX}2026-01-01 00:00:00'
        key2 = f'{VERSION_HISTORY_KEY_PREFIX}2027-01-01 00:00:00'
        key3 = f'{VERSION_HISTORY_KEY_PREFIX}2027-10-01 00:00:00'
        _get_store_prefix.return_value = {
            key1: '{}',
            key2: '{}',
            key3: '{}',
        }
        vt: VersionTracker = cephadm_module.version_tracker
        err, msg = vt.remove_cluster_version_history(before='2027-10-01 00:00:00', after='2026-01-01 00:00:00')
        assert err == 0
        assert msg == 'Cluster Version History Deletion Successful'
        _set_store.assert_called_once_with(key2, None)
