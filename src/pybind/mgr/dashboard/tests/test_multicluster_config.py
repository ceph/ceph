# -*- coding: utf-8 -*-
import json
import unittest
from unittest.mock import patch

from ..controllers import auth as auth_controller
from ..controllers.multi_cluster import MultiCluster
from ..services import multicluster_config as mc_store
from ..tests import KVStoreMockMixin

FSID = '5f5b6f4e-0000-0000-0000-000000000001'
REMOTE_FSID = '5f5b6f4e-0000-0000-0000-000000000002'
SECRET_TOKEN = 'header.eyJleHAiOjk5OTk5OTk5OTl9.signature'  # noqa: S105


def _config():
    return {
        'current_url': 'https://hub:8443',
        'current_user': 'admin',
        'hub_url': 'https://hub:8443',
        'config': {
            FSID: [{
                'name': FSID, 'url': 'https://hub:8443',
                'cluster_alias': 'local-cluster', 'user': 'admin',
                'token': 'old-local-token',
            }],
            REMOTE_FSID: [{
                'name': REMOTE_FSID, 'url': 'https://remote:8443',
                'cluster_alias': 'remote', 'user': 'admin',
                'token': SECRET_TOKEN,
                'prometheus_url': 'https://remote:9095',
                'prometheus_access_info': {'user': 'u', 'password': 'p', 'certificate': 'c'},
                'ssl_verify': False, 'ssl_certificate': '',
            }],
        },
    }


class MultiClusterConfigStoreTest(unittest.TestCase, KVStoreMockMixin):
    def setUp(self):
        self.mock_kv_store()
        mc_store._legacy_checked = False  # pylint: disable=protected-access

    def test_save_and_load_use_kv_store_not_module_option(self):
        mc_store.save(_config())
        self.assertIn(mc_store.STORE_KEY, self.CONFIG_KEY_DICT)
        self.assertEqual(mc_store.load()['config'][REMOTE_FSID][0]['token'], SECRET_TOKEN)

    def test_load_empty(self):
        self.assertEqual(mc_store.load(), {})

    def test_load_corrupted(self):
        self.set_key(mc_store.STORE_KEY, 'not json')
        self.assertEqual(mc_store.load(), {})

    def test_legacy_option_is_migrated_and_removed(self):
        self.set_key(mc_store.LEGACY_OPTION, json.dumps(_config()))
        config = mc_store.load()
        # remote cluster credentials are preserved: existing setups keep working
        remote = config['config'][REMOTE_FSID][0]
        self.assertEqual(remote['token'], SECRET_TOKEN)
        self.assertEqual(remote['prometheus_access_info']['password'], 'p')
        # the stale token stored for the local cluster is dropped
        self.assertNotIn('token', config['config'][FSID][0])
        # the plain text copy is gone from the module options
        self.assertIsNone(self.get_key(mc_store.LEGACY_OPTION))
        self.assertIn(mc_store.STORE_KEY, self.CONFIG_KEY_DICT)

    def test_legacy_option_as_python_literal_is_migrated(self):
        # values saved by older releases could be stored as str(dict)
        self.set_key(mc_store.LEGACY_OPTION, str(_config()))
        self.assertIn(REMOTE_FSID, mc_store.load()['config'])
        self.assertIsNone(self.get_key(mc_store.LEGACY_OPTION))

    def test_migration_runs_once(self):
        mc_store.save(_config())
        mc_store.load()
        self.set_key(mc_store.LEGACY_OPTION, json.dumps({'config': {}}))
        # already checked: a later write to the legacy option is not picked up
        self.assertIn(REMOTE_FSID, mc_store.load()['config'])

    def test_redact_removes_all_secrets(self):
        redacted = mc_store.redact(_config())
        dumped = json.dumps(redacted)
        self.assertNotIn(SECRET_TOKEN, dumped)
        self.assertNotIn('old-local-token', dumped)
        self.assertNotIn('prometheus_access_info', dumped)
        remote = redacted['config'][REMOTE_FSID][0]
        # the non secret fields used by the UI are untouched
        self.assertEqual(remote['url'], 'https://remote:8443')
        self.assertEqual(remote['prometheus_url'], 'https://remote:9095')
        self.assertEqual(remote['cluster_alias'], 'remote')

    def test_redact_does_not_modify_the_input(self):
        config = _config()
        mc_store.redact(config)
        self.assertEqual(config, _config())

    def test_redact_reveals_only_the_requested_remote_token(self):
        redacted = mc_store.redact(_config(),
                                   reveal_token_for=('https://remote:8443', 'admin'))
        self.assertEqual(redacted['config'][REMOTE_FSID][0]['token'], SECRET_TOKEN)
        self.assertNotIn('prometheus_access_info', redacted['config'][REMOTE_FSID][0])
        self.assertNotIn('token', redacted['config'][FSID][0])

    def test_redact_never_reveals_a_local_cluster_token(self):
        redacted = mc_store.redact(_config(), reveal_token_for=('https://hub:8443', 'admin'))
        self.assertNotIn('token', redacted['config'][FSID][0])

    def test_find_token(self):
        self.assertEqual(mc_store.find_token(_config(), 'https://remote:8443', 'admin'),
                         SECRET_TOKEN)
        self.assertIsNone(mc_store.find_token(_config(), 'https://remote:8443', 'other'))


class MultiClusterControllerTest(unittest.TestCase, KVStoreMockMixin):
    def setUp(self):
        self.mock_kv_store()
        mc_store._legacy_checked = False  # pylint: disable=protected-access
        mc_store.save(_config())
        self.ctrl = MultiCluster()

    def test_get_config_does_not_leak_secrets(self):
        result = self.ctrl.get_config()
        self.assertNotIn(SECRET_TOKEN, json.dumps(result))
        self.assertNotIn('prometheus_access_info', json.dumps(result))

    def test_set_config_returns_only_the_selected_cluster_token(self):
        result = self.ctrl.set_config({'url': 'https://remote:8443', 'user': 'admin'})
        self.assertEqual(result['config'][REMOTE_FSID][0]['token'], SECRET_TOKEN)
        self.assertEqual(result['current_url'], 'https://remote:8443')
        # selection is persisted
        self.assertEqual(mc_store.load()['current_url'], 'https://remote:8443')

    def test_set_config_to_local_cluster_returns_no_token(self):
        result = self.ctrl.set_config({'url': 'https://hub:8443', 'user': 'admin'})
        self.assertNotIn(SECRET_TOKEN, json.dumps(result))

    def test_edit_cluster_response_is_redacted_and_keeps_stored_secrets(self):
        result = self.ctrl.edit_cluster(REMOTE_FSID, 'https://new:8443', 'alias', 'admin')
        self.assertNotIn(SECRET_TOKEN, json.dumps(result))
        stored = mc_store.load()['config'][REMOTE_FSID][0]
        self.assertEqual(stored['url'], 'https://new:8443')
        self.assertEqual(stored['token'], SECRET_TOKEN)

    def test_check_token_status_skips_clusters_without_token(self):
        # local cluster entries carry no token anymore
        config = _config()
        config['config'][REMOTE_FSID][0]['token'] = (
            'h.eyJleHAiOjF9.s')  # exp=1 -> expired
        mc_store.save(config)
        status = self.ctrl.check_token_status_array()
        self.assertNotIn(FSID, status)
        self.assertEqual(status[REMOTE_FSID]['status'], 1)
        self.assertEqual(status[REMOTE_FSID]['user'], 'admin')

    def test_reconnect_without_password_uses_the_stored_token(self):
        with patch.object(MultiCluster, 'check_connection_errors') as check, \
                patch.object(MultiCluster, '_proxy', return_value='x'), \
                patch('dashboard.controllers.multi_cluster._remove_prometheus_targets'), \
                patch('dashboard.controllers.multi_cluster._set_prometheus_targets'), \
                patch('dashboard.controllers.multi_cluster.time.sleep'):
            self.assertTrue(self.ctrl.reconnect_cluster('https://remote:8443', 'admin'))
        check.assert_called_once()
        self.assertEqual(check.call_args[0][1], SECRET_TOKEN)

    def test_reconnect_without_password_nor_stored_token_fails(self):
        from ..exceptions import DashboardException
        with self.assertRaises(DashboardException):
            self.ctrl.reconnect_cluster('https://unknown:8443', 'admin')


class LoginDoesNotStoreTokensTest(unittest.TestCase, KVStoreMockMixin):
    def setUp(self):
        self.mock_kv_store()
        mc_store._legacy_checked = False  # pylint: disable=protected-access

    def test_first_login_registers_local_cluster_without_token(self):
        auth_controller._register_local_cluster(  # pylint: disable=protected-access
            FSID, 'https://hub:8443', 'admin')
        config = mc_store.load()
        entry = config['config'][FSID][0]
        self.assertEqual(entry['cluster_alias'], 'local-cluster')
        self.assertNotIn('token', entry)
        self.assertEqual(config['current_user'], 'admin')

    def test_login_never_overwrites_remote_entries(self):
        mc_store.save(_config())
        auth_controller._register_local_cluster(  # pylint: disable=protected-access
            FSID, 'https://hub:8443', 'other-user')
        config = mc_store.load()
        self.assertEqual(config['config'][REMOTE_FSID][0]['token'], SECRET_TOKEN)
        self.assertEqual(config['config'][REMOTE_FSID][0]['user'], 'admin')

    def test_login_does_not_write_the_legacy_option(self):
        auth_controller._register_local_cluster(  # pylint: disable=protected-access
            FSID, 'https://hub:8443', 'admin')
        self.assertIsNone(self.get_key(mc_store.LEGACY_OPTION))
