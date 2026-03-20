import json
import unittest

from ..tests import CLICommandTestMixin


class TestNVMeoFConfCLI(unittest.TestCase, CLICommandTestMixin):
    def setUp(self):
        self.mock_kv_store()

    def test_cli_add_gateway(self):

        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool.group',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group='group'
        )

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool.group': [{
                    'group': 'group',
                    'daemon_name': 'nvmeof_daemon',
                    'service_url': 'http://nvmf:port'
                }]
            }
        )

    def test_cli_migration_from_legacy_config(self):
        legacy_config = json.dumps({
            'gateways': {
                'nvmeof.pool': {
                    'service_url': 'http://nvmf:port'
                }
            }
        })
        self.set_key('_nvmeof_config', legacy_config)

        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon',
                    'group': '',
                    'service_url': 'http://nvmf:port'
                }]
            }
        )

    def test_cli_add_gw_to_existing(self):
        # add first gw
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        # add another daemon to the first gateway
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf-2:port',
            daemon_name='nvmeof_daemon_2',
            group=''
        )

        config = json.loads(self.get_key('_nvmeof_config'))

        # make sure its appended to the existing gateway
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon',
                    'group': '',
                    'service_url': 'http://nvmf:port'
                }, {
                    'daemon_name': 'nvmeof_daemon_2',
                    'group': '',
                    'service_url': 'http://nvmf-2:port'
                }]
            }
        )

    def test_cli_add_new_gw(self):
        # add first config
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        # add another gateway
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof2.pool.group',
            inbuf='http://nvmf-2:port',
            daemon_name='nvmeof_daemon_2',
            group='group'
        )

        config = json.loads(self.get_key('_nvmeof_config'))

        # make sure its added as a new entry
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon',
                    'group': '',
                    'service_url': 'http://nvmf:port'
                }],
                'nvmeof2.pool.group': [{
                    'daemon_name': 'nvmeof_daemon_2',
                    'group': 'group',
                    'service_url': 'http://nvmf-2:port'
                }]
            }
        )

    def test_cli_rm_gateway(self):
        self.test_cli_add_gateway()
        self.exec_cmd('nvmeof-gateway-rm', name='nvmeof.pool.group')

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {}
        )

    def test_cli_rm_daemon_from_gateway(self):
        self.test_cli_add_gw_to_existing()
        self.exec_cmd(
            'nvmeof-gateway-rm',
            name='nvmeof.pool',
            daemon_name='nvmeof_daemon'
        )

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon_2',
                    'group': '',
                    'service_url': 'http://nvmf-2:port'
                }]
            }
        )

    def test_cli_legacy_config_rm(self):
        legacy_config = json.dumps({
            'gateways': {
                'nvmeof.pool': {
                    'service_url': 'http://nvmf:port'
                }
            }
        })
        self.set_key('_nvmeof_config', legacy_config)

        self.exec_cmd('nvmeof-gateway-rm', name='nvmeof.pool')

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {}
        )
