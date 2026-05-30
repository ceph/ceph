import json
import pytest
from unittest.mock import MagicMock, patch
from typing import Dict, List
from ceph.utils import datetime_now

from cephadm.services.nvmeof import NvmeofService, NVMEOF_CLIENT_CERT_LABEL
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import NvmeofServiceSpec
from cephadm.tests.fixtures import with_host, with_service, _run_cephadm, async_side_effect
from orchestrator import OrchestratorError
from cephadm.tlsobject_types import TLSCredentials

cephadm_root_ca = """-----BEGIN CERTIFICATE-----\nMIIE7DCCAtSgAwIBAgIUE8b2zZ64geu2ns3Zfn3/4L+Cf6MwDQYJKoZIhvcNAQEL\nBQAwFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI0MDYyNjE0NDA1M1oXDTM0\nMDYyNzE0NDA1M1owFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MIICIjANBgkqhkiG\n9w0BAQEFAAOCAg8AMIICCgKCAgEAsZRJsdtTr9GLG1lWFql5SGc46ldFanNJd1Gl\nqXq5vgZVKRDTmNgAb/XFuNEEmbDAXYIRZolZeYKMHfn0pouPRSel0OsC6/02ZUOW\nIuN89Wgo3IYleCFpkVIumD8URP3hwdu85plRxYZTtlruBaTRH38lssyCqxaOdEt7\nAUhvYhcMPJThB17eOSQ73mb8JEC83vB47fosI7IhZuvXvRSuZwUW30rJanWNhyZq\neS2B8qw2RSO0+77H6gA4ftBnitfsE1Y8/F9Z/f92JOZuSMQXUB07msznPbRJia3f\nueO8gOc32vxd1A1/Qzp14uX34yEGY9ko2lW226cZO29IVUtXOX+LueQttwtdlpz8\ne6Npm09pXhXAHxV/OW3M28MdXmobIqT/m9MfkeAErt5guUeC5y8doz6/3VQRjFEn\nRpN0WkblgnNAQ3DONPc+Qd9Fi/wZV2X7bXoYpNdoWDsEOiE/eLmhG1A2GqU/mneP\nzQ6u79nbdwTYpwqHpa+PvusXeLfKauzI8lLUJotdXy9EK8iHUofibB61OljYye6B\nG3b8C4QfGsw8cDb4APZd/6AZYyMx/V3cGZ+GcOV7WvsC8k7yx5Uqasm/kiGQ3EZo\nuNenNEYoGYrjb8D/8QzqNUTwlEh27/ps80tO7l2GGTvWVZL0PRZbmLDvO77amtOf\nOiRXMoUCAwEAAaMwMC4wGwYDVR0RBBQwEocQAAAAAAAAAAAAAAAAAAAAATAPBgNV\nHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQAxwzX5AhYEWhTV4VUwUj5+\nqPdl4Q2tIxRokqyE+cDxoSd+6JfGUefUbNyBxDt0HaBq8obDqqrbcytxnn7mpnDu\nhtiauY+I4Amt7hqFOiFA4cCLi2mfok6g2vL53tvhd9IrsfflAU2wy7hL76Ejm5El\nA+nXlkJwps01Whl9pBkUvIbOn3pXX50LT4hb5zN0PSu957rjd2xb4HdfuySm6nW4\n4GxtVWfmGA6zbC4XMEwvkuhZ7kD2qjkAguGDF01uMglkrkCJT3OROlNBuSTSBGqt\ntntp5VytHvb7KTF7GttM3ha8/EU2KYaHM6WImQQTrOfiImAktOk4B3lzUZX3HYIx\n+sByO4P4dCvAoGz1nlWYB2AvCOGbKf0Tgrh4t4jkiF8FHTXGdfvWmjgi1pddCNAy\nn65WOCmVmLZPERAHOk1oBwqyReSvgoCFo8FxbZcNxJdlhM0Z6hzKggm3O3Dl88Xl\n5euqJjh2STkBW8Xuowkg1TOs5XyWvKoDFAUzyzeLOL8YSG+gXV22gPTUaPSVAqdb\nwd0Fx2kjConuC5bgTzQHs8XWA930U3XWZraj21Vaa8UxlBLH4fUro8H5lMSYlZNE\nJHRNW8BkznAClaFSDG3dybLsrzrBFAu/Qb5zVkT1xyq0YkepGB7leXwq6vjWA5Pw\nmZbKSphWfh0qipoqxqhfkw==\n-----END CERTIFICATE-----\n"""


ceph_generated_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""


ceph_generated_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


class FakeInventory:
    def get_addr(self, name: str) -> str:
        return '1.2.3.4'


class FakeMgr:
    def __init__(self):
        self.config = ''
        self.set_mon_crush_locations: Dict[str, List[str]] = {}
        self.check_mon_command = MagicMock(side_effect=self._check_mon_command)
        self.mon_command = MagicMock(side_effect=self._check_mon_command)
        self.template = MagicMock()
        self.log = MagicMock()
        self.cert_mgr = MagicMock()
        self.inventory = FakeInventory()

    def _check_mon_command(self, cmd_dict, inbuf=None):
        prefix = cmd_dict.get('prefix')
        if prefix == 'get-cmd':
            return 0, self.config, ''
        if prefix == 'set-cmd':
            self.config = cmd_dict.get('value')
            return 0, 'value set', ''
        if prefix in ['auth get']:
            return 0, '[foo]\nkeyring = asdf\n', ''
        if prefix == 'quorum_status':
            # actual quorum status output from testing
            # note in this output all of the mons have blank crush locations
            return 0, """{"election_epoch": 14, "quorum": [0, 1, 2], "quorum_names": ["vm-00", "vm-01", "vm-02"], "quorum_leader_name": "vm-00", "quorum_age": 101, "features": {"quorum_con": "4540138322906710015", "quorum_mon": ["kraken", "luminous", "mimic", "osdmap-prune", "nautilus", "octopus", "pacific", "elector-pinging", "quincy", "reef"]}, "monmap": {"epoch": 3, "fsid": "9863e1b8-6f24-11ed-8ad8-525400c13ad2", "modified": "2022-11-28T14:00:29.972488Z", "created": "2022-11-28T13:57:55.847497Z", "min_mon_release": 18, "min_mon_release_name": "reef", "election_strategy": 1, "disallowed_leaders: ": "", "stretch_mode": false, "tiebreaker_mon": "", "features": {"persistent": ["kraken", "luminous", "mimic", "osdmap-prune", "nautilus", "octopus", "pacific", "elector-pinging", "quincy", "reef"], "optional": []}, "mons": [{"rank": 0, "name": "vm-00", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.61:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.61:6789", "nonce": 0}]}, "addr": "192.168.122.61:6789/0", "public_addr": "192.168.122.61:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}, {"rank": 1, "name": "vm-01", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.63:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.63:6789", "nonce": 0}]}, "addr": "192.168.122.63:6789/0", "public_addr": "192.168.122.63:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}, {"rank": 2, "name": "vm-02", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.82:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.82:6789", "nonce": 0}]}, "addr": "192.168.122.82:6789/0", "public_addr": "192.168.122.82:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}]}}""", ''
        if prefix == 'mon set_location':
            self.set_mon_crush_locations[cmd_dict.get('name')] = cmd_dict.get('args')
            return 0, '', ''
        return -1, '', 'error'

    def get_minimal_ceph_conf(self) -> str:
        return ''

    def get_mgr_ip(self) -> str:
        return '1.2.3.4'


class TestNVMEOFService:

    mgr = FakeMgr()
    nvmeof_service = NvmeofService(mgr)

    nvmeof_spec = NvmeofServiceSpec(service_type='nvmeof', service_id="a")
    nvmeof_spec.daemon_type = 'nvmeof'
    nvmeof_spec.daemon_id = "a"
    nvmeof_spec.spec = MagicMock()
    nvmeof_spec.spec.daemon_type = 'nvmeof'

    mgr.spec_store = MagicMock()
    mgr.spec_store.all_specs.get.return_value = nvmeof_spec

    def test_nvmeof_client_caps(self):
        pass

    @patch('cephadm.utils.resolve_ip')
    def test_nvmeof_dashboard_config(self, mock_resolve_ip):
        pass

    @patch("cephadm.inventory.Inventory.get_addr", lambda _, __: '192.168.100.100')
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    def test_nvmeof_config(self, _get_name, _run_cephadm, cephadm_module: CephadmOrchestrator):

        pool = 'testpool'
        group = 'mygroup'
        nvmeof_daemon_id = f'{pool}.{group}.test.qwert'
        tgt_cmd_extra_args = '--cpumask=0xFF --msg-mempool-size=524288'
        default_port = 5500
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_name.return_value = nvmeof_daemon_id

        nvmeof_gateway_conf = f"""# This file is generated by cephadm.
[gateway]
name = client.nvmeof.{nvmeof_daemon_id}
group = {group}
addr = 192.168.100.100
port = {default_port}
enable_auth = False
state_update_notify = True
state_update_interval_sec = 5
break_update_interval_sec = 25
enable_spdk_discovery_controller = False
encryption_key = /encryption.key
rebalance_period_sec = 7
max_gws_in_grp = 16
max_ns_to_change_lb_grp = 8
enable_prometheus_exporter = True
prometheus_exporter_ssl = False
prometheus_port = 10008
prometheus_stats_interval = 10
prometheus_frequency_slow_down_factor = 3.0
prometheus_cycles_to_adjust_speed = 3
prometheus_startup_delay = 240
prometheus_connection_list_cache_expiration = 60
verify_nqns = True
verify_keys = True
verify_listener_ip = True
# This is a development flag, do not change it
abort_on_errors = True
# This is a development flag, do not change it
abort_on_update_error = True
# This is a development flag, do not change it
omap_file_ignore_unlock_errors = False
# This is a development flag, do not change it
omap_file_lock_on_read = True
omap_file_lock_duration = 40
omap_file_lock_retries = 30
omap_file_lock_retry_sleep_interval = 1.0
omap_file_update_reloads = 10
omap_file_update_attempts = 500
allowed_consecutive_spdk_ping_failures = 1
spdk_ping_interval_in_seconds = 2.0
ping_spdk_under_lock = False
enable_monitor_client = True
max_hosts_per_namespace = 16
max_namespaces_with_netmask = 1000
max_subsystems = 128
max_hosts = 2048
max_namespaces = 4096
max_namespaces_per_subsystem = 512
max_hosts_per_subsystem = 128
subsystem_cache_expiration = 30
force_tls = False
# This is a development flag, do not change it
max_message_length_in_mb = 4
io_stats_enabled = True

[gateway-logs]
log_level = INFO
log_files_enabled = True
log_files_rotation_enabled = True
verbose_log_messages = True
max_log_file_size_in_mb = 10
max_log_files_count = 20
max_log_directory_backups = 10
log_directory = /var/log/ceph/

[discovery]
addr = 192.168.100.100
port = 8009
# This is a development flag, do not change it
abort_on_errors = True
bind_retries_limit = 10
bind_sleep_interval = 0.5

[ceph]
pool = {pool}
config_file = /etc/ceph/ceph.conf
id = nvmeof.{nvmeof_daemon_id}

[mtls]
server_key = /server.key
client_key = /client.key
server_cert = /server.cert
client_cert = /client.cert
root_ca_cert = /root.ca.cert

[kmip]
cert_dir = ./certs/kmip/{{server_name}}

[spdk]
tgt_path = /usr/local/bin/nvmf_tgt
rpc_socket_dir = /var/tmp/
rpc_socket_name = spdk.sock
timeout = 60.0
cluster_connections = 32
protocol_log_level = WARNING
conn_retries = 10
transports = tcp
transport_tcp_options = {{"in_capsule_data_size": 8192, "max_io_qpairs_per_ctrlr": 7}}
enable_dsa_acceleration = False
rbd_with_crc32c = True
tgt_cmd_extra_args = {tgt_cmd_extra_args}
qos_timeslice_in_usecs = 0
notifications_interval = 60

[monitor]
timeout = 1.0\n"""

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, NvmeofServiceSpec(service_id=f'{pool}.{group}',
                                                                tgt_cmd_extra_args=tgt_cmd_extra_args,
                                                                group=group,
                                                                pool=pool)):
                _run_cephadm.assert_called_with(
                    'test',
                    f'nvmeof.{nvmeof_daemon_id}',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": "nvmeof.testpool.mygroup.test.qwert",
                        "image": "",
                        "deploy_arguments": [],
                        "params": {
                            "tcp_ports": [5500, 4420, 8009, 10008]
                        },
                        "meta": {
                            "service_name": "nvmeof.testpool.mygroup",
                            "ports": [5500, 4420, 8009, 10008],
                            "ip": None,
                            "deployed_by": [],
                            "rank": None,
                            "rank_generation": None,
                            "extra_container_args": None,
                            "extra_entrypoint_args": None
                        },
                        "config_blobs": {
                            "config": "",
                            "keyring": "[client.nvmeof.testpool.mygroup.test.qwert]\nkey = None\n",
                            "files": {
                                "ceph-nvmeof.conf": nvmeof_gateway_conf
                            }
                        }
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_validate_no_group_duplicate_on_apply(self, cephadm_module: CephadmOrchestrator):
        nvmeof_spec_group1 = NvmeofServiceSpec(
            service_id='testpool.testgroup',
            group='testgroup',
            pool='testpool'
        )
        nvmeof_spec_also_group1 = NvmeofServiceSpec(
            service_id='testpool2.testgroup',
            group='testgroup',
            pool='testpool2'
        )
        with with_host(cephadm_module, 'test'):
            out = cephadm_module._apply_service_spec(nvmeof_spec_group1)
            assert out == 'Scheduled nvmeof.testpool.testgroup update...'
            nvmeof_specs = cephadm_module.spec_store.get_by_service_type('nvmeof')
            assert len(nvmeof_specs) == 1
            assert nvmeof_specs[0].spec.service_name() == 'nvmeof.testpool.testgroup'
            with pytest.raises(
                OrchestratorError,
                match='Cannot create nvmeof service with group testgroup. That group is already '
                      'being used by the service nvmeof.testpool.testgroup'
            ):
                cephadm_module._apply_service_spec(nvmeof_spec_also_group1)
            assert len(cephadm_module.spec_store.get_by_service_type('nvmeof')) == 1

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_validate_service_id_matches_group_on_apply(self, cephadm_module: CephadmOrchestrator):
        matching_nvmeof_spec_group_service_id = NvmeofServiceSpec(
            service_id='pool1.right_group',
            group='right_group',
            pool='pool1'
        )
        mismatch_nvmeof_spec_group_service_id = NvmeofServiceSpec(
            service_id='pool2.wrong_group',
            group='right_group',
            pool='pool2'
        )
        matching_nvmeof_spec_group_service_id_with_dot = NvmeofServiceSpec(
            service_id='pool3.right.group',
            group='right.group',
            pool='pool3'
        )
        mismatch_nvmeof_spec_group_service_id_with_dot = NvmeofServiceSpec(
            service_id='pool4.wrong.group',
            group='right.group',
            pool='pool4'
        )
        with with_host(cephadm_module, 'test'):
            cephadm_module._apply_service_spec(matching_nvmeof_spec_group_service_id)
            with pytest.raises(
                OrchestratorError,
                match='The \'nvmeof\' service id/name must end with \'.<nvmeof-group-name>\'. Found '
                      'group name \'right_group\' and service id \'pool2.wrong_group\''
            ):
                cephadm_module._apply_service_spec(mismatch_nvmeof_spec_group_service_id)
            cephadm_module._apply_service_spec(matching_nvmeof_spec_group_service_id_with_dot)
            with pytest.raises(
                OrchestratorError,
                match='The \'nvmeof\' service id/name must end with \'.<nvmeof-group-name>\'. Found '
                      'group name \'right.group\' and service id \'pool4.wrong.group\''
            ):
                cephadm_module._apply_service_spec(mismatch_nvmeof_spec_group_service_id_with_dot)

    def test_nvmeof_service_spec_defaults_pool_to_metadata_pool(self):
        spec = NvmeofServiceSpec(service_id='pool4.bla.group', group='group')

        assert spec.pool == ".nvmeof"

    @patch("cephadm.inventory.Inventory.get_addr", lambda _, __: '192.168.100.100')
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials('mycert', 'mykey'),
           )
    @patch("cephadm.services.nvmeof.NvmeofService.get_self_signed_certificates_with_label",
           lambda instance, spec, daemon_spec, label: TLSCredentials('client_cert', 'client_key', 'nvmeof_root_ca'),
           )
    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    def test_nvmeof_config_when_ssl_and_auth_enabled(
        self,
        _get_name,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
    ):
        """
        When ssl=True and enable_auth=True are set on NvmeofServiceSpec, the generated
        ceph-nvmeof.conf must have enable_auth = True, and config_blobs.files must
        include server_cert/server_key as well as client_cert/client_key/root_ca_cert.

        This ensures mTLS wiring (configure_tls) is correct.
        """

        pool = 'testpool'
        group = 'mygroup'
        nvmeof_daemon_id = f'{pool}.{group}.test.qwert'
        tgt_cmd_extra_args = '--cpumask=0xFF --msg-mempool-size=524288'
        default_port = 5500

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_name.return_value = nvmeof_daemon_id

        # Expected config when ssl=True and enable_auth=True.
        nvmeof_gateway_conf_mtls = f"""# This file is generated by cephadm.
[gateway]
name = client.nvmeof.{nvmeof_daemon_id}
group = {group}
addr = 192.168.100.100
port = {default_port}
enable_auth = True
state_update_notify = True
state_update_interval_sec = 5
break_update_interval_sec = 25
enable_spdk_discovery_controller = False
encryption_key = /encryption.key
rebalance_period_sec = 7
max_gws_in_grp = 16
max_ns_to_change_lb_grp = 8
enable_prometheus_exporter = True
prometheus_exporter_ssl = False
prometheus_port = 10008
prometheus_stats_interval = 10
prometheus_frequency_slow_down_factor = 3.0
prometheus_cycles_to_adjust_speed = 3
prometheus_startup_delay = 240
prometheus_connection_list_cache_expiration = 60
verify_nqns = True
verify_keys = True
verify_listener_ip = True
# This is a development flag, do not change it
abort_on_errors = True
# This is a development flag, do not change it
abort_on_update_error = True
# This is a development flag, do not change it
omap_file_ignore_unlock_errors = False
# This is a development flag, do not change it
omap_file_lock_on_read = True
omap_file_lock_duration = 40
omap_file_lock_retries = 30
omap_file_lock_retry_sleep_interval = 1.0
omap_file_update_reloads = 10
omap_file_update_attempts = 500
allowed_consecutive_spdk_ping_failures = 1
spdk_ping_interval_in_seconds = 2.0
ping_spdk_under_lock = False
enable_monitor_client = True
max_hosts_per_namespace = 16
max_namespaces_with_netmask = 1000
max_subsystems = 128
max_hosts = 2048
max_namespaces = 4096
max_namespaces_per_subsystem = 512
max_hosts_per_subsystem = 128
subsystem_cache_expiration = 30
force_tls = False
# This is a development flag, do not change it
max_message_length_in_mb = 4
io_stats_enabled = True

[gateway-logs]
log_level = INFO
log_files_enabled = True
log_files_rotation_enabled = True
verbose_log_messages = True
max_log_file_size_in_mb = 10
max_log_files_count = 20
max_log_directory_backups = 10
log_directory = /var/log/ceph/

[discovery]
addr = 192.168.100.100
port = 8009
# This is a development flag, do not change it
abort_on_errors = True
bind_retries_limit = 10
bind_sleep_interval = 0.5

[ceph]
pool = {pool}
config_file = /etc/ceph/ceph.conf
id = nvmeof.{nvmeof_daemon_id}

[mtls]
server_key = /server.key
client_key = /client.key
server_cert = /server.cert
client_cert = /client.cert
root_ca_cert = /root.ca.cert

[kmip]
cert_dir = ./certs/kmip/{{server_name}}

[spdk]
tgt_path = /usr/local/bin/nvmf_tgt
rpc_socket_dir = /var/tmp/
rpc_socket_name = spdk.sock
timeout = 60.0
cluster_connections = 32
protocol_log_level = WARNING
conn_retries = 10
transports = tcp
transport_tcp_options = {{"in_capsule_data_size": 8192, "max_io_qpairs_per_ctrlr": 7}}
enable_dsa_acceleration = False
rbd_with_crc32c = True
tgt_cmd_extra_args = {tgt_cmd_extra_args}
qos_timeslice_in_usecs = 0
notifications_interval = 60

[monitor]
timeout = 1.0
"""

        with with_host(cephadm_module, 'test'):
            with with_service(
                cephadm_module,
                NvmeofServiceSpec(
                    service_id=f'{pool}.{group}',
                    tgt_cmd_extra_args=tgt_cmd_extra_args,
                    group=group,
                    pool=pool,
                    ssl=True,
                    enable_auth=True
                ),
            ):
                _run_cephadm.assert_called_with(
                    'test',
                    f'nvmeof.{nvmeof_daemon_id}',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": f"nvmeof.{nvmeof_daemon_id}",
                        "image": "",
                        "deploy_arguments": [],
                        "params": {
                            "tcp_ports": [5500, 4420, 8009, 10008],
                        },
                        "meta": {
                            "service_name": "nvmeof.testpool.mygroup",
                            "ports": [5500, 4420, 8009, 10008],
                            "ip": None,
                            "deployed_by": [],
                            "rank": None,
                            "rank_generation": None,
                            "extra_container_args": None,
                            "extra_entrypoint_args": None,
                        },
                        "config_blobs": {
                            "config": "",
                            "keyring": "[client.nvmeof.testpool.mygroup.test.qwert]\nkey = None\n",
                            "files": {
                                # server-side TLS
                                "server_cert": "mycert",
                                "server_key": "mykey",
                                # mTLS client-side pieces from get_self_signed_certificates_with_label
                                "client_cert": "client_cert",
                                "client_key": "client_key",
                                "root_ca_cert": "nvmeof_root_ca",
                                "ceph-nvmeof.conf": nvmeof_gateway_conf_mtls,
                            },
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.inventory.Inventory.get_addr", lambda _, __: '192.168.100.100')
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.cert_mgr.CertMgr.get_root_ca", lambda instance: 'nvmeof_root_cert')
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials('mycert', 'mykey'),
           )
    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    def test_nvmeof_config_when_ssl_enabled(
        self,
        _get_name,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
    ):
        """
        When ssl=True is set on NvmeofServiceSpec, the generated ceph-nvmeof.conf
        must enable TLS, and the generated certificate files must be passed in config_blobs.
        """

        pool = 'testpool'
        group = 'mygroup'
        nvmeof_daemon_id = f'{pool}.{group}.test.qwert'
        tgt_cmd_extra_args = '--cpumask=0xFF --msg-mempool-size=524288'
        default_port = 5500

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_name.return_value = nvmeof_daemon_id

        # Expected config when ssl=True:
        # note: enable_auth stays False on purpose in this case
        nvmeof_gateway_conf_ssl = f"""# This file is generated by cephadm.
[gateway]
name = client.nvmeof.{nvmeof_daemon_id}
group = {group}
addr = 192.168.100.100
port = {default_port}
enable_auth = False
state_update_notify = True
state_update_interval_sec = 5
break_update_interval_sec = 25
enable_spdk_discovery_controller = False
encryption_key = /encryption.key
rebalance_period_sec = 7
max_gws_in_grp = 16
max_ns_to_change_lb_grp = 8
enable_prometheus_exporter = True
prometheus_exporter_ssl = False
prometheus_port = 10008
prometheus_stats_interval = 10
prometheus_frequency_slow_down_factor = 3.0
prometheus_cycles_to_adjust_speed = 3
prometheus_startup_delay = 240
prometheus_connection_list_cache_expiration = 60
verify_nqns = True
verify_keys = True
verify_listener_ip = True
# This is a development flag, do not change it
abort_on_errors = True
# This is a development flag, do not change it
abort_on_update_error = True
# This is a development flag, do not change it
omap_file_ignore_unlock_errors = False
# This is a development flag, do not change it
omap_file_lock_on_read = True
omap_file_lock_duration = 40
omap_file_lock_retries = 30
omap_file_lock_retry_sleep_interval = 1.0
omap_file_update_reloads = 10
omap_file_update_attempts = 500
allowed_consecutive_spdk_ping_failures = 1
spdk_ping_interval_in_seconds = 2.0
ping_spdk_under_lock = False
enable_monitor_client = True
max_hosts_per_namespace = 16
max_namespaces_with_netmask = 1000
max_subsystems = 128
max_hosts = 2048
max_namespaces = 4096
max_namespaces_per_subsystem = 512
max_hosts_per_subsystem = 128
subsystem_cache_expiration = 30
force_tls = False
# This is a development flag, do not change it
max_message_length_in_mb = 4
io_stats_enabled = True

[gateway-logs]
log_level = INFO
log_files_enabled = True
log_files_rotation_enabled = True
verbose_log_messages = True
max_log_file_size_in_mb = 10
max_log_files_count = 20
max_log_directory_backups = 10
log_directory = /var/log/ceph/

[discovery]
addr = 192.168.100.100
port = 8009
# This is a development flag, do not change it
abort_on_errors = True
bind_retries_limit = 10
bind_sleep_interval = 0.5

[ceph]
pool = {pool}
config_file = /etc/ceph/ceph.conf
id = nvmeof.{nvmeof_daemon_id}

[mtls]
server_key = /server.key
client_key = /client.key
server_cert = /server.cert
client_cert = /client.cert
root_ca_cert = /root.ca.cert

[kmip]
cert_dir = ./certs/kmip/{{server_name}}

[spdk]
tgt_path = /usr/local/bin/nvmf_tgt
rpc_socket_dir = /var/tmp/
rpc_socket_name = spdk.sock
timeout = 60.0
cluster_connections = 32
protocol_log_level = WARNING
conn_retries = 10
transports = tcp
transport_tcp_options = {{"in_capsule_data_size": 8192, "max_io_qpairs_per_ctrlr": 7}}
enable_dsa_acceleration = False
rbd_with_crc32c = True
tgt_cmd_extra_args = {tgt_cmd_extra_args}
qos_timeslice_in_usecs = 0
notifications_interval = 60

[monitor]
timeout = 1.0
"""

        with with_host(cephadm_module, 'test'):
            with with_service(
                cephadm_module,
                NvmeofServiceSpec(
                    service_id=f'{pool}.{group}',
                    tgt_cmd_extra_args=tgt_cmd_extra_args,
                    group=group,
                    pool=pool,
                    ssl=True,
                ),
            ):
                _run_cephadm.assert_called_with(
                    'test',
                    f'nvmeof.{nvmeof_daemon_id}',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": f"nvmeof.{nvmeof_daemon_id}",
                        "image": "",
                        "deploy_arguments": [],
                        "params": {
                            "tcp_ports": [5500, 4420, 8009, 10008],
                        },
                        "meta": {
                            "service_name": "nvmeof.testpool.mygroup",
                            "ports": [5500, 4420, 8009, 10008],
                            "ip": None,
                            "deployed_by": [],
                            "rank": None,
                            "rank_generation": None,
                            "extra_container_args": None,
                            "extra_entrypoint_args": None,
                        },
                        "config_blobs": {
                            "config": "",
                            "keyring": "[client.nvmeof.testpool.mygroup.test.qwert]\nkey = None\n",
                            "files": {
                                # certs come from the mocked get_certificates()
                                "server_cert": "mycert",
                                "server_key": "mykey",
                                "ceph-nvmeof.conf": nvmeof_gateway_conf_ssl,
                            },
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )


class TestNvmeofTLSBundle:
    def _store_spec(self, cephadm_module: CephadmOrchestrator, spec: NvmeofServiceSpec) -> None:
        # SpecStore in unit tests stores ServiceSpec objects directly.
        cephadm_module.spec_store._specs[spec.service_name()] = spec
        # Some SpecStore helpers expect spec_created to exist.
        if hasattr(cephadm_module.spec_store, 'spec_created'):
            cephadm_module.spec_store.spec_created[spec.service_name()] = datetime_now()

    def _daemon_name(self, spec: NvmeofServiceSpec) -> str:
        return f'{spec.service_name()}.test-daemon'

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_ssl_disabled(self, cephadm_module: CephadmOrchestrator):
        """Test that SSL disabled returns empty bundle"""
        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=False,
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        bundle = nvmeof_service.get_nvmeof_tls_bundle(
            spec.service_name(), self._daemon_name(spec)
        )

        assert bundle is not None
        assert bundle.server_cert == ''
        assert bundle.server_key == ''
        assert bundle.client_cert == ''
        assert bundle.client_key == ''
        assert bundle.ca_cert == ''

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_inline_server_only(self, cephadm_module: CephadmOrchestrator):
        """Test INLINE certificate source with server creds only (no mTLS)"""
        server_cert = '-----BEGIN CERTIFICATE-----\nSERVER_CERT\n-----END CERTIFICATE-----'
        server_key = '-----BEGIN PRIVATE KEY-----\nSERVER_KEY\n-----END PRIVATE KEY-----'

        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='inline',
            ssl_cert=server_cert,
            ssl_key=server_key,
            enable_auth=False,
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        bundle = nvmeof_service.get_nvmeof_tls_bundle(
            spec.service_name(), self._daemon_name(spec)
        )

        assert bundle is not None
        assert bundle.server_cert == server_cert
        assert bundle.server_key == server_key
        assert bundle.client_cert == ''
        assert bundle.client_key == ''
        assert bundle.ca_cert == ''

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_inline_with_mtls(self, cephadm_module: CephadmOrchestrator):
        """Test INLINE certificate source with mTLS enabled"""
        server_cert = '-----BEGIN CERTIFICATE-----\nSERVER_CERT\n-----END CERTIFICATE-----'
        server_key = '-----BEGIN PRIVATE KEY-----\nSERVER_KEY\n-----END PRIVATE KEY-----'
        client_cert = '-----BEGIN CERTIFICATE-----\nCLIENT_CERT\n-----END CERTIFICATE-----'
        client_key = '-----BEGIN PRIVATE KEY-----\nCLIENT_KEY\n-----END PRIVATE KEY-----'
        ca_cert = '-----BEGIN CERTIFICATE-----\nCA_CERT\n-----END CERTIFICATE-----'

        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='inline',
            ssl_cert=server_cert,
            ssl_key=server_key,
            enable_auth=True,
            client_cert=client_cert,
            client_key=client_key,
            root_ca_cert=ca_cert,
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        bundle = nvmeof_service.get_nvmeof_tls_bundle(
            spec.service_name(), self._daemon_name(spec)
        )

        assert bundle is not None
        assert bundle.server_cert == server_cert
        assert bundle.server_key == server_key
        assert bundle.client_cert == client_cert
        assert bundle.client_key == client_key
        assert bundle.ca_cert == ca_cert

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_reference_server_only(self, cephadm_module: CephadmOrchestrator):
        """Test REFERENCE certificate source with server creds only"""
        server_cert = '-----BEGIN CERTIFICATE-----\nREF_SERVER_CERT\n-----END CERTIFICATE-----'
        server_key = '-----BEGIN PRIVATE KEY-----\nREF_SERVER_KEY\n-----END PRIVATE KEY-----'

        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='reference',
            enable_auth=False,
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        daemon_name = self._daemon_name(spec)

        # Mock cert_mgr.get_cert and get_key for SERVICE-scoped lookups
        def _get_cert(name, service_name=None, host=None):
            if name == nvmeof_service.cert_name:
                return server_cert
            return None

        def _get_key(name, service_name=None, host=None):
            if name == nvmeof_service.key_name:
                return server_key
            return None

        with patch.object(cephadm_module.cert_mgr, 'get_cert', side_effect=_get_cert), \
             patch.object(cephadm_module.cert_mgr, 'get_key', side_effect=_get_key):
            bundle = nvmeof_service.get_nvmeof_tls_bundle(spec.service_name(), daemon_name)

        assert bundle is not None
        assert bundle.server_cert == server_cert
        assert bundle.server_key == server_key
        assert bundle.client_cert == ''
        assert bundle.client_key == ''
        assert bundle.ca_cert == ''

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_reference_with_mtls(self, cephadm_module: CephadmOrchestrator):
        """Test REFERENCE certificate source with mTLS enabled"""
        server_cert = '-----BEGIN CERTIFICATE-----\nREF_SERVER_CERT\n-----END CERTIFICATE-----'
        server_key = '-----BEGIN PRIVATE KEY-----\nREF_SERVER_KEY\n-----END PRIVATE KEY-----'
        client_cert = '-----BEGIN CERTIFICATE-----\nREF_CLIENT_CERT\n-----END CERTIFICATE-----'
        client_key = '-----BEGIN PRIVATE KEY-----\nREF_CLIENT_KEY\n-----END PRIVATE KEY-----'
        ca_cert = '-----BEGIN CERTIFICATE-----\nREF_CA_CERT\n-----END CERTIFICATE-----'

        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='reference',
            enable_auth=True,
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        daemon_name = self._daemon_name(spec)

        def _get_cert(name, service_name=None, host=None):
            if name == nvmeof_service.cert_name:
                return server_cert
            if name == nvmeof_service.client_cert_name:
                return client_cert
            if name == nvmeof_service.ca_cert_name:
                return ca_cert
            return None

        def _get_key(name, service_name=None, host=None):
            if name == nvmeof_service.key_name:
                return server_key
            if name == nvmeof_service.client_key_name:
                return client_key
            return None

        with patch.object(cephadm_module.cert_mgr, 'get_cert', side_effect=_get_cert), \
             patch.object(cephadm_module.cert_mgr, 'get_key', side_effect=_get_key):
            bundle = nvmeof_service.get_nvmeof_tls_bundle(spec.service_name(), daemon_name)

        assert bundle is not None
        assert bundle.server_cert == server_cert
        assert bundle.server_key == server_key
        assert bundle.client_cert == client_cert
        assert bundle.client_key == client_key
        assert bundle.ca_cert == ca_cert

    @patch("cephadm.services.nvmeof.NvmeofService._get_daemon_hostname")
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_cephadm_signed_server_only(self, _get_daemon_hostname, cephadm_module: CephadmOrchestrator):
        """Test CEPHADM_SIGNED certificate source without mTLS"""
        _get_daemon_hostname.return_value = 'test-host'

        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='cephadm-signed',
            enable_auth=False,
        )
        self._store_spec(cephadm_module, spec)

        server_creds = TLSCredentials(
            cert=ceph_generated_cert,
            key=ceph_generated_key,
            ca_cert=cephadm_root_ca,
        )

        def _get_self_signed(service_name, hostname, label=None):
            assert label is None
            return server_creds

        nvmeof_service = NvmeofService(cephadm_module)
        daemon_name = self._daemon_name(spec)
        with patch.object(
            cephadm_module.cert_mgr,
            'get_self_signed_tls_credentials',
            side_effect=_get_self_signed,
        ):
            bundle = nvmeof_service.get_nvmeof_tls_bundle(spec.service_name(), daemon_name)

        assert bundle is not None
        assert bundle.server_cert == ceph_generated_cert
        assert bundle.server_key == ceph_generated_key
        assert bundle.ca_cert == cephadm_root_ca
        assert bundle.client_cert == ''
        assert bundle.client_key == ''

    @patch("cephadm.services.nvmeof.NvmeofService._get_daemon_hostname")
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_cephadm_signed_with_mtls(self, _get_daemon_hostname, cephadm_module: CephadmOrchestrator):
        """Test CEPHADM_SIGNED certificate source with mTLS enabled"""
        _get_daemon_hostname.return_value = 'test-host'

        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='cephadm-signed',
            enable_auth=True,
        )
        self._store_spec(cephadm_module, spec)

        server_creds = TLSCredentials(
            cert=ceph_generated_cert,
            key=ceph_generated_key,
            ca_cert=cephadm_root_ca,
        )
        client_creds = TLSCredentials(
            cert='-----BEGIN CERTIFICATE-----\nCLIENT_CERT\n-----END CERTIFICATE-----',
            key='-----BEGIN PRIVATE KEY-----\nCLIENT_KEY\n-----END PRIVATE KEY-----',
            ca_cert=cephadm_root_ca,
        )

        def _get_self_signed(service_name, hostname, label=None):
            if label == NVMEOF_CLIENT_CERT_LABEL:
                return client_creds
            assert label is None
            return server_creds

        nvmeof_service = NvmeofService(cephadm_module)
        daemon_name = self._daemon_name(spec)
        with patch.object(
            cephadm_module.cert_mgr,
            'get_self_signed_tls_credentials',
            side_effect=_get_self_signed,
        ):
            bundle = nvmeof_service.get_nvmeof_tls_bundle(spec.service_name(), daemon_name)

        assert bundle is not None
        assert bundle.server_cert == ceph_generated_cert
        assert bundle.server_key == ceph_generated_key
        assert bundle.client_cert == client_creds.cert
        assert bundle.client_key == client_creds.key
        # API returns the CA from server_creds
        assert bundle.ca_cert == cephadm_root_ca

    @patch("cephadm.services.nvmeof.NvmeofService._get_daemon_hostname")
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_cephadm_signed_no_hostname(self, _get_daemon_hostname, cephadm_module: CephadmOrchestrator):
        """Test CEPHADM_SIGNED returns None when no hostname can be resolved"""
        _get_daemon_hostname.return_value = None

        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='cephadm-signed',
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        bundle = nvmeof_service.get_nvmeof_tls_bundle(
            spec.service_name(), self._daemon_name(spec)
        )
        assert bundle is None

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_service_not_found(self, cephadm_module: CephadmOrchestrator):
        """Test that None is returned when service doesn't exist"""
        nvmeof_service = NvmeofService(cephadm_module)
        bundle = nvmeof_service.get_nvmeof_tls_bundle(
            'nvmeof.nonexistent', 'nvmeof.nonexistent.test-daemon'
        )
        assert bundle is None

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_unknown_cert_source(self, cephadm_module: CephadmOrchestrator):
        """Test that None is returned for unknown certificate_source"""
        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='unknown-source',
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        bundle = nvmeof_service.get_nvmeof_tls_bundle(
            spec.service_name(), self._daemon_name(spec)
        )
        assert bundle is None

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_get_nvmeof_tls_bundle_reference_missing_certs(self, cephadm_module: CephadmOrchestrator):
        """Test REFERENCE source when certs are not stored in certmgr"""
        spec = NvmeofServiceSpec(
            service_id='test.group',
            pool='testpool',
            group='group',
            ssl=True,
            certificate_source='reference',
            enable_auth=True,
        )
        self._store_spec(cephadm_module, spec)

        nvmeof_service = NvmeofService(cephadm_module)
        daemon_name = self._daemon_name(spec)

        # Mock get_cert and get_key to return None for all lookups
        with patch.object(cephadm_module.cert_mgr, 'get_cert', return_value=None), \
             patch.object(cephadm_module.cert_mgr, 'get_key', return_value=None):
            bundle = nvmeof_service.get_nvmeof_tls_bundle(spec.service_name(), daemon_name)

        assert bundle is not None
        assert bundle.server_cert == ''
        assert bundle.server_key == ''
        assert bundle.client_cert == ''
        assert bundle.client_key == ''
        assert bundle.ca_cert == ''
