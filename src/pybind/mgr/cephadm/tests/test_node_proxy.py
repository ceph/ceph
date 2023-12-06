import cherrypy
import json
from _pytest.monkeypatch import MonkeyPatch
from cherrypy.test import helper
from cephadm.agent import NodeProxy
from unittest.mock import MagicMock, call, patch
from cephadm.inventory import AgentCache, NodeProxyCache, Inventory
from cephadm.ssl_cert_utils import SSLCerts
from . import node_proxy_data

PORT = 58585


class FakeMgr:
    def __init__(self) -> None:
        self.log = MagicMock()
        self.get_store = MagicMock(return_value=json.dumps(node_proxy_data.mgr_inventory_cache))
        self.set_store = MagicMock()
        self.set_health_warning = MagicMock()
        self.remove_health_warning = MagicMock()
        self.inventory = Inventory(self)
        self.agent_cache = AgentCache(self)
        self.agent_cache.agent_ports = {"host01": 1234}
        self.node_proxy = NodeProxyCache(self)
        self.node_proxy.save = MagicMock()
        self.http_server = MagicMock()
        self.http_server.agent = MagicMock()
        self.http_server.agent.ssl_certs = SSLCerts()
        self.http_server.agent.ssl_certs.generate_root_cert(self.get_mgr_ip())

    def get_mgr_ip(self) -> str:
        return '0.0.0.0'


class TestNodeProxy(helper.CPWebCase):
    mgr = FakeMgr()
    app = NodeProxy(mgr)
    mgr.agent_cache.agent_keys = {"host01": "fake-secret01",
                                  "host02": "fake-secret02"}
    mgr.node_proxy.oob = {"host01": {"username": "oob-user01",
                                     "password": "oob-pass01"},
                          "host02": {"username": "oob-user02",
                                     "password": "oob-pass02"}}
    mgr.node_proxy.data = node_proxy_data.full_set

    @classmethod
    def setup_server(cls):
        # cherrypy.tree.mount(NodeProxy(TestNodeProxy.mgr))
        cherrypy.tree.mount(TestNodeProxy.app)
        cherrypy.config.update({'global': {
            'server.socket_host': '127.0.0.1',
            'server.socket_port': PORT}})

    def setUp(self):
        self.PORT = PORT
        self.monkeypatch = MonkeyPatch()

    def test_oob_data_misses_cephx_field(self):
        data = '{}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_oob_data_misses_name_field(self):
        data = '{"cephx": {"secret": "fake-secret"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_oob_data_misses_secret_field(self):
        data = '{"cephx": {"name": "host01"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_oob_agent_not_running(self):
        data = '{"cephx": {"name": "host03", "secret": "fake-secret03"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('502 Bad Gateway')

    def test_oob_wrong_keyring(self):
        data = '{"cephx": {"name": "host01", "secret": "wrong-keyring"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('403 Forbidden')

    def test_oob_ok(self):
        data = '{"cephx": {"name": "host01", "secret": "fake-secret01"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('200 OK')

    def test_data_missing_patch(self):
        data = '{"cephx": {"name": "host01", "secret": "fake-secret01"}}'
        self.getPage("/data", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                 ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_data_raises_alert(self):
        patch = node_proxy_data.full_set_with_critical
        data = {"cephx": {"name": "host01", "secret": "fake-secret01"}, "patch": patch}
        data_str = json.dumps(data)
        self.getPage("/data", method="POST", body=data_str, headers=[('Content-Type', 'application/json'),
                                                                     ('Content-Length', str(len(data_str)))])
        self.assertStatus('200 OK')

        calls = [call('HARDWARE_STORAGE',
                      count=2,
                      detail=['disk.bay.0:enclosure.internal.0-1:raid.integrated.1-1 is critical: Enabled',
                              'disk.bay.9:enclosure.internal.0-1 is critical: Enabled'],
                      summary='2 storage members are not ok'),
                 call('HARDWARE_MEMORY',
                      count=1,
                      detail=['dimm.socket.a1 is critical: Enabled'],
                      summary='1 memory member is not ok')]

        assert TestNodeProxy.mgr.set_health_warning.mock_calls == calls

    # @pytest.mark.parametrize("method", ["GET", "PATCH"])
    # def test_led_no_hostname(self, method):
    #     self.getPage("/led", method=method)
    #     self.assertStatus('501 Not Implemented')

    def test_led_GET_no_hostname(self):
        self.getPage("/led", method="GET")
        self.assertStatus('501 Not Implemented')

    def test_led_PATCH_no_hostname(self):
        data = "{}"
        self.getPage("/led", method="PATCH", body=data, headers=[('Content-Type', 'application/json'),
                                                                 ('Content-Length', str(len(data)))])
        self.assertStatus('501 Not Implemented')

    @patch('cephadm.agent.AgentMessageThread.join', return_value=MagicMock)
    @patch('cephadm.agent.AgentMessageThread.start', return_value=MagicMock)
    def test_set_led_no_type(self, m_agent_msg_thread_start, m_agent_msg_thread_join):
        data = '{"IndicatorLED": "Blinking"}'
        self.getPage("/host01/led", method="PATCH", body=data, headers=[('Content-Type', 'application/json'),
                                                                        ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    @patch('cephadm.agent.AgentMessageThread.join', return_value=MagicMock)
    @patch('cephadm.agent.AgentMessageThread.start', return_value=MagicMock)
    def test_set_chassis_led(self, m_agent_msg_thread_start, m_agent_msg_thread_join):
        data = '{"IndicatorLED": "Blinking"}'
        with patch('cephadm.agent.AgentMessageThread.get_agent_response') as a:
            a.return_value = '{"http_code": 200}'
            self.getPage("/host01/led/chassis", method="PATCH", body=data, headers=[('Content-Type', 'application/json'),
                                                                                    ('Content-Length', str(len(data)))])
            self.assertStatus('200 OK')

    def test_get_led_missing_type(self):
        self.getPage("/host01/led", method="GET")
        self.assertStatus('400 Bad Request')

    def test_get_led_no_hostname(self):
        self.getPage("/led", method="GET")
        self.assertStatus('501 Not Implemented')

    def test_get_led_type_chassis_no_hostname(self):
        self.getPage("/led/chassis", method="GET")
        self.assertStatus('404 Not Found')

    def test_get_led_type_drive_no_hostname(self):
        self.getPage("/led/chassis", method="GET")
        self.assertStatus('404 Not Found')

    def test_get_led_type_drive_missing_id(self):
        self.getPage("/host01/led/drive", method="GET")
        self.assertStatus('400 Bad Request')

    def test_get_led_type_chassis_answer_invalid_json(self):
        self.getPage("/host01/led/chassis", method="GET")
        self.assertStatus('503 Service Unavailable')

    @patch('cephadm.agent.AgentMessageThread.join', return_value=MagicMock)
    @patch('cephadm.agent.AgentMessageThread.start', return_value=MagicMock)
    def test_get_led_type_chassis_answer_no_http_code(self, m_agent_msg_thread_start, m_agent_msg_thread_join):
        with patch('cephadm.agent.AgentMessageThread.get_agent_response') as a:
            a.return_value = '{"foo": "bar"}'
            self.getPage("/host01/led/chassis", method="GET")
            self.assertStatus('503 Service Unavailable')

    def test_get_led_status_not_200(self):
        self.getPage("/host01/led/chassis", method="GET")
        self.assertStatus('503 Service Unavailable')

    def test_get_led_key_error(self):
        self.getPage("/host02/led/chassis", method="GET")
        self.assertStatus('502 Bad Gateway')

    @patch('cephadm.agent.AgentMessageThread.join', return_value=MagicMock)
    @patch('cephadm.agent.AgentMessageThread.start', return_value=MagicMock)
    def test_get_chassis_led_ok(self, m_agent_msg_thread_start, m_agent_msg_thread_join):
        with patch('cephadm.agent.AgentMessageThread.get_agent_response') as a:
            a.return_value = '{"http_code": 200}'
            self.getPage("/host01/led/chassis", method="GET")
            self.assertStatus('200 OK')

    @patch('cephadm.agent.AgentMessageThread.join', return_value=MagicMock)
    @patch('cephadm.agent.AgentMessageThread.start', return_value=MagicMock)
    def test_get_drive_led_without_id(self, m_agent_msg_thread_start, m_agent_msg_thread_join):
        self.getPage("/host01/led/drive", method="GET")
        self.assertStatus('400 Bad Request')

    @patch('cephadm.agent.AgentMessageThread.join', return_value=MagicMock)
    @patch('cephadm.agent.AgentMessageThread.start', return_value=MagicMock)
    def test_get_drive_led_with_id(self, m_agent_msg_thread_start, m_agent_msg_thread_join):
        with patch('cephadm.agent.AgentMessageThread.get_agent_response') as a:
            a.return_value = '{"http_code": 200}'
            self.getPage("/host01/led/drive/123", method="GET")
            self.assertStatus('200 OK')

    # def test_led_endpoint_unreachable(self):
    #     TestNodeProxy.app.query_endpoint = MagicMock(side_effect=URLError("fake-error"))
    #     self.getPage("/host02/led", method="GET")
    #     calls = [call(addr='10.10.10.12',
    #                   data=None,
    #                   endpoint='/led',
    #                   headers={},
    #                   method='GET',
    #                   port=8080,
    #                   ssl_ctx=TestNodeProxy.app.ssl_ctx)]
    #     self.assertStatus('502 Bad Gateway')
    #     assert TestNodeProxy.app.query_endpoint.mock_calls == calls

    def test_fullreport_with_valid_hostname(self):
        self.getPage("/host02/fullreport", method="GET")
        self.assertStatus('200 OK')

    def test_fullreport_no_hostname(self):
        self.getPage("/fullreport", method="GET")
        self.assertStatus('200 OK')

    def test_fullreport_with_invalid_hostname(self):
        self.getPage("/host03/fullreport", method="GET")
        self.assertStatus('404 Not Found')

    def test_summary_with_valid_hostname(self):
        self.getPage("/host02/summary", method="GET")
        self.assertStatus('200 OK')

    def test_summary_no_hostname(self):
        self.getPage("/summary", method="GET")
        self.assertStatus('200 OK')

    def test_summary_with_invalid_hostname(self):
        self.getPage("/host03/summary", method="GET")
        self.assertStatus('404 Not Found')

    def test_criticals_with_valid_hostname(self):
        self.getPage("/host02/criticals", method="GET")
        self.assertStatus('200 OK')

    def test_criticals_no_hostname(self):
        self.getPage("/criticals", method="GET")
        self.assertStatus('200 OK')

    def test_criticals_with_invalid_hostname(self):
        self.getPage("/host03/criticals", method="GET")
        self.assertStatus('404 Not Found')

    def test_memory_with_valid_hostname(self):
        self.getPage("/host02/memory", method="GET")
        self.assertStatus('200 OK')

    def test_memory_no_hostname(self):
        self.getPage("/memory", method="GET")
        self.assertStatus('200 OK')

    def test_memory_with_invalid_hostname(self):
        self.getPage("/host03/memory", method="GET")
        self.assertStatus('404 Not Found')

    def test_network_with_valid_hostname(self):
        self.getPage("/host02/network", method="GET")
        self.assertStatus('200 OK')

    def test_network_no_hostname(self):
        self.getPage("/network", method="GET")
        self.assertStatus('200 OK')

    def test_network_with_invalid_hostname(self):
        self.getPage("/host03/network", method="GET")
        self.assertStatus('404 Not Found')

    def test_processors_with_valid_hostname(self):
        self.getPage("/host02/processors", method="GET")
        self.assertStatus('200 OK')

    def test_processors_no_hostname(self):
        self.getPage("/processors", method="GET")
        self.assertStatus('200 OK')

    def test_processors_with_invalid_hostname(self):
        self.getPage("/host03/processors", method="GET")
        self.assertStatus('404 Not Found')

    def test_storage_with_valid_hostname(self):
        self.getPage("/host02/storage", method="GET")
        self.assertStatus('200 OK')

    def test_storage_no_hostname(self):
        self.getPage("/storage", method="GET")
        self.assertStatus('200 OK')

    def test_storage_with_invalid_hostname(self):
        self.getPage("/host03/storage", method="GET")
        self.assertStatus('404 Not Found')

    def test_power_with_valid_hostname(self):
        self.getPage("/host02/power", method="GET")
        self.assertStatus('200 OK')

    def test_power_no_hostname(self):
        self.getPage("/power", method="GET")
        self.assertStatus('200 OK')

    def test_power_with_invalid_hostname(self):
        self.getPage("/host03/power", method="GET")
        self.assertStatus('404 Not Found')

    def test_fans_with_valid_hostname(self):
        self.getPage("/host02/fans", method="GET")
        self.assertStatus('200 OK')

    def test_fans_no_hostname(self):
        self.getPage("/fans", method="GET")
        self.assertStatus('200 OK')

    def test_fans_with_invalid_hostname(self):
        self.getPage("/host03/fans", method="GET")
        self.assertStatus('404 Not Found')

    def test_firmwares_with_valid_hostname(self):
        self.getPage("/host02/firmwares", method="GET")
        self.assertStatus('200 OK')

    def test_firmwares_no_hostname(self):
        self.getPage("/firmwares", method="GET")
        self.assertStatus('200 OK')

    def test_firmwares_with_invalid_hostname(self):
        self.getPage("/host03/firmwares", method="GET")
        self.assertStatus('404 Not Found')
