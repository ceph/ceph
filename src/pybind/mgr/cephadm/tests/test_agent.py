from unittest import mock

import pytest

from cephadm.agent import AgentMessageThread
from cephadm.tests.fixtures import _run_cephadm, with_host


@pytest.mark.parametrize('addr', ['10.0.0.1', '2001:db8::1'])
@mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
@mock.patch('cephadm.agent.socket.create_connection')
@mock.patch('cephadm.agent.ssl.create_default_context')
def test_agent_message_thread_connect(_create_default_context, _create_connection, addr, cephadm_module):
    with mock.patch.object(cephadm_module.cert_mgr, 'get_root_ca', return_value='root-ca'), \
            mock.patch.object(cephadm_module.cert_mgr, 'generate_cert',
                              return_value=mock.Mock(cert='cert', key='key')):
        with with_host(cephadm_module, 'host1', addr=addr):
            ssl_ctx = _create_default_context.return_value
            secure_socket = ssl_ctx.wrap_socket.return_value
            secure_socket.recv.return_value = b'ACK'

            t = AgentMessageThread('host1', 7150, {'counter': 1}, cephadm_module)
            t.run()

            _create_connection.assert_called_once_with((addr, 7150))
            ssl_ctx.wrap_socket.assert_called_once_with(
                _create_connection.return_value, server_hostname=addr)
            secure_socket.sendall.assert_called_once()
            assert t.agent_response == 'ACK'
            assert cephadm_module.agent_cache.sending_agent_message['host1'] is False
