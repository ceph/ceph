import socket
import ssl
from unittest.mock import MagicMock, call, patch

from cephadm.agent import AGENT_MESSAGE_TIMEOUT_S, AgentMessageThread
from cephadm.services.agent import CephadmAgent
from .fixtures import with_cephadm_module


def test_client_ssl_context_is_reused_across_messages():
    """The client TLS context is built once and shared by later messages."""
    with with_cephadm_module() as mgr:
        agent = mgr.http_server.agent
        first = agent.get_client_ssl_context()
        with patch.object(mgr.cert_mgr, 'generate_cert',
                          wraps=mgr.cert_mgr.generate_cert) as generate_cert:
            for _ in range(5):
                assert agent.get_client_ssl_context() is first
        generate_cert.assert_not_called()


def test_client_ssl_context_uses_stored_agent_credentials():
    """The context presents the mgr agent credentials kept by the cert manager."""
    with with_cephadm_module() as mgr:
        agent = mgr.http_server.agent
        ctx = agent.get_client_ssl_context()
        stored = mgr.cert_mgr.get_self_signed_tls_credentials(CephadmAgent.TYPE, mgr.get_hostname())
        assert stored
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is True
        assert len(ctx.get_ca_certs()) == 1


def test_client_ssl_context_is_rebuilt_when_credentials_change():
    """New mgr agent credentials produce a new client TLS context."""
    with with_cephadm_module() as mgr:
        agent = mgr.http_server.agent
        first = agent.get_client_ssl_context()
        host = mgr.get_hostname()
        new_creds = mgr.cert_mgr.generate_cert(host, mgr.get_mgr_ip())
        mgr.cert_mgr.save_self_signed_cert_key_pair(CephadmAgent.TYPE, new_creds, host=host)
        second = agent.get_client_ssl_context()
        assert second is not first
        assert agent.get_client_ssl_context() is second


def test_agent_message_uses_shared_client_context():
    """Sending a message to an agent takes the shared context and generates no certificate."""
    with with_cephadm_module() as mgr:
        host = 'test'
        agent = mgr.http_server.agent
        ctx = agent.get_client_ssl_context()
        thread = AgentMessageThread(host, 1234, {'counter': 1}, mgr)
        with patch.object(mgr.cert_mgr, 'generate_cert') as generate_cert, \
                patch.object(agent, 'get_client_ssl_context', return_value=ctx) as get_ctx, \
                patch('cephadm.agent.socket.socket', side_effect=OSError('no network in tests')):
            thread.run()
        get_ctx.assert_called_once()
        generate_cert.assert_not_called()
        assert mgr.agent_cache.sending_agent_message[host] is False


def _fake_sockets(connect_errors):
    """
    Raw and TLS socket mocks for AgentMessageThread.run(). Each connect()
    raises the next item of connect_errors; None means it succeeds.
    """
    raw, tls = [], []
    errors = list(connect_errors)

    def make_raw(*args, **kwargs):
        sock = MagicMock(name='raw')
        sock.__enter__.return_value = sock
        raw.append(sock)
        return sock

    def wrap_socket(sock, server_hostname=None):
        secure = MagicMock(name='tls')
        secure.__enter__.return_value = secure
        err = errors.pop(0)
        if err is not None:
            secure.connect.side_effect = err
        secure.recv.return_value = b'ACK'
        tls.append(secure)
        return secure

    ctx = MagicMock(name='ssl_ctx')
    ctx.wrap_socket.side_effect = wrap_socket
    return ctx, make_raw, raw, tls


def _run_message(mgr, ctx, make_raw):
    host = 'test'
    agent = mgr.http_server.agent
    thread = AgentMessageThread(host, 1234, {'counter': 1}, mgr)
    with patch.object(agent, 'get_client_ssl_context', return_value=ctx), \
            patch('cephadm.agent.socket.socket', side_effect=make_raw), \
            patch('cephadm.agent.time.sleep') as sleep:
        thread.run()
    return thread, sleep


def test_agent_message_sets_socket_timeout_and_closes_socket():
    with with_cephadm_module() as mgr:
        ctx, make_raw, raw, tls = _fake_sockets([None])
        thread, _ = _run_message(mgr, ctx, make_raw)
        assert thread.get_agent_response() == 'ACK'
        raw[0].settimeout.assert_called_once_with(AGENT_MESSAGE_TIMEOUT_S)
        # the timeout is set before the TLS socket connects
        assert raw[0].method_calls[0] == call.settimeout(AGENT_MESSAGE_TIMEOUT_S)
        tls[0].__exit__.assert_called_once()
        raw[0].__exit__.assert_called_once()
        assert mgr.agent_cache.sending_agent_message['test'] is False


def test_agent_message_timeout_is_not_retried():
    with with_cephadm_module() as mgr:
        ctx, make_raw, raw, tls = _fake_sockets([socket.timeout('timed out')])
        with patch.object(mgr.log, 'error') as log_error:
            _, sleep = _run_message(mgr, ctx, make_raw)
        assert len(tls) == 1
        sleep.assert_not_called()
        assert 'Timed out' in log_error.call_args[0][0]
        tls[0].__exit__.assert_called_once()
        assert mgr.agent_cache.sending_agent_message['test'] is False


def test_agent_message_refused_connection_is_still_retried():
    with with_cephadm_module() as mgr:
        ctx, make_raw, raw, tls = _fake_sockets([ConnectionRefusedError(), None])
        thread, sleep = _run_message(mgr, ctx, make_raw)
        assert len(tls) == 2
        sleep.assert_called_once_with(3)
        assert thread.get_agent_response() == 'ACK'
        # the socket of the failed attempt is closed too
        tls[0].__exit__.assert_called_once()
        assert mgr.agent_cache.sending_agent_message['test'] is False
