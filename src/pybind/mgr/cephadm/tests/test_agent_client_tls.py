import ssl
from unittest.mock import patch

from cephadm.agent import AgentMessageThread
from cephadm.services.cephadmservice import CephadmAgent
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
