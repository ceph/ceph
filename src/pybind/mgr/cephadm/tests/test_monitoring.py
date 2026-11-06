from unittest import mock

from cephadm.services.monitoring import GrafanaService
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec


class TestGrafanaServiceCertificates:
    """
    Regression tests confirming Grafana's self-signed certificate
    generation no longer builds the X.509 Common Name (CN) directly
    from the host's FQDN. mgr_util.create_self_signed_cert() had no
    length check, and RFC 5280 limits CN to 64 characters, so cloud
    providers with long auto-assigned FQDNs (e.g. GCP) would fail
    certificate signing with an opaque low-level error:
    [('asn1 encoding routines', '', 'string too long')]

    Fixed by switching to ssl_certs.generate_cert(host, addr), which
    puts the (always-short) node IP in the CN and the full FQDN in the
    SAN instead, avoiding the length limit entirely rather than
    truncating anything.
    """

    def _make_service(self, node_ip, host_fqdn):
        mgr = mock.MagicMock()
        mgr.inventory.get_addr.return_value = node_ip
        mgr.cert_key_store.get_cert.return_value = None
        mgr.cert_key_store.get_key.return_value = None
        mgr.get.return_value = {'modules': []}

        service = GrafanaService.__new__(GrafanaService)
        service.mgr = mgr
        service._inventory_get_fqdn = mock.MagicMock(return_value=host_fqdn)
        return service, mgr

    def test_long_fqdn_uses_generate_cert_not_create_self_signed_cert(self):
        """
        Regression test: prepare_certificates() must call
        ssl_certs.generate_cert(host, addr), not the old
        create_self_signed_cert(), for a host with a long FQDN.
        """
        long_fqdn = "ceph-node1.europe-west3-a.c.project-12850071-31c6-4077-a2f.internal"
        node_ip = "10.156.0.5"
        service, mgr = self._make_service(node_ip, long_fqdn)

        daemon_spec = mock.MagicMock(spec=CephadmDaemonDeploySpec)
        daemon_spec.host = 'ceph-node1'

        mgr.http_server.service_discovery.ssl_certs.generate_cert.return_value = (
            'FAKE_CERT_PEM', 'FAKE_KEY_PEM'
        )

        service.prepare_certificates(daemon_spec)

        mgr.http_server.service_discovery.ssl_certs.generate_cert.assert_called_once_with(
            long_fqdn, node_ip
        )
        mgr.cert_key_store.save_cert.assert_called_once_with(
            'grafana_cert', 'FAKE_CERT_PEM', host='ceph-node1'
        )

    def test_short_fqdn_also_uses_generate_cert(self):
        """
        The fix applies uniformly, not just to long FQDNs: even a
        short hostname now goes through generate_cert(), consistent
        with how Prometheus's cert is already generated elsewhere in
        this file.
        """
        short_fqdn = "node1.local"
        node_ip = "10.156.0.5"
        service, mgr = self._make_service(node_ip, short_fqdn)

        daemon_spec = mock.MagicMock(spec=CephadmDaemonDeploySpec)
        daemon_spec.host = 'node1'

        mgr.http_server.service_discovery.ssl_certs.generate_cert.return_value = (
            'FAKE_CERT_PEM', 'FAKE_KEY_PEM'
        )

        service.prepare_certificates(daemon_spec)

        mgr.http_server.service_discovery.ssl_certs.generate_cert.assert_called_once_with(
            short_fqdn, node_ip
        )
