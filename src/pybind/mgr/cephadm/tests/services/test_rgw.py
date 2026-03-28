
import pytest
from unittest.mock import patch, MagicMock

from cephadm.services.service_registry import service_registry
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import RGWSpec, CertificateSource
from cephadm.tests.fixtures import with_host, with_service, _run_cephadm
from cephadm.tlsobject_types import TLSCredentials


ceph_generated_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""


ceph_generated_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


class TestRGWService:

    @pytest.mark.parametrize(
        "frontend, ssl, extra_args, expected",
        [
            ('beast', False, ['tcp_nodelay=1'],
             'beast endpoint=[fd00:fd00:fd00:3000::1]:80 tcp_nodelay=1'),
            ('beast', True, ['tcp_nodelay=0', 'max_header_size=65536'],
             'beast ssl_endpoint=[fd00:fd00:fd00:3000::1]:443 ssl_certificate=config://rgw/cert/rgw.foo tcp_nodelay=0 max_header_size=65536'),
            ('civetweb', False, [], 'civetweb port=[fd00:fd00:fd00:3000::1]:80'),
            ('civetweb', True, None,
             'civetweb port=[fd00:fd00:fd00:3000::1]:443s ssl_certificate=config://rgw/cert/rgw.foo'),
        ]
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_rgw_update(self, frontend, ssl, extra_args, expected, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            cephadm_module.cache.update_host_networks('host1', {
                'fd00:fd00:fd00:3000::/64': {
                    'if0': ['fd00:fd00:fd00:3000::1']
                }
            })
            s = RGWSpec(service_id="foo",
                        networks=['fd00:fd00:fd00:3000::/64'],
                        ssl=ssl,
                        rgw_frontend_type=frontend,
                        rgw_frontend_extra_args=extra_args)
            with with_service(cephadm_module, s) as dds:
                _, f, _ = cephadm_module.check_mon_command({
                    'prefix': 'config get',
                    'who': f'client.{dds[0]}',
                    'key': 'rgw_frontends',
                })
                assert f == expected

    @pytest.mark.parametrize(
        "disable_sync_traffic",
        [
            (True),
            (False),
        ]
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_rgw_disable_sync_traffic(self, disable_sync_traffic, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            s = RGWSpec(service_id="foo",
                        disable_multisite_sync_traffic=disable_sync_traffic)
            with with_service(cephadm_module, s) as dds:
                _, f, _ = cephadm_module.check_mon_command({
                    'prefix': 'config get',
                    'who': f'client.{dds[0]}',
                    'key': 'rgw_run_sync_thread',
                })
                assert f == ('false' if disable_sync_traffic else 'true')

    def _make_rgw_post_remove_fixtures(self, cephadm_module, host='host1', ssl=True, certificate_source='inline'):
        """
        Returns (cm, svc_name, spec, daemon, mock_spec_store) with all common
        setup done.  The caller is responsible for patching spec_store and
        cache.get_daemons_by_service.
        """
        cephadm_module._init_cert_mgr()
        cm = cephadm_module.cert_mgr

        spec = RGWSpec(
            service_id='foo',
            ssl=ssl,
            certificate_source=certificate_source,
            rgw_frontend_type='beast',
        )
        svc_name = spec.service_name()  # 'rgw.foo'

        daemon = MagicMock()
        daemon.daemon_type = 'rgw'
        daemon.daemon_id = f'foo.{host}.0'
        daemon.hostname = host
        daemon.name.return_value = f'rgw.{daemon.daemon_id}'
        daemon.service_name.return_value = svc_name

        mock_entry = MagicMock()
        mock_entry.spec = spec
        mock_spec_store = MagicMock()
        mock_spec_store.__contains__ = MagicMock(return_value=True)
        mock_spec_store.__getitem__ = MagicMock(return_value=mock_entry)

        return cm, svc_name, spec, daemon, mock_spec_store

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_no_op_when_requires_certificates_is_false(
            self, cephadm_module: CephadmOrchestrator):
        """
        When requires_certificates is False, post_remove() must return
        immediately without touching cert_mgr at all.
        """
        _, svc_name, _, daemon, _ = self._make_rgw_post_remove_fixtures(cephadm_module)

        rgw_svc = service_registry.get_service('rgw')

        with patch.object(type(rgw_svc), 'requires_certificates',
                          new_callable=lambda: property(lambda self: False)):
            with patch.object(rgw_svc.mgr.cert_mgr,
                              'rm_inline_saved_cert_key_pair') as rm_mock:
                rgw_svc.post_remove(daemon, is_failed_deploy=False)
                rm_mock.assert_not_called()

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_no_op_when_svc_not_in_spec_store(
            self, cephadm_module: CephadmOrchestrator):
        """
        When the service is not found in spec_store, post_remove() must return
        immediately without touching cert_mgr.
        """
        cephadm_module._init_cert_mgr()

        spec = RGWSpec(service_id='foo', ssl=True, rgw_frontend_type='beast')
        daemon = MagicMock()
        daemon.daemon_type = 'rgw'
        daemon.daemon_id = 'foo.host1.0'
        daemon.hostname = 'host1'
        daemon.name.return_value = 'rgw.foo.host1.0'
        daemon.service_name.return_value = spec.service_name()

        # spec_store explicitly does NOT contain the service
        mock_spec_store = MagicMock()
        mock_spec_store.__contains__ = MagicMock(return_value=False)

        rgw_svc = service_registry.get_service('rgw')

        with with_host(cephadm_module, 'host1'):
            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cert_mgr,
                              'rm_inline_saved_cert_key_pair') as rm_mock:
                rgw_svc.post_remove(daemon, is_failed_deploy=False)
                rm_mock.assert_not_called()

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_no_op_when_other_daemons_remain_on_same_host(
            self, cephadm_module: CephadmOrchestrator):
        """
        When another daemon of the same service is still running on the same
        host, post_remove() must NOT clean up certs for that host yet.
        """
        host = 'host1'
        cm, svc_name, _, daemon, mock_spec_store = \
            self._make_rgw_post_remove_fixtures(cephadm_module, host=host)

        # A sibling daemon still on the same host
        sibling = MagicMock()
        sibling.hostname = host
        sibling.name.return_value = 'rgw.foo.host1.1'   # different name

        rgw_svc = service_registry.get_service('rgw')

        with with_host(cephadm_module, host):
            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service',
                              return_value=[daemon, sibling]), \
                 patch.object(cm, 'rm_inline_saved_cert_key_pair') as rm_mock:
                rgw_svc.post_remove(daemon, is_failed_deploy=False)
                rm_mock.assert_not_called()

    # SERVICE-scope cleanup branches

    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_inline_cleanup_called_when_last_daemon_in_service(
            self, cephadm_module: CephadmOrchestrator):
        """
        When the removed daemon is the very last one in the service (SERVICE
        scope), rm_inline_saved_cert_key_pair() must be called with
        service_name=svc_name and host=None.
        """
        host = 'host1'
        cm, svc_name, _, daemon, mock_spec_store = \
            self._make_rgw_post_remove_fixtures(cephadm_module, host=host)

        rgw_svc = service_registry.get_service('rgw')

        with with_host(cephadm_module, host):
            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service',
                              return_value=[daemon]):   # only the daemon being removed
                with patch.object(cm, 'rm_inline_saved_cert_key_pair') as rm_mock:
                    rgw_svc.post_remove(daemon, is_failed_deploy=False)
                    rm_mock.assert_called_once_with(
                        rgw_svc.cert_name,
                        rgw_svc.key_name,
                        service_name=svc_name,
                        host=None,
                        ca_cert_name=rgw_svc.ca_cert_name,
                    )

    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_inline_cleanup_skipped_when_other_daemons_on_other_hosts(
            self, cephadm_module: CephadmOrchestrator):
        """
        SERVICE scope: when other daemons of the same service still exist on
        OTHER hosts, the service-level cert must NOT be removed yet
        (other_daemons_in_service=True → early return from _cleanup).
        """
        host = 'host1'
        cm, svc_name, _, daemon, mock_spec_store = \
            self._make_rgw_post_remove_fixtures(cephadm_module, host=host)

        # A peer daemon on a different host
        peer = MagicMock()
        peer.hostname = 'host2'
        peer.name.return_value = 'rgw.foo.host2.0'

        rgw_svc = service_registry.get_service('rgw')

        with with_host(cephadm_module, host):
            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service',
                              return_value=[daemon, peer]):
                with patch.object(cm, 'rm_inline_saved_cert_key_pair') as rm_mock:
                    rgw_svc.post_remove(daemon, is_failed_deploy=False)
                    rm_mock.assert_not_called()

    # ssl=False branch

    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_cert_source_is_none_when_ssl_disabled(
            self, cephadm_module: CephadmOrchestrator):
        """
        When spec.ssl=False, cert_source is forced to None inside post_remove().
        Cleanup should still run (SERVICE scope, last daemon) but cert_source
        passed to _cleanup_tls_creds_for_host must be None.
        """
        host = 'host1'
        cm, svc_name, _, daemon, mock_spec_store = \
            self._make_rgw_post_remove_fixtures(cephadm_module, host=host, ssl=False)

        rgw_svc = service_registry.get_service('rgw')

        with with_host(cephadm_module, host):
            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service',
                              return_value=[daemon]):
                with patch.object(cm, 'rm_inline_saved_cert_key_pair') as rm_mock:
                    rgw_svc.post_remove(daemon, is_failed_deploy=False)
                    # cleanup still fires (last daemon), cert_source=None doesn't block it
                    rm_mock.assert_called_once_with(
                        rgw_svc.cert_name,
                        rgw_svc.key_name,
                        service_name=svc_name,
                        host=None,
                        ca_cert_name=rgw_svc.ca_cert_name,
                    )

    # Reference cert-source branch

    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_reference_source_logs_and_still_cleans(
            self, cephadm_module: CephadmOrchestrator):
        """
        When certificate_source='reference', post_remove() must still call
        rm_inline_saved_cert_key_pair (the reference note is informational
        only) AND emit the expected INFO log.
        """
        host = 'host1'
        cm, svc_name, _, daemon, mock_spec_store = \
            self._make_rgw_post_remove_fixtures(
                cephadm_module, host=host,
                certificate_source=CertificateSource.REFERENCE.value)

        rgw_svc = service_registry.get_service('rgw')

        with with_host(cephadm_module, host):
            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service',
                              return_value=[daemon]):
                with patch.object(cm, 'rm_inline_saved_cert_key_pair') as rm_mock, \
                     patch('cephadm.services.cephadmservice.logger') as log_mock:
                    rgw_svc.post_remove(daemon, is_failed_deploy=False)
                    rm_mock.assert_called_once()
                    # The "reference; user-provided" info log must have fired
                    assert any(
                        'reference' in str(call_args)
                        for call_args in log_mock.info.call_args_list
                    )

    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_cleans_cephadm_signed_leftovers_for_host(self, cephadm_module: CephadmOrchestrator):
        """
        Ensures RGW service post_remove() removes cephadm-signed
        cert/key leftovers for (service, host) even if the CURRENT cert source
        is not cephadm-signed.
        """
        cephadm_module._init_cert_mgr()
        cm = cephadm_module.cert_mgr

        host = 'host1'

        with with_host(cephadm_module, host):
            spec = RGWSpec(
                service_id="foo",
                ssl=True,
                certificate_source="inline",
                rgw_frontend_type="beast",
            )
            svc_name = spec.service_name()  # typically "rgw.foo"

            # Register the self-signed cert/key pair
            cm.register_self_signed_cert_key_pair(svc_name)

            # Build a mock spec_store that satisfies the two access patterns
            # post_remove() needs:
            #   if svc_name not in self.mgr.spec_store: return
            #   spec = self.mgr.spec_store[svc_name].spec
            mock_entry = MagicMock()
            mock_entry.spec = spec

            mock_spec_store = MagicMock()
            mock_spec_store.__contains__ = MagicMock(return_value=True)
            mock_spec_store.__getitem__ = MagicMock(return_value=mock_entry)

            # Minimal daemon mock used by post_remove()
            daemon = MagicMock()
            daemon.daemon_type = 'rgw'
            daemon.daemon_id = 'foo.host1.0'
            daemon.hostname = host
            daemon.name.return_value = f'rgw.{daemon.daemon_id}'
            daemon.service_name.return_value = svc_name

            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service', return_value=[daemon]):

                # Seed cephadm-signed leftovers for this host
                cm.save_self_signed_cert_key_pair(
                    svc_name,
                    TLSCredentials(ceph_generated_cert, ceph_generated_key),
                    host=host,
                )

                cert_name = cm.self_signed_cert(svc_name)
                key_name = cm.self_signed_key(svc_name)

                # Sanity: leftovers exist pre-cleanup
                assert cm.get_cert(cert_name, host=host) is not None
                assert cm.get_key(key_name, host=host) is not None

                # Get RGW service instance
                rgw_svc = service_registry.get_service('rgw')

                # Ensure the call site is exercised + cleanup actually happens
                with patch.object(cm, "try_rm_self_signed_cert_key_pair",
                                  wraps=cm.try_rm_self_signed_cert_key_pair) as rm_mock:
                    rgw_svc.post_remove(daemon, is_failed_deploy=False)
                    rm_mock.assert_called_once_with(svc_name, host)

                # Assert cephadm-signed leftovers are gone
                assert cm.get_cert(cert_name, host=host) is None
                assert cm.get_key(key_name, host=host) is None

    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_cleans_inline_certs_for_last_daemon_in_service(
            self, cephadm_module: CephadmOrchestrator):
        """
        Ensures RGW service post_remove() actually removes
        inline-saved cert/key from the cert store when the last daemon of the
        service is removed (SERVICE scope → host=None cleanup).
        """
        cephadm_module._init_cert_mgr()
        cm = cephadm_module.cert_mgr
        host = 'host1'

        with with_host(cephadm_module, host):
            spec = RGWSpec(
                service_id='foo',
                ssl=True,
                certificate_source='inline',
                rgw_frontend_type='beast',
            )
            svc_name = spec.service_name()  # 'rgw.foo'

            rgw_svc = service_registry.get_service('rgw')

            # Seed inline cert/key for this service (SERVICE scope → service_name=svc_name)
            cm.save_cert(rgw_svc.cert_name, ceph_generated_cert,
                         service_name=svc_name, user_made=True, editable=False)
            cm.save_key(rgw_svc.key_name, ceph_generated_key,
                        service_name=svc_name, user_made=True, editable=False)

            # Sanity: inline certs exist pre-cleanup
            assert cm.get_cert(rgw_svc.cert_name, service_name=svc_name) is not None
            assert cm.get_key(rgw_svc.key_name, service_name=svc_name) is not None

            mock_entry = MagicMock()
            mock_entry.spec = spec
            mock_spec_store = MagicMock()
            mock_spec_store.__contains__ = MagicMock(return_value=True)
            mock_spec_store.__getitem__ = MagicMock(return_value=mock_entry)

            daemon = MagicMock()
            daemon.daemon_type = 'rgw'
            daemon.daemon_id = f'foo.{host}.0'
            daemon.hostname = host
            daemon.name.return_value = f'rgw.{daemon.daemon_id}'
            daemon.service_name.return_value = svc_name

            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service',
                              return_value=[daemon]):   # only this daemon → last one
                with patch.object(cm, 'rm_inline_saved_cert_key_pair',
                                  wraps=cm.rm_inline_saved_cert_key_pair) as rm_mock:
                    rgw_svc.post_remove(daemon, is_failed_deploy=False)
                    rm_mock.assert_called_once_with(
                        rgw_svc.cert_name,
                        rgw_svc.key_name,
                        service_name=svc_name,
                        host=None,
                        ca_cert_name=rgw_svc.ca_cert_name,
                    )

            # Assert inline certs are actually gone from the store
            assert cm.get_cert(rgw_svc.cert_name, service_name=svc_name) is None
            assert cm.get_key(rgw_svc.key_name, service_name=svc_name) is None

    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_post_remove_preserves_inline_certs_when_other_daemons_remain_in_service(
            self, cephadm_module: CephadmOrchestrator):
        """
        When other daemons of the same service still exist on other hosts,
        inline certs must NOT be removed from the store (SERVICE scope cert
        is shared across the whole service).
        """
        cephadm_module._init_cert_mgr()
        cm = cephadm_module.cert_mgr
        host = 'host1'

        with with_host(cephadm_module, host):
            spec = RGWSpec(
                service_id='foo',
                ssl=True,
                certificate_source='inline',
                rgw_frontend_type='beast',
            )
            svc_name = spec.service_name()

            rgw_svc = service_registry.get_service('rgw')

            # Seed inline cert/key
            cm.save_cert(rgw_svc.cert_name, ceph_generated_cert,
                         service_name=svc_name, user_made=True, editable=False)
            cm.save_key(rgw_svc.key_name, ceph_generated_key,
                        service_name=svc_name, user_made=True, editable=False)

            mock_entry = MagicMock()
            mock_entry.spec = spec
            mock_spec_store = MagicMock()
            mock_spec_store.__contains__ = MagicMock(return_value=True)
            mock_spec_store.__getitem__ = MagicMock(return_value=mock_entry)

            daemon = MagicMock()
            daemon.daemon_type = 'rgw'
            daemon.daemon_id = f'foo.{host}.0'
            daemon.hostname = host
            daemon.name.return_value = f'rgw.{daemon.daemon_id}'
            daemon.service_name.return_value = svc_name

            # A peer daemon still running on a different host
            peer = MagicMock()
            peer.hostname = 'host2'
            peer.name.return_value = 'rgw.foo.host2.0'

            with patch.object(cephadm_module, 'spec_store', mock_spec_store), \
                 patch.object(cephadm_module.cache, 'get_daemons_by_service',
                              return_value=[daemon, peer]):
                rgw_svc.post_remove(daemon, is_failed_deploy=False)

            # Inline certs must still be present — the service is still running on host2
            assert cm.get_cert(rgw_svc.cert_name, service_name=svc_name) is not None
            assert cm.get_key(rgw_svc.key_name, service_name=svc_name) is not None
