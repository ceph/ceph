# -*- coding: utf-8 -*-
import unittest
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import patch

from orchestrator import DaemonDescription

from ..controllers._version import APIVersion
from ..controllers.certificate import Certificate
from ..tests import ControllerTestCase


class CertificateTestBase(ControllerTestCase):
    """Base class for certificate controller tests with shared helpers."""

    URL_CERTIFICATE = '/api/service/certificate'
    URL_ROOT_CA = '/api/service/certificate/root-ca'

    @classmethod
    def setup_server(cls):
        # Mock mgr.get_module_option_ex to return default threshold of 30
        # Patch it where it's used in the certificate service
        patch('dashboard.services.certificate.mgr.get_module_option_ex', return_value=30).start()
        cls.setup_controllers([Certificate])

    def _setup_fake_client(self, fake_client):
        """Helper to set up fake orchestrator client with required mocks."""
        fake_client.available.return_value = True
        fake_client.get_missing_features.return_value = []
        return fake_client

    def _create_mock_cert_details(self, remaining_days=100, common_name='test.example.com',
                                  issuer='cephadm', key_type='RSA', key_size=2048):
        """Helper to create mock certificate details."""
        not_after = datetime.now() + timedelta(days=remaining_days)
        return {
            'validity': {
                'not_before': '2024-01-01T00:00:00Z',
                'not_after': not_after.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'remaining_days': remaining_days
            },
            'subject': {
                'commonName': common_name,
                'CN': common_name
            },
            'issuer': {
                'commonName': issuer,
                'CN': issuer
            },
            'public_key': {
                'key_type': key_type,
                'key_size': key_size
            },
            'extensions': {
                'subjectAltName': {
                    'DNS': [common_name, '*.example.com'],
                    'IP': ['192.168.1.1']
                }
            }
        }

    def _create_mock_cert_ls_data(self, cert_name='rgw_ssl_cert', scope='SERVICE',
                                  service_name='rgw.test', cert_details=None):
        """Helper to create mock cert_ls data structure."""
        if cert_details is None:
            cert_details = self._create_mock_cert_details()
        return {
            cert_name: {
                'scope': scope,
                'certificates': {
                    service_name: cert_details
                }
            }
        }


class CertificateListControllerTest(CertificateTestBase):
    """Tests for the certificate list() endpoint."""

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_empty(self, instance):
        """Test listing certificates when none exist."""
        fake_client = self._setup_fake_client(mock.Mock())
        fake_client.cert_store.cert_ls.return_value = {}
        instance.return_value = fake_client

        self._get(self.URL_CERTIFICATE, version=APIVersion(1, 0))
        self.assertStatus(200)
        self.assertJsonBody([])

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_basic(self, instance):
        """Test listing certificates with basic data."""
        fake_client = self._setup_fake_client(mock.Mock())
        cert_details = self._create_mock_cert_details(remaining_days=100)
        cert_ls_data = self._create_mock_cert_ls_data(
            cert_name='rgw_ssl_cert',
            scope='SERVICE',
            service_name='rgw.test',
            cert_details=cert_details
        )
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        instance.return_value = fake_client

        self._get(self.URL_CERTIFICATE, version=APIVersion(1, 0))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['cert_name'], 'rgw_ssl_cert')
        self.assertEqual(result[0]['scope'], 'SERVICE')
        self.assertEqual(result[0]['status'], 'valid')
        self.assertEqual(result[0]['target'], 'rgw.test')

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_with_filters(self, instance):
        """Test listing certificates with status and scope filters."""
        fake_client = self._setup_fake_client(mock.Mock())
        cert_details_expiring = self._create_mock_cert_details(remaining_days=10)

        # Mock cert_ls to return filtered data (simulating backend filtering)
        # When status=expiring filter is applied, cert_ls returns only expiring certs
        cert_ls_data_expiring = {
            'grafana_ssl_cert': {
                'scope': 'SERVICE',
                'certificates': {
                    'grafana.test': cert_details_expiring
                }
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data_expiring
        instance.return_value = fake_client

        # Test status filter
        self._get('{}?status=expiring'.format(self.URL_CERTIFICATE),
                  version=APIVersion(1, 0))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['cert_name'], 'grafana_ssl_cert')
        self.assertEqual(result[0]['status'], 'expiring')
        # Verify filter_by was passed to cert_ls
        call_args = fake_client.cert_store.cert_ls.call_args
        self.assertIn('status=expiring', call_args[1]['filter_by'])

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_with_service_name_filter(self, instance):
        """Test listing certificates with service name filter."""
        fake_client = self._setup_fake_client(mock.Mock())
        cert_details = self._create_mock_cert_details()
        # Mock cert_ls to return filtered data (simulating backend filtering)
        # When service_name=rgw filter is applied, cert_ls returns only rgw certs
        cert_ls_data = {
            'rgw_ssl_cert': {
                'scope': 'SERVICE',
                'certificates': {
                    'rgw.test': cert_details
                }
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        instance.return_value = fake_client

        self._get('{}?service_type=rgw'.format(self.URL_CERTIFICATE),
                  version=APIVersion(1, 0))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['cert_name'], 'rgw_ssl_cert')
        # Verify filter_by was passed to cert_ls
        call_args = fake_client.cert_store.cert_ls.call_args
        self.assertIn('name=*rgw*', call_args[1]['filter_by'])

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_include_cephadm_signed(self, instance):
        """Test listing certificates with include_cephadm_signed parameter."""
        fake_client = self._setup_fake_client(mock.Mock())
        cert_ls_data = {
            'rgw_ssl_cert': {
                'scope': 'SERVICE',
                'certificates': {
                    'rgw.test': self._create_mock_cert_details()
                }
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        instance.return_value = fake_client

        # Test with include_cephadm_signed=True
        self._get('{}?include_cephadm_signed=true'.format(self.URL_CERTIFICATE),
                  version=APIVersion(1, 0))
        self.assertStatus(200)
        fake_client.cert_store.cert_ls.assert_called_with(
            filter_by='',
            show_details=False,
            include_cephadm_signed=True
        )

        # Test with include_cephadm_signed=False
        self._get('{}?include_cephadm_signed=false'.format(self.URL_CERTIFICATE),
                  version=APIVersion(1, 0))
        self.assertStatus(200)
        fake_client.cert_store.cert_ls.assert_called_with(
            filter_by='',
            show_details=False,
            include_cephadm_signed=False
        )

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_global_scope(self, instance):
        """Test listing certificates with GLOBAL scope."""
        fake_client = self._setup_fake_client(mock.Mock())
        cert_details = self._create_mock_cert_details()
        cert_ls_data = {
            'cephadm_root_ca_cert': {
                'scope': 'GLOBAL',
                'certificates': cert_details
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        instance.return_value = fake_client

        self._get(self.URL_CERTIFICATE, version=APIVersion(1, 0))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['scope'], 'GLOBAL')
        self.assertEqual(result[0].get('target'), '')

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_error_handling(self, instance):
        """Test error handling when cert_ls fails."""
        fake_client = self._setup_fake_client(mock.Mock())
        fake_client.cert_store.cert_ls.side_effect = RuntimeError('Certificate store error')
        instance.return_value = fake_client

        self._get(self.URL_CERTIFICATE, version=APIVersion(1, 0))
        self.assertStatus(500)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_multiple_scopes(self, instance):
        """Test listing certificates with multiple scopes."""
        fake_client = self._setup_fake_client(mock.Mock())
        cert_ls_data = {
            'rgw_ssl_cert': {
                'scope': 'SERVICE',
                'certificates': {
                    'rgw.test': self._create_mock_cert_details()
                }
            },
            'grafana_ssl_cert': {
                'scope': 'HOST',
                'certificates': {
                    'node0': self._create_mock_cert_details()
                }
            },
            'cephadm_root_ca_cert': {
                'scope': 'GLOBAL',
                'certificates': self._create_mock_cert_details()
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        instance.return_value = fake_client

        self._get(self.URL_CERTIFICATE, version=APIVersion(1, 0))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(len(result), 3)
        scopes = [cert['scope'] for cert in result]
        self.assertIn('SERVICE', scopes)
        self.assertIn('HOST', scopes)
        self.assertIn('GLOBAL', scopes)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_list_certificates_combined_filters(self, instance):
        """Test listing certificates with combined filters."""
        fake_client = self._setup_fake_client(mock.Mock())
        cert_details = self._create_mock_cert_details(remaining_days=10)
        cert_ls_data = {
            'rgw_ssl_cert': {
                'scope': 'SERVICE',
                'certificates': {
                    'rgw.test': cert_details
                }
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        instance.return_value = fake_client

        self._get('{}?status=expiring&scope=SERVICE&service_type=rgw'.format(self.URL_CERTIFICATE),
                  version=APIVersion(1, 0))
        self.assertStatus(200)
        fake_client.cert_store.cert_ls.assert_called()
        # Verify filter_by was constructed correctly
        call_args = fake_client.cert_store.cert_ls.call_args
        filter_by = call_args[1]['filter_by']
        self.assertIn('status=expiring', filter_by)
        self.assertIn('scope=service', filter_by)
        self.assertIn('name=*rgw*', filter_by)


class CertificateGetControllerTest(CertificateTestBase):
    """Tests for the certificate get() endpoint."""

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_service_not_found(self, instance):
        """Test getting certificate for non-existent service."""
        fake_client = self._setup_fake_client(mock.Mock())
        fake_client.services.get.return_value = [None]
        instance.return_value = fake_client

        self._get('{}/rgw.nonexistent'.format(self.URL_CERTIFICATE))
        self.assertStatus(500)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_service_no_cert_required(self, instance):
        """Test getting certificate for service that doesn't require certificates."""
        fake_client = self._setup_fake_client(mock.Mock())
        # Create a mock service that doesn't require certificates (e.g., mon)
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'mon'
        service.spec.service_name = mock.Mock(return_value='mon.test')
        fake_client.services.get.return_value = [service]
        instance.return_value = fake_client

        self._get('{}/mon.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertFalse(result['requires_certificate'])

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_not_configured(self, instance):
        """Test getting certificate when certificate is not configured."""
        fake_client = self._setup_fake_client(mock.Mock())
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'rgw'
        service.spec.service_name = mock.Mock(return_value='rgw.test')
        fake_client.services.get.return_value = [service]
        fake_client.cert_store.cert_ls.return_value = {}
        fake_client.services.list_daemons.return_value = [
            DaemonDescription(daemon_type='rgw', daemon_id='test', hostname='node0')
        ]
        instance.return_value = fake_client

        self._get('{}/rgw.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result['status'], 'not_configured')
        self.assertFalse(result['has_certificate'])

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_user_provided(self, instance):
        """Test getting certificate with user-provided certificate."""
        fake_client = self._setup_fake_client(mock.Mock())
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'rgw'
        service.spec.service_name = mock.Mock(return_value='rgw.test')
        fake_client.services.get.return_value = [service]

        cert_details = self._create_mock_cert_details(remaining_days=100, common_name='rgw.test')
        cert_ls_data = self._create_mock_cert_ls_data(
            cert_name='rgw_ssl_cert',
            scope='SERVICE',
            service_name='rgw.test',
            cert_details=cert_details
        )
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        fake_client.services.list_daemons.return_value = [
            DaemonDescription(daemon_type='rgw', daemon_id='test', hostname='node0')
        ]
        instance.return_value = fake_client

        self._get('{}/rgw.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result['cert_name'], 'rgw_ssl_cert')
        self.assertEqual(result['status'], 'valid')
        self.assertEqual(result['signed_by'], 'user')
        self.assertTrue(result['has_certificate'])
        self.assertEqual(result['common_name'], 'rgw.test')
        self.assertIn('details', result)
        self.assertIn('san_entries', result['details'])

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_cephadm_signed(self, instance):
        """Test getting certificate with cephadm-signed certificate."""
        fake_client = self._setup_fake_client(mock.Mock())
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'rgw'
        service.spec.service_name = mock.Mock(return_value='rgw.test')
        fake_client.services.get.return_value = [service]

        cert_details = self._create_mock_cert_details(remaining_days=100)
        cert_ls_data = {
            'cephadm-signed_rgw_cert': {
                'scope': 'HOST',
                'certificates': {
                    'node0': cert_details
                }
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        fake_client.services.list_daemons.return_value = [
            DaemonDescription(daemon_type='rgw', daemon_id='test', hostname='node0')
        ]
        instance.return_value = fake_client

        self._get('{}/rgw.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result['cert_name'], 'cephadm-signed_rgw_cert')
        self.assertEqual(result['signed_by'], 'cephadm')
        self.assertEqual(result['scope'], 'HOST')

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_expiring(self, instance):
        """Test getting certificate that is expiring."""
        fake_client = self._setup_fake_client(mock.Mock())
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'rgw'
        service.spec.service_name = mock.Mock(return_value='rgw.test')
        fake_client.services.get.return_value = [service]

        cert_details = self._create_mock_cert_details(remaining_days=10)
        cert_ls_data = self._create_mock_cert_ls_data(
            cert_name='rgw_ssl_cert',
            scope='SERVICE',
            service_name='rgw.test',
            cert_details=cert_details
        )
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        fake_client.services.list_daemons.return_value = [
            DaemonDescription(daemon_type='rgw', daemon_id='test', hostname='node0')
        ]
        instance.return_value = fake_client

        self._get('{}/rgw.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result['status'], 'expiring')
        self.assertEqual(result['days_to_expiration'], 10)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_expired(self, instance):
        """Test getting certificate that is expired."""
        fake_client = self._setup_fake_client(mock.Mock())
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'rgw'
        service.spec.service_name = mock.Mock(return_value='rgw.test')
        fake_client.services.get.return_value = [service]

        cert_details = self._create_mock_cert_details(remaining_days=-10)
        cert_ls_data = self._create_mock_cert_ls_data(
            cert_name='rgw_ssl_cert',
            scope='SERVICE',
            service_name='rgw.test',
            cert_details=cert_details
        )
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        fake_client.services.list_daemons.return_value = [
            DaemonDescription(daemon_type='rgw', daemon_id='test', hostname='node0')
        ]
        instance.return_value = fake_client

        self._get('{}/rgw.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result['status'], 'expired')
        self.assertEqual(result['days_to_expiration'], -10)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_invalid(self, instance):
        """Test getting certificate that is invalid."""
        fake_client = self._setup_fake_client(mock.Mock())
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'rgw'
        service.spec.service_name = mock.Mock(return_value='rgw.test')
        fake_client.services.get.return_value = [service]

        cert_ls_data = {
            'rgw_ssl_cert': {
                'scope': 'SERVICE',
                'certificates': {
                    'rgw.test': {'Error': 'Invalid certificate format'}
                }
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        fake_client.services.list_daemons.return_value = [
            DaemonDescription(daemon_type='rgw', daemon_id='test', hostname='node0')
        ]
        instance.return_value = fake_client

        self._get('{}/rgw.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result['status'], 'invalid')
        self.assertTrue(result['has_certificate'])
        self.assertIn('error', result)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_get_certificate_host_scope(self, instance):
        """Test getting certificate with HOST scope."""
        fake_client = self._setup_fake_client(mock.Mock())
        service = mock.Mock()
        service.spec = mock.Mock()
        service.spec.service_type = 'grafana'
        service.spec.service_name = mock.Mock(return_value='grafana.test')
        fake_client.services.get.return_value = [service]

        cert_details = self._create_mock_cert_details()
        cert_ls_data = {
            'grafana_ssl_cert': {
                'scope': 'HOST',
                'certificates': {
                    'node0': cert_details
                }
            }
        }
        fake_client.cert_store.cert_ls.return_value = cert_ls_data
        fake_client.services.list_daemons.return_value = [
            DaemonDescription(daemon_type='grafana', daemon_id='test', hostname='node0')
        ]
        instance.return_value = fake_client

        self._get('{}/grafana.test'.format(self.URL_CERTIFICATE))
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result['scope'], 'HOST')
        self.assertEqual(result['target'], 'node0')


class CertificateRootCAControllerTest(CertificateTestBase):
    """Tests for the certificate root_ca() endpoint."""

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_root_ca_success(self, instance):
        """Test getting root CA certificate successfully."""
        fake_client = self._setup_fake_client(mock.Mock())
        root_ca_cert = '-----BEGIN CERTIFICATE-----\nMOCK_ROOT_CA_CERT\n-----END CERTIFICATE-----'
        fake_client.cert_store.get_cert.return_value = root_ca_cert
        instance.return_value = fake_client

        self._get(self.URL_ROOT_CA)
        self.assertStatus(200)
        result = self.json_body()
        self.assertEqual(result, root_ca_cert)
        fake_client.cert_store.get_cert.assert_called_with('cephadm_root_ca_cert')

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_root_ca_not_found(self, instance):
        """Test getting root CA certificate when it doesn't exist."""
        fake_client = self._setup_fake_client(mock.Mock())
        fake_client.cert_store.get_cert.return_value = None
        instance.return_value = fake_client

        self._get(self.URL_ROOT_CA)
        self.assertStatus(404)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_root_ca_error_handling(self, instance):
        """Test error handling when getting root CA certificate fails."""
        fake_client = self._setup_fake_client(mock.Mock())
        fake_client.cert_store.get_cert.side_effect = Exception('Certificate store error')
        instance.return_value = fake_client

        self._get(self.URL_ROOT_CA)
        self.assertStatus(500)

    @mock.patch('dashboard.controllers.certificate.OrchClient.instance')
    def test_root_ca_method_not_available(self, instance):
        """Test getting root CA when get_cert method is not available."""
        fake_client = self._setup_fake_client(mock.Mock())
        del fake_client.cert_store.get_cert
        instance.return_value = fake_client

        self._get(self.URL_ROOT_CA)
        self.assertStatus(500)


if __name__ == '__main__':
    unittest.main()
