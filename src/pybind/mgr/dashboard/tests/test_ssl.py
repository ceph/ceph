import errno
import unittest

from .. import mgr
from ..services.orchestrator import OrchClient
from ..tests import CLICommandTestMixin, CmdException


class SslTest(unittest.TestCase, CLICommandTestMixin):

    def test_ssl_certificate_and_key(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('set-ssl-certificate', inbuf='', mgr_id='x')
        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception), 'Please specify the certificate with "-i" option')

        result = self.exec_cmd('set-ssl-certificate', inbuf='content', mgr_id='x')
        self.assertEqual(result, 'SSL certificate updated')

        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('set-ssl-certificate-key', inbuf='', mgr_id='x')
        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception), 'Please specify the certificate key with "-i" option')

        result = self.exec_cmd('set-ssl-certificate-key', inbuf='content', mgr_id='x')
        self.assertEqual(result, 'SSL certificate key updated')

    def test_set_mgr_created_self_signed_cert(self):
        result = self.exec_cmd('create-self-signed-cert')
        self.assertEqual(result, 'Self-signed certificate created')

    def test_set_mgr_created_self_signed_cert_without_cephadm(self):
        with unittest.mock.patch.object(mgr, 'get_module_option_ex', return_value=None), \
            unittest.mock.patch.object(OrchClient, 'instance') as mock_orch_instance, \
            unittest.mock.patch('dashboard.module.create_self_signed_cert',
                                return_value=('cert', 'key')) as mock_create:
            result = self.exec_cmd('create-self-signed-cert')
        self.assertEqual(result, 'Self-signed certificate created')
        mock_orch_instance.assert_not_called()
        mock_create.assert_called_once_with('IT', 'ceph-dashboard')

    def test_set_mgr_created_self_signed_cert_with_cephadm(self):
        mock_orch = unittest.mock.MagicMock()
        mock_orch.available.return_value = True
        mock_orch.cert_store.generate_cert.return_value = {
            'cert': 'cephadm-cert',
            'key': 'cephadm-key',
        }

        with unittest.mock.patch.object(mgr, 'get_module_option_ex', return_value='cephadm'), \
                unittest.mock.patch.object(OrchClient, 'instance', return_value=mock_orch), \
                unittest.mock.patch('dashboard.module.create_self_signed_cert') as mock_create:
            result = self.exec_cmd('create-self-signed-cert')

        self.assertEqual(result, 'Self-signed certificate created')
        mock_orch.cert_store.generate_cert.assert_called_once_with()
        mock_create.assert_not_called()

    def test_set_mgr_created_self_signed_cert_cephadm_invalid_result(self):
        invalid_results = [
            None,
            {},
            {'cert': 'cephadm-cert'},
            {'key': 'cephadm-key'},
        ]

        with unittest.mock.patch.object(mgr, 'get_module_option_ex', return_value='cephadm'):
            for invalid_result in invalid_results:
                with self.subTest(result=invalid_result):
                    mock_orch = unittest.mock.MagicMock()
                    mock_orch.available.return_value = True
                    mock_orch.cert_store.generate_cert.return_value = invalid_result

                    with unittest.mock.patch.object(
                        OrchClient, 'instance', return_value=mock_orch
                    ):
                        with self.assertRaises(CmdException) as ctx:
                            self.exec_cmd('create-self-signed-cert')

                    self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
                    self.assertEqual(str(ctx.exception), 'Failed to generate certificates')

    def test_set_mgr_created_self_signed_cert_cephadm_unavailable_fallback(self):
        mock_orch = unittest.mock.MagicMock()
        mock_orch.available.return_value = False

        with unittest.mock.patch.object(mgr, 'get_module_option_ex', return_value='cephadm'), \
                unittest.mock.patch.object(OrchClient, 'instance', return_value=mock_orch), \
                unittest.mock.patch('dashboard.module.create_self_signed_cert',
                                    return_value=('fallback-cert', 'fallback-key')) as mock_create:
            result = self.exec_cmd('create-self-signed-cert')

        self.assertEqual(result, 'Self-signed certificate created')
        mock_orch.cert_store.generate_cert.assert_not_called()
        mock_create.assert_called_once_with('IT', 'ceph-dashboard')
