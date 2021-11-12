import errno
import unittest

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
