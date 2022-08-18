import errno
import unittest

from ..tests import CLICommandTestMixin, CmdException


class DashboardCommandsTest(unittest.TestCase, CLICommandTestMixin):

    CMD_OK = 'Option GRAFANA_API_URL updated'
    CMD_ERR = "invalid value. '{}' is not a valid URL"

    def test_grafana_set_url_invalid_http_protocol(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('set-grafana-api-url', value='foo://localhost:3000')
        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception), self.CMD_ERR.format('foo://localhost:3000'))

    def test_grafana_set_url_invalid_hostname(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('set-grafana-api-url', value='https://bar:3000')
        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception), self.CMD_ERR.format('https://bar:3000'))

    def test_grafana_set_url_invalid_port(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('set-grafana-api-url', value='https://localhost:foobar')
        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception), self.CMD_ERR.format('https://localhost:foobar'))

    def test_grafana_set_url(self):
        res = self.exec_cmd('set-grafana-api-url', value='http://localhost:3000')
        self.assertEqual(res, self.CMD_OK)
        res = self.exec_cmd('set-grafana-api-url', value='https://localhost:3000')
        self.assertEqual(res, self.CMD_OK)
        res = self.exec_cmd('set-grafana-api-url', value='https://127.0.0.1:3000')
        self.assertEqual(res, self.CMD_OK)
