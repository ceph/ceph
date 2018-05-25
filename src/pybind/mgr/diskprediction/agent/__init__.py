from __future__ import absolute_import


from requests.auth import HTTPBasicAuth

from ..common.netlib import BaseNET
from ..common import timeout, TimeoutError


class Command(BaseNET):
    def __init__(self, host, user=None, password=None,
                 dbname='telegraf', port=8086):
        self.host = host
        self.dbname = dbname
        self.port = port
        self.auth = None
        if user and password:
            self.auth = HTTPBasicAuth(user, password)

    def __nonzero__(self):
        if self.host:
            return True
        else:
            return False

    def _base_url(self):
        return "https://{0}:{1}".format(self.host, self.port)

    def _send_cmd(self, method, url, **kwargs):
        return self.send_data(method=method, url=url, verify=False, **kwargs)

    def test_connection(self):
        resp = self.show_database()
        if resp.status_code == 200:
            return True
        return False

    def send_info(self, data):
        kwargs = {
            'auth': self.auth,
            'data': data,
            'headers': {"Content-Type": "application/x-www-form-urlencoded"}
        }
        url = "{0}/write?db={1}".format(self._base_url(), self.dbname)
        return self._send_cmd('POST', url=url, **kwargs)

    def query_info(self, sql):
        kwargs = {
            'auth': self.auth,
            'headers': {"Accept": "application/json"},
            'params': {'db': self.dbname, 'q': sql, 'epoch': 'ns'}
        }
        url = "{0}/query".format(self._base_url())
        return self._send_cmd('GET', url=url, **kwargs)

    def show_database(self):
        kwargs = {
            'auth': self.auth,
            'params': {
                "q": "SHOW DATABASES"},
            'headers': {"Accept": "application/json"}
        }
        url = "{0}/query".format(self._base_url())
        return self._send_cmd('GET', url=url, **kwargs)

    def create_database(self):
        kwargs = {
            'auth': self.auth,
            'headers': {"Content-Type": "application/x-www-form-urlencoded"}
        }
        url = "{0}/query?q=CREATE DATABASE {1}".format(
            self._base_url(), self.dbname)
        return self._send_cmd('POST', url=url, **kwargs)


class BaseAgent(object):

    measurement = ''

    def __init__(self, ceph_conext, obj_sender, timeout=30):
        self.data = []
        self._command = None
        if obj_sender and isinstance(obj_sender, Command):
            self._command = obj_sender
        else:
            raise TypeError
        self._logger = ceph_conext.log
        self._ceph_context = ceph_conext
        self._timeout = timeout

    def run(self):
        try:
            self._collect_data()
            self._run()
        except TimeoutError:
            self._logger.error(
                "%s failed to execute %s task" % (__name__, self.measurement))

    def __nonzero__(self):
        if not self._ceph_context and not self._command:
            return False
        else:
            return True

    @timeout()
    def _run(self):
        pass

    @timeout()
    def _collect_data(self):
        pass
