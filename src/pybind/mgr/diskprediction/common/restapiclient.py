# vim: tabstop=4 shiftwidth=4 softtabstop=4

from httplib import SERVICE_UNAVAILABLE, REQUEST_TIMEOUT, BAD_REQUEST
from requests import Request, Session, exceptions
from requests.auth import HTTPBasicAuth

DEFAULT_TIMEOUT = 30
SUPPORTED_METHOD = ['GET', 'POST']


class DummyResonse:
    def __init__(self):
        self.json = {}
        self.content = 'DummyResponse'
        self.status_code = BAD_REQUEST


def gen_configuration(**kwargs):
    configuration = {
        'host': kwargs.get('host'),
        'user': kwargs.get('user'),
        'password': kwargs.get('password'),
        'port': kwargs.get('port', 8086),
        'dbname': 'telegraf',
        'mgr_inst': kwargs.get('mgr_inst', None)}
    return configuration


class RestApiClient:

    def __init__(self, configuration):
        self.auth = None
        self.url = None
        self.host = configuration.get('host')
        self.port = configuration.get('port')
        self.dbname = configuration.get('dbname')
        if configuration.get('user') and configuration.get('password'):
            self.auth = HTTPBasicAuth(
                configuration.get('user'),
                configuration.get('password'))
        if self.host and self.port:
            self.url = self._base_url()

    def __nonzero__(self):
        if self.auth and self.url:
            return True
        else:
            return False

    def _base_url(self):
        return 'https://{}:{}'.format(self.host, self.port)

    def test_connection(self):
        kwargs = {
            'auth': self.auth,
            'params': {
                'q': 'SHOW DATABASES'},
            'headers': {'Accept': 'application/json'}
        }
        url = '{0}/query'.format(self.url)
        return self.send_data('GET', url=url, verify=False, **kwargs)

    def send_info(self, *args, **kwargs):
        kwargs['auth'] = self.auth
        kwargs['headers'] = \
            {'Content-Type': 'application/x-www-form-urlencoded'}
        url = '{}/write?db={}'.format(self._base_url(), self.dbname)
        return self.send_data('POST', url=url, **kwargs)

    def query_info(self, sql):
        kwargs = {
            'auth': self.auth,
            'headers': {'Accept': 'application/json'},
            'params': {'db': self.dbname, 'q': sql, 'epoch': 'ns'}
        }
        url = '{0}/query'.format(self._base_url())
        return self.send_data('GET', url=url, **kwargs)

    @staticmethod
    def send_data(method, url=None, verify=None, **kwargs):
        if method.upper() not in SUPPORTED_METHOD:
            return None

        if kwargs.get('measurement'):
            del kwargs['measurement']

        with Session() as s:
            try:
                req = Request(method, url=url, **kwargs)
                prepared = req.prepare()
                resp = s.send(
                    prepared, verify=verify, timeout=DEFAULT_TIMEOUT)
            except exceptions.Timeout as e:
                resp = DummyResonse()
                resp.json = {}
                resp.status_code = REQUEST_TIMEOUT
                resp.content = str(e)
            except exceptions.ConnectionError as e:
                resp = DummyResonse()
                resp.json = {}
                resp.status_code = SERVICE_UNAVAILABLE
                resp.content = str(e)
            except Exception as e:
                resp = DummyResonse()
                resp.json = {}
                resp.status_code = BAD_REQUEST
                resp.content = str(e)
            return resp
