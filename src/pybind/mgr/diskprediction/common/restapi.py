# vim: tabstop=4 shiftwidth=4 softtabstop=4

from httplib import SERVICE_UNAVAILABLE, REQUEST_TIMEOUT, BAD_REQUEST
from requests import Request, Session, exceptions
from requests.auth import HTTPBasicAuth

DEFAULT_TIMEOUT = 30
SUPPORTED_METHOD = ['GET', 'POST']


class DummyResonse:
    def __init__(self):
        self.json = {}
        self.content = "DummyResponse"
        self.status_code = BAD_REQUEST


def gen_configuration(**kwargs):
    configuration = {
        'host': kwargs.get('host'),
        'user': kwargs.get('user'),
        'password': kwargs.get('password'),
        'port': kwargs.get('port', 8086),
        'dbname': 'telegraf'}
    return configuration


class RestAPI:

    def __init__(self, configuration):
        self.auth = None
        self.url = None
        if configuration.get('user') and configuration.get('password'):
            self.auth = HTTPBasicAuth(
                configuration.get('user'),
                configuration.get('password'))
        if configuration.get('host') and configuration.get('port'):
            self.url = "https://{0}:{1}".format(
                configuration.get('host'),
                configuration.get('port'))

    def __nonzero__(self):
        if self.auth and self.url:
            return True
        else:
            return False

    def test_connection(self):
        kwargs = {
            'auth': self.auth,
            'params': {
                "q": "SHOW DATABASES"},
            'headers': {"Accept": "application/json"}
        }
        url = "{0}/query".format(self.url)
        return self.send_data('GET', url=url, verify=False, **kwargs)

    def send_data(self, method, url=None, verify=None, **kwargs):
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
