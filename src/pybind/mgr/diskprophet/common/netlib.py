from __future__ import absolute_import

from httplib import INTERNAL_SERVER_ERROR, SERVICE_UNAVAILABLE, REQUEST_TIMEOUT, BAD_REQUEST

from requests import Request, Session, exceptions
from requests.models import Response


DEFAULT_TIMEOUT = 30
SUPPORTED_METHOD = ['GET', 'POST']


class DummyResonse(object):
    def __init__(self):
        self.json = {}
        self.content = "DummyResponse"
        self.status_code = BAD_REQUEST


class BaseNET(object):
    def send_data(self, method, url, verify=None, **kwargs):
        if method.upper() not in SUPPORTED_METHOD:
            return None

        with Session() as s:
            try:
                req = Request(method, url=url, **kwargs)
                prepared = req.prepare()
                resp = s.send(prepared, verify=verify, timeout=DEFAULT_TIMEOUT)
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
