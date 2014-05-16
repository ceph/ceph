import httplib2
import requests
import logging
import os
from .config import config

log = logging.getLogger(__name__)


def send_request(method, url, body=None, headers=None):
    http = httplib2.Http()
    resp, content = http.request(url, method=method, body=body, headers=headers)
    if resp.status == 200:
        return (True, content, resp.status)
    log.info("%s request to '%s' with body '%s' failed with response code %d",
             method, url, body, resp.status)
    return (False, None, resp.status)


def get_status(name):
    uri = os.path.join(config.lock_server, 'nodes', name, '')
    response = requests.get(uri)
    success = response.ok
    if success:
        return response.json()
    return None
