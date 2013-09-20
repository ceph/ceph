import json
import httplib2
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


def get_status(ctx, name):
    success, content, _ = send_request('GET', os.path.join(config.lock_server, name))
    if success:
        return json.loads(content)
    return None
