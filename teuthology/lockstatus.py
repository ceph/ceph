import json
import httplib2
import logging

log = logging.getLogger(__name__)

def _lock_url(ctx):
    return ctx.teuthology_config['lock_server']

def send_request(method, url, body=None, headers=None):
    http = httplib2.Http()
    resp, content = http.request(url, method=method, body=body, headers=headers)
    if resp.status == 200:
        return (True, content, resp.status)
    log.info("%s request to '%s' with body '%s' failed with response code %d",
             method, url, body, resp.status)
    return (False, None, resp.status)

def get_status(ctx, name):
    success, content, _ = send_request('GET', _lock_url(ctx) + '/' + name)
    if success:
        return json.loads(content)
    return None


