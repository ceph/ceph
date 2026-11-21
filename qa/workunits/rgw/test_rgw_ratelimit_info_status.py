#!/usr/bin/env python3
"""
GET /admin/ratelimit must record a successful request as successful.

https://tracker.ceph.com/issues/82558: for ratelimit-scope=user the op fell
through to "op_ret = -EINVAL" after it had already sent its 200 response, and
for global=true with no stored period config it left -ENOENT in op_ret.  The
client saw HTTP 200 with the right body, but radosgw logged the request as
failed:

  ====== req done ... op=get_ratelimit_info ... status=-22 http_status=200 ...

The response itself was always correct, so this test reads the radosgw log
and checks the "req done" line of each request (logged at debug_rgw 1).

Runs against vstart (from the build dir) or in teuthology (client.0, with
radosgw on the same host).  Override discovery with:
  RGW_ENDPOINT      e.g. http://localhost:8000
  RGW_ADMIN_SOCKET  radosgw admin socket, used to find its log file
  RGW_LOG_FILE      radosgw log file
"""

import glob
import json
import os
import re
import subprocess
import sys
import time
import uuid

import botocore.auth
import botocore.awsrequest
import botocore.credentials
import requests

ADMIN_UID = 'ratelimit-status-admin'
ADMIN_ACCESS = 'RLSTATUSADMINACCESS0'
ADMIN_SECRET = 'rlstatusadminsecretkey000000000000000000'
SUFFIX = uuid.uuid4().hex[:8]
PLAIN_UID = f'rl-status-plain-{SUFFIX}'
LIMITED_UID = f'rl-status-limited-{SUFFIX}'
BUCKET = f'rl-status-bucket-{SUFFIX}'


def log(msg):
    print(msg, flush=True)


def tool(name):
    local = os.path.join('.', 'bin', name)
    return local if os.access(local, os.X_OK) else name


def run(*args, check=True):
    cmd = list(args)
    log('$ ' + ' '.join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(f'{cmd} failed ({p.returncode}): {p.stderr.strip()}')
    return p


def rgwadmin(*args, check=True):
    return run(tool('radosgw-admin'), *args, check=check)


def find_admin_socket():
    if os.environ.get('RGW_ADMIN_SOCKET'):
        return os.environ['RGW_ADMIN_SOCKET']
    candidates = []
    ceph_id = os.environ.get('CEPH_ID')
    if ceph_id:
        candidates += [f'/var/run/ceph/ceph-client.{ceph_id}.asok']
        candidates += sorted(glob.glob(f'/var/run/ceph/ceph-client.{ceph_id}.*.asok'))
    candidates += sorted(glob.glob('./out/radosgw.*.asok'))
    candidates += sorted(glob.glob('/var/run/ceph/ceph-client.rgw.*.asok'))
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def find_log_file(asok):
    if os.environ.get('RGW_LOG_FILE'):
        return os.environ['RGW_LOG_FILE']
    if asok:
        p = run(tool('ceph'), '--admin-daemon', asok, 'config', 'get', 'log_file',
                check=False)
        if p.returncode == 0:
            path = json.loads(p.stdout).get('log_file')
            if path and os.path.exists(path):
                return path
    logs = sorted(glob.glob('./out/radosgw.*.log'))
    if logs:
        return logs[0]
    raise RuntimeError('cannot find the radosgw log; set RGW_LOG_FILE')


def find_endpoint():
    if os.environ.get('RGW_ENDPOINT'):
        return os.environ['RGW_ENDPOINT'].rstrip('/')
    for ep in ('http://localhost:80', 'http://localhost:8000'):
        try:
            requests.get(ep, timeout=5)
            return ep
        except requests.exceptions.ConnectionError:
            continue
    raise RuntimeError('no radosgw on localhost:80 or :8000; set RGW_ENDPOINT')


def signed(endpoint, method, path, params):
    """Send a SigV4-signed request as the admin user."""
    url = endpoint + path
    req = botocore.awsrequest.AWSRequest(method=method, url=url, params=params)
    creds = botocore.credentials.Credentials(ADMIN_ACCESS, ADMIN_SECRET)
    botocore.auth.S3SigV4Auth(creds, 's3', 'us-east-1').add_auth(req)
    prepared = req.prepare()
    return requests.request(method, prepared.url, headers=dict(prepared.headers),
                            timeout=60)


def req_done_line(log_file, request_id, timeout=30):
    """Return the "req done" line radosgw logged for request_id."""
    pat = re.compile(r'req done .*request_id=' + re.escape(request_id) + r'\b')
    deadline = time.time() + timeout
    while True:
        with open(log_file, errors='replace') as f:
            for line in f:
                if pat.search(line):
                    return line.strip()
        if time.time() > deadline:
            raise AssertionError(f'no "req done" line for request_id={request_id} '
                                 f'in {log_file} (debug_rgw must be >= 1)')
        time.sleep(1)


def check(endpoint, log_file, name, params, want_http, want_status_zero):
    r = signed(endpoint, 'GET', '/admin/ratelimit', params)
    rid = r.headers.get('x-amz-request-id', '')
    log(f'{name}: GET /admin/ratelimit {params} -> HTTP {r.status_code} '
        f'request_id={rid} body={r.text}')
    assert r.status_code == want_http, f'{name}: expected HTTP {want_http}'
    assert rid, f'{name}: response has no x-amz-request-id'
    line = req_done_line(log_file, rid)
    log(f'  log: {line}')
    m = re.search(r'\bstatus=(-?\d+) http_status=(\d+)', line)
    assert m, f'{name}: cannot parse status from: {line}'
    status, http_status = int(m.group(1)), int(m.group(2))
    assert http_status == want_http, f'{name}: logged http_status={http_status}'
    if want_status_zero:
        assert status == 0, (f'{name}: answered HTTP {want_http} but radosgw recorded '
                             f'op status {status} (tracker #82558)')
    else:
        assert status != 0, f'{name}: expected a nonzero op status, got {status}'
    return r


def main():
    endpoint = find_endpoint()
    asok = find_admin_socket()
    log_file = find_log_file(asok)
    log(f'endpoint={endpoint} admin_socket={asok} log_file={log_file}')

    p = rgwadmin('user', 'create', f'--uid={ADMIN_UID}', '--display-name=ratelimit status admin',
                 f'--access-key={ADMIN_ACCESS}', f'--secret={ADMIN_SECRET}', check=False)
    if p.returncode not in (0, 17):  # EEXIST
        raise RuntimeError(f'admin user create failed: {p.stderr}')
    rgwadmin('caps', 'add', f'--uid={ADMIN_UID}', '--caps=ratelimit=read, write')
    for uid in (PLAIN_UID, LIMITED_UID):
        rgwadmin('user', 'create', f'--uid={uid}', f'--display-name={uid}')
    rgwadmin('ratelimit', 'set', '--ratelimit-scope=user', f'--uid={LIMITED_UID}',
             '--max-read-ops=7')
    rgwadmin('ratelimit', 'enable', '--ratelimit-scope=user', f'--uid={LIMITED_UID}')

    r = signed(endpoint, 'PUT', f'/{BUCKET}', {})
    assert r.status_code == 200, f'create bucket: HTTP {r.status_code} {r.text}'

    def check_user_body(r):
        info = r.json()['user_ratelimit']
        assert info['enabled'] is True and info['max_read_ops'] == 7, info

    cases = [
        # sanity check of the log parsing: a request that really fails
        ('user does not exist',
         {'ratelimit-scope': 'user', 'uid': f'no-such-user-{SUFFIX}'}, 404, False, None),
        ('bucket scope',
         {'ratelimit-scope': 'bucket', 'bucket': BUCKET}, 200, True, None),
        ('user scope, no stored limit',
         {'ratelimit-scope': 'user', 'uid': PLAIN_UID}, 200, True, None),
        ('user scope, stored limit',
         {'ratelimit-scope': 'user', 'uid': LIMITED_UID}, 200, True, check_user_body),
        # Without a realm and with no global ratelimit/quota ever stored,
        # this exercises the -ENOENT path; otherwise the stored-config path.
        ('global',
         {'global': 'true'}, 200, True, None),
    ]
    failures = []
    try:
        for name, params, want_http, want_zero, extra in cases:
            try:
                r = check(endpoint, log_file, name, params, want_http, want_zero)
                if extra:
                    extra(r)
            except AssertionError as e:
                log(f'  FAIL: {e}')
                failures.append(str(e))
    finally:
        signed(endpoint, 'DELETE', f'/{BUCKET}', {})
        for uid in (PLAIN_UID, LIMITED_UID):
            rgwadmin('user', 'rm', f'--uid={uid}', check=False)

    if failures:
        log('FAILED:\n  ' + '\n  '.join(failures))
        return 1
    log('PASS')
    return 0


if __name__ == '__main__':
    sys.exit(main())
