"""RGW-specific Swift API tests that tempest doesn't cover.

Runs against the RGW of a vstart cluster at localhost:$CEPH_RGW_PORT.
Requests without an X-Auth-Token header are anonymous.
"""
import base64
import hmac
import os
import time
import unittest
import uuid
from urllib.parse import urlsplit

import requests

RGW = f'http://localhost:{os.environ.get("CEPH_RGW_PORT", 8000)}'
USER = 'test:tester'  # vstart's Swift user
KEY = 'testing'
TEMP_URL_KEY = 'tempurl-key'


def setUpModule():
    # vstart.sh doesn't wait for RGW.
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        try:
            requests.get(RGW, timeout=2)
            return
        except requests.RequestException:
            time.sleep(0.1)
    raise RuntimeError(f'{RGW} did not come up')


def login():
    """Log in with TempAuth; return the storage URL and token."""
    r = requests.get(f'{RGW}/auth/v1.0',
                     headers={'X-Auth-User': USER, 'X-Auth-Key': KEY})
    r.raise_for_status()
    return r.headers['X-Storage-Url'], r.headers['X-Auth-Token']


def new_container():
    """Create a container with a unique name; return its URL and a token."""
    url, token = login()
    container = f'{url}/test-{uuid.uuid4().hex[:8]}'
    requests.put(container, headers={'X-Auth-Token': token}).raise_for_status()
    return container, token


def set_temp_url_key(container, token):
    """Set the account's TempURL key."""
    requests.post(container.rsplit('/', 1)[0], headers={
        'X-Auth-Token': token, 'X-Account-Meta-Temp-URL-Key': TEMP_URL_KEY,
    }).raise_for_status()


def named_sig(digest, msg, key=TEMP_URL_KEY):
    """An HMAC signature as <digest>:<url-safe base64>."""
    mac = hmac.new(key.encode(), msg.encode(), digest).digest()
    return f'{digest}:{base64.urlsafe_b64encode(mac).decode()}'


def hex_sig(digest, msg, key=TEMP_URL_KEY):
    """An HMAC signature as bare hex."""
    return hmac.new(key.encode(), msg.encode(), digest).hexdigest()


class SwiftTest(unittest.TestCase):

    def test_tempauth(self):
        url, token = login()
        self.assertTrue(token)
        self.assertTrue(url.startswith(RGW))

    def test_tempauth_wrong_key(self):
        r = requests.get(f'{RGW}/auth/v1.0',
                         headers={'X-Auth-User': USER, 'X-Auth-Key': 'wrong'})
        self.assertEqual(r.status_code, 401)

    def test_cors_preflight(self):
        container, token = new_container()
        origin = 'http://example.com'

        r = requests.post(container, headers={
            'X-Auth-Token': token,
            'X-Container-Meta-Access-Control-Allow-Origin': origin})
        self.assertEqual(r.status_code, 204)

        r = requests.options(f'{container}/obj', headers={
            'Origin': origin, 'Access-Control-Request-Method': 'GET'})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Access-Control-Allow-Origin'], origin)

        r = requests.options(f'{container}/obj', headers={
            'Origin': 'http://other.example',
            'Access-Control-Request-Method': 'GET'})
        self.assertEqual(r.status_code, 403)  # Swift: 401

    def test_public_read(self):
        container, token = new_container()

        r = requests.put(f'{container}/public.txt', data=b'public',
                         headers={'X-Auth-Token': token})
        self.assertEqual(r.status_code, 201)

        r = requests.get(f'{container}/public.txt')
        self.assertEqual(r.status_code, 401)

        r = requests.post(container, headers={
            'X-Auth-Token': token, 'X-Container-Read': '.r:*'})
        self.assertEqual(r.status_code, 204)

        r = requests.get(f'{container}/public.txt')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.content, b'public')

    def test_tempurl_signature(self):
        container, token = new_container()
        set_temp_url_key(container, token)

        r = requests.put(f'{container}/obj', data=b'obj',
                         headers={'X-Auth-Token': token})
        self.assertEqual(r.status_code, 201)

        expires = int(time.time()) + 600
        msg = f'GET\n{expires}\n{urlsplit(container).path}/obj'
        for digest in ('sha1', 'sha256', 'sha512'):
            for make_sig in (named_sig, hex_sig):
                with self.subTest(digest=digest, form=make_sig.__name__):
                    r = requests.get(f'{container}/obj', params={
                        'temp_url_sig': make_sig(digest, msg),
                        'temp_url_expires': expires})
                    self.assertEqual(r.status_code, 200)
                    self.assertEqual(r.content, b'obj')

                    r = requests.get(f'{container}/obj', params={
                        'temp_url_sig': make_sig(digest, msg, 'wrong'),
                        'temp_url_expires': expires})
                    self.assertEqual(r.status_code, 403)  # Swift: 401

        # no such digest
        r = requests.get(f'{container}/obj', params={
            'temp_url_sig': 'sha42:abc', 'temp_url_expires': expires})
        self.assertEqual(r.status_code, 403)  # Swift: 401


if __name__ == '__main__':
    unittest.main()
