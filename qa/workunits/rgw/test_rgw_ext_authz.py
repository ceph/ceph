#!/usr/bin/env python3
"""
radosgw-admin checks of the STS external authorizer config.

Runs against a single zone with the fake IAM from ext_authz_mock.py. The STS
decision paths (allow, deny, error, timeout, malformed) need a web identity
token and are covered by the unit tests of the provider; this workunit covers
the configuration lifecycle, validation and redaction.
"""

import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 18443
CERT = '/tmp/ext-authz-mock.pem'
CRN = ('crn:v1:bluemix:public:software-defined-storage:us-south:'
       'a/0123456789abcdef0123456789abcdef:'
       '11111111-2222-3333-4444-555555555555::')


def admin(*args, check=True):
    p = subprocess.run(['radosgw-admin', *args], capture_output=True,
                       text=True)
    if check and p.returncode != 0:
        print(p.stderr)
        raise SystemExit('radosgw-admin %s failed' % ' '.join(args))
    return p


def set_config(cfg, check=True):
    with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as f:
        json.dump(cfg, f)
    try:
        return admin('ext-authz', 'set', '--infile', f.name, check=check)
    finally:
        os.unlink(f.name)


def expect(cond, msg):
    if not cond:
        print('FAILED: ' + msg)
        sys.exit(1)
    print('ok: ' + msg)


def main():
    mock = subprocess.Popen([sys.executable,
                             os.path.join(HERE, 'ext_authz_mock.py'),
                             '--port', str(PORT), '--cert', CERT])
    try:
        for _ in range(50):
            if os.path.exists(CERT + '.key'):
                break
            time.sleep(0.2)
        with open(CERT) as f:
            ca = f.read()

        cfg = {
            'enabled': True,
            'provider': 'ibm-iam',
            'endpoint': 'https://localhost:%d' % PORT,
            'service_credential': 'test-api-key',
            'service_instance': CRN,
            'tls_ca_bundle': ca,
            'readonly_role_name': 'reader',
            'readwrite_role_name': 'writer',
        }

        expect(admin('ext-authz', 'get', check=False).returncode != 0,
               'no config before set')

        out = json.loads(set_config(cfg).stdout)
        expect(out['service_credential'] == '<redacted>',
               'set does not print the credential')
        got = json.loads(admin('ext-authz', 'get').stdout)
        expect(got['service_credential'] == '<redacted>',
               'get does not print the credential')
        expect(got['connect_timeout_secs'] == 2 and
               got['request_timeout_secs'] == 5, 'default timeouts')

        # the redacted output can be edited and submitted again
        got['enabled'] = False
        expect(set_config(got).returncode == 0,
               'redacted credential keeps the stored one')
        got = json.loads(admin('ext-authz', 'get').stdout)
        expect(got['enabled'] is False, 'update applied')
        md = json.loads(admin('metadata', 'get', 'ext_authz:default').stdout)
        expect(md['data']['service_credential'] == 'test-api-key',
               'stored credential was kept')

        for name, bad in [
                ('plain http endpoint', dict(cfg, endpoint='http://x.example')),
                ('unknown provider', dict(cfg, provider='nope')),
                ('no credential', dict(cfg, service_credential='')),
                ('zero timeout', dict(cfg, request_timeout_secs=0)),
                ('bad crn', dict(cfg, service_instance='not-a-crn')),
                ('same roles', dict(cfg, readonly_role_name='x',
                                    readwrite_role_name='x'))]:
            expect(set_config(bad, check=False).returncode != 0,
                   'rejects ' + name)
        got = json.loads(admin('ext-authz', 'get').stdout)
        expect(got['enabled'] is False, 'rejected sets leave the config as is')

        admin('ext-authz', 'rm')
        expect(admin('ext-authz', 'get', check=False).returncode != 0,
               'removed')
        print('PASSED')
    finally:
        admin('ext-authz', 'rm', check=False)
        mock.terminate()


if __name__ == '__main__':
    main()
