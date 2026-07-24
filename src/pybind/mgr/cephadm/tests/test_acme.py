import json

from cephadm.acme import ACMEManager
from cephadm.tlsobject_types import TLSCredentials


class FakeCertMgr:
    def __init__(self):
        self.saved = []

    def save_acme_cert_key_pair(self, service_name, tls_creds):
        assert isinstance(tls_creds, TLSCredentials)
        self.saved.append((service_name, tls_creds))

    def get_associated_service(self, cert_info):
        return 'mgmt-gateway'


class FakeMgr:
    def __init__(self):
        self.store = {}
        self.cert_mgr = FakeCertMgr()
        self.acme_profiles = ''

    def get_store(self, key):
        return self.store.get(key)

    def set_store(self, key, value):
        if value is None:
            self.store.pop(key, None)
        else:
            self.store[key] = value

    def get_store_prefix(self, prefix):
        return {k: v for k, v in self.store.items() if k.startswith(prefix)}


class FakeACMEManager(ACMEManager):
    def __init__(self, mgr):
        super().__init__(mgr)
        self.nonce = 0
        self.order_gets = 0

    def _http_request(self, url, method='GET', data=None, headers=None):
        if url == 'https://ca.example/directory':
            return 200, {}, json.dumps({
                'newNonce': 'https://ca.example/new-nonce',
                'newAccount': 'https://ca.example/new-account',
                'newOrder': 'https://ca.example/new-order',
            }).encode('utf-8')
        if url == 'https://ca.example/new-nonce':
            self.nonce += 1
            return 200, {'Replay-Nonce': f'nonce-{self.nonce}'}, b''
        if url == 'https://ca.example/new-account':
            return 201, {'Location': 'https://ca.example/acct/1'}, b'{"status":"valid"}'
        if url == 'https://ca.example/new-order':
            return 201, {'Location': 'https://ca.example/order/1'}, json.dumps({
                'status': 'pending',
                'authorizations': ['https://ca.example/authz/1'],
                'finalize': 'https://ca.example/finalize/1',
            }).encode('utf-8')
        if url == 'https://ca.example/authz/1':
            return 200, {}, json.dumps({
                'status': 'pending',
                'identifier': {'type': 'dns', 'value': 'ceph.example.com'},
                'challenges': [{
                    'type': 'http-01',
                    'token': 'tok123',
                    'url': 'https://ca.example/challenge/1',
                }],
            }).encode('utf-8')
        if url == 'https://ca.example/challenge/1':
            return 200, {}, b'{"status":"pending"}'
        if url == 'https://ca.example/order/1':
            self.order_gets += 1
            return 200, {}, json.dumps({
                'status': 'ready' if self.order_gets == 1 else 'valid',
                'authorizations': ['https://ca.example/authz/1'],
                'finalize': 'https://ca.example/finalize/1',
                'certificate': 'https://ca.example/cert/1',
            }).encode('utf-8')
        if url == 'https://ca.example/finalize/1':
            return 200, {}, json.dumps({
                'status': 'valid',
                'certificate': 'https://ca.example/cert/1',
            }).encode('utf-8')
        if url == 'https://ca.example/cert/1':
            return 200, {'Content-Type': 'application/pem-certificate-chain'}, (
                '-----BEGIN CERTIFICATE-----\n'
                'MIIB\n'
                '-----END CERTIFICATE-----\n'
            ).encode('utf-8')
        raise AssertionError(f'unexpected ACME request: {method} {url}')


def test_acme_order_flow_stores_challenge_then_certificate():
    mgr = FakeMgr()
    acme_mgr = FakeACMEManager(mgr)

    acme_cfg = {
        'domains': ['ceph.example.com'],
        'directory_url': 'https://ca.example/directory',
        'email': 'admin@example.com',
        'terms_of_service_agreed': True,
    }

    acme_mgr.ensure_certificate(
        'mgmt-gateway',
        'acme_mgmt-gateway_cert',
        'acme_mgmt-gateway_key',
        acme_cfg,
    )

    assert acme_mgr.process_pending_orders() == []
    assert acme_mgr.get_http01_key_authorization('tok123').startswith('tok123.')

    assert acme_mgr.process_pending_orders() == ['mgmt-gateway']
    assert mgr.cert_mgr.saved[0][0] == 'mgmt-gateway'
    assert 'BEGIN CERTIFICATE' in mgr.cert_mgr.saved[0][1].cert
    assert 'BEGIN RSA PRIVATE KEY' in mgr.cert_mgr.saved[0][1].key
    assert acme_mgr.get_http01_key_authorization('tok123') is None


def test_acme_config_uses_global_profile_defaults():
    mgr = FakeMgr()
    mgr.acme_profiles = json.dumps({
        'profiles': {
            'default': {
                'directory_url': 'https://ca.example/directory',
                'email': 'global@example.com',
                'terms_of_service_agreed': True,
                'account_name': 'global-account',
            }
        }
    })
    acme_mgr = FakeACMEManager(mgr)

    acme_mgr.ensure_certificate(
        'mgmt-gateway',
        'acme_mgmt-gateway_cert',
        'acme_mgmt-gateway_key',
        {'domains': ['Ceph.EXAMPLE.com']},
    )

    order = json.loads(mgr.store[acme_mgr.ORDER_STORE_PREFIX + 'mgmt-gateway'])
    assert order['domains'] == ['ceph.example.com']
    assert order['profile'] == 'default'
    assert order['directory_url'] == 'https://ca.example/directory'
    assert order['email'] == 'global@example.com'
    assert order['account_name'] == 'global-account'


def test_acme_config_service_overrides_global_profile():
    mgr = FakeMgr()
    mgr.acme_profiles = json.dumps({
        'profiles': {
            'default': {
                'directory_url': 'https://ca.example/directory',
                'email': 'global@example.com',
                'terms_of_service_agreed': True,
                'account_name': 'global-account',
            }
        }
    })
    acme_mgr = FakeACMEManager(mgr)

    acme_mgr.ensure_certificate(
        'mgmt-gateway',
        'acme_mgmt-gateway_cert',
        'acme_mgmt-gateway_key',
        {
            'domains': ['ceph.example.com'],
            'email': 'service@example.com',
            'account_name': 'service-account',
        },
    )

    order = json.loads(mgr.store[acme_mgr.ORDER_STORE_PREFIX + 'mgmt-gateway'])
    assert order['directory_url'] == 'https://ca.example/directory'
    assert order['email'] == 'service@example.com'
    assert order['account_name'] == 'service-account'
