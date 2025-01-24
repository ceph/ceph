import re
import pytest
import json
from tests import mock

from cephadm.tlsobject_types import (Cert, PrivKey, TLSObjectException)
from cephadm.tlsobject_store import TLSOBJECT_STORE_PREFIX
from cephadm.module import CephadmOrchestrator

ceph_generated_cert_1 = """
-----BEGIN CERTIFICATE-----
MIIFFzCCAv+gAwIBAgIUHOJMW2YGSTs30hRXi8OhP9TFzuUwDQYJKoZIhvcNAQEL
BQAwJjENMAsGA1UECgwEQ2VwaDEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI1
MDEyMTE2MDY1NloXDTI4MDEyMTE2MDY1NlowGjEYMBYGA1UEAwwPMTkyLjE2OC4x
MDAuMTAwMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAj9WNrDts3RUu
8ikG2Qdi5YL2IX1P/9PLXIGM6VBK/DVz4Oh13YBAH54gzJGeBjvZxwDpATgUQuuu
AgREE+3uigzfq6sS31Ru+Gx5WU5LhrHHKOGkofD2Xa04mC0nvcXuOWanItfy1Dmc
MKcl9QywIHp+1qQw0ammYjpvqvRgBIZgg2qqa2rpu+zVHC4omJHaNxAruE6wPTpL
RF5oAIXyZ/0+cyC7YgVqxBOaVQZuH/D36aSSXN/S5ZuNiMUo5Y01hfz989T2iVw6
2KROt6FmBG55EAGNYw7MDtjaBBdkG9zuA7TkdqvgEE2ty7p035rllmu6CxuiToHR
C0w20FrYDkudhHaANB2EQspEE2JhiBnHk8OGhm09mtau9IohQCBGxkz9LJ27IeVK
rs9ElECtn9Ql1a5K6MTxH/dCGKAsH7/0Qg7OHH+2pS7TZ1yd9PhGYdR9ybephxth
288bki1cyg5bXTnhUKBSqORV+26b/dWx3dZ9v7N+oRXmoq6FyOxBP/4DWNAFPUax
gmTGrLM4pa/8IlLkDvn4poyQ4aacjk978LVBJoJlFo4r47gEl3I2WNiTU0D05oXP
aje0xXlgYFOfhZXHBfiny9hPJvpfjIF4MoQJJItfrLhrq5RZpY2QTETOj2A2lzki
wlLZ4D7YwQd0p0xuXDbVPmhCsWmF1C8CAwEAAaNJMEcwNwYDVR0RBDAwLoIVY2Vw
aC1ub2RlLTAuY2VwaC1vcmNogg9ncmFmYW5hX3NlcnZlcnOHBMCoZGQwDAYDVR0T
AQH/BAIwADANBgkqhkiG9w0BAQsFAAOCAgEARJgeYvvbuZkf+h6GGXXRVBmdY6FE
Jt+IHjtLJSmJZyqxNatAPXWHTydnwjN45fOMr2DS4r2Ll8LaUGS0fs+DBUJLSQIN
KDzqj1tnSMMZiHrOAO52m81lz6DE1c9BsOfdc82xCxbFaWqU+5KWtacLMSFAB7oV
ZezjA/G4LkUeNszv2YXw65nj7XKZbo+nn7p802aalbTxyiOpsYd9sa0FGhpczfkf
CZDZfTOhWnmn4yxHhcXI4ndIj6L3njki7/fyljROZjPZU4I3cPvPB2ihhm9H4RiC
A4XtWdZZH1uM+saneO1TGKKBYtEfR5YUnatutHhJ4V6jVUGuRiz6FL+a921dk/IU
kyiQL7MfoeVpoTYCsWM38UL/PeeaU1j47wYHBZJm65tjBsFzpqe/ljYiUHVEJdqn
Didgf03tKBS8KEUAT42EheFDnkEOk4uC16CuTmvREXSWHCfesSIdGH8tIlS6OE6Y
pAVcOecURaqpvdNNGD1tg5qKoO6Fj/In9xMfBD4vct70GIK6n1DsvZDGc29D7TBq
cMKK0xnnIgKE1wea9mNEi3CayN2zwNi4zpINVeJDqJG/fRcYCqVkG7iQFObh63wq
R0AaiVaSDXmCtB0pchTNUEOiGXlO0HUrC4YFVbRvzJRT/d3C5wG76QXbLjSfAzeS
3+7M6o3tu4UActk=
-----END CERTIFICATE-----
"""

ceph_generated_cert_2 = """
-----BEGIN CERTIFICATE-----
MIIFFzCCAv+gAwIBAgIUX4PkdhdD70oeykAelVSyhL6AdsUwDQYJKoZIhvcNAQEL
BQAwJjENMAsGA1UECgwEQ2VwaDEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI1
MDEyNDEyMjEyMloXDTI4MDEyNDEyMjEyMlowGjEYMBYGA1UEAwwPMTkyLjE2OC4x
MDAuMTAyMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxiIbWtQf48Cd
UuSFA7PWo3aYmHBkfFluxTi9u0flAaTgA2057DZOOxn3xkXATUFpcsKzGdsHZvR1
C0q0qX38Bsp+21Ctei4vsEVyZA86msxd9AvhLQF5VTyyo0Wc7702qb5dOOp9peXk
1OS+FA46Y2+J9HsF2Y5K3OPPeYx+D1hTY8tUZaus1Zpi72w/zq+zdrRa2pB2Hc9O
BSNOg+ZoU8vZm3LyQK7p5bjCa0llfFD5WIRjJsuQZ+s56ELSTg7asqT3VaiuskrI
xe+atQ+lZj/XJw3TLxoWwYAm9mRtbiOlNL2LsyZVKHlz5Dw1y3RXNbnIZWwZusUn
7aRO3kVj8ZIS+Yhz+TcLqP1p3y9mjmzywZWXrgfiRsCCUIduIczqQbCIeev7JX8D
e7QRQYmVGxEJGJZHmiNvaHUvuReHjbSo4OfogH7GZKX6TBc8pJ2Qfz8ciUaUo6Gi
yf6UdkC7KIMvtYR/YtyuJPH89yFpNi8J/DUvSJYUospBUoQE3S6TF6dIN86NJLDX
7ab+/CxZaePn7XLxxwY1uLjrxQ8J5dxVOU5twS70CK6pfOaK5XfPyUT48jXfHmTd
YLwdstMg/eN/xldAawoqWJP+u3jsQ9eNr28lzMw3enufRdaNoHC1C6ox5tqxYht0
AdcsImFW17FjluN3NnM94DBMud4qK30CAwEAAaNJMEcwNwYDVR0RBDAwLoIVY2Vw
aC1ub2RlLTIuY2VwaC1vcmNogg9ncmFmYW5hX3NlcnZlcnOHBMCoZGYwDAYDVR0T
AQH/BAIwADANBgkqhkiG9w0BAQsFAAOCAgEAXMSDznkjm7LvNqDnvOWYOSWd/mOT
X/BkfdpYc9QmEKJzaIuTaWMyMW4SQt0kZSHG7//1JSF0lBEK882QEkdJ7CUachu4
fcGlNI7zcmOYXVGEW5yBxiJFjeGNZNkkL8uE4uOg1rANf1oWalTNlEWWUNDE1eds
sGcpph1AZMvHQsFhC8q9jyqMkUSyzegK6WYkXrxl2v8mYOKGmQudvblc+mztSHQp
0XTWlowL5dzMHDjCdlFJVfbDkk73acwTTwODizWXa0+EU6JgfViMyypypW/C4/Gl
Z4kAzt8PeVkSQCA30s+63ApMAMgpuItyziBORfv3btleX4gC6zaMtGJQLQrZot69
/fCha1jzxsKbEpZ4/7E4GluSRGA3XshPqzPROITYXWjTuR0J1P8v9kzFrlj17bbj
FSY5HAaKptLHZM8h/g+vTqnAFRIrbKoTnOsnzGtZ9UrIT4BxhNZo3m/8skEXbpau
IfADqfsEQkm09WC1hUn+cSp2AIveqkTQnivL+N7vkiv7tFEeACKEYe3rXUuTM2fq
cYVn8aLcRMIDqOR09aOnLDkqfbnoQEeIALnEZP2sn58hmbP7gfjQjRF/+J2evn7l
MqnoatrcAJsFLNw0hvbQNNBDXzjn4tZ0GQondRp0M2Eqd0qOxQYbrfTe8Zj3g49E
YHhohGcnp4nmo1g=
-----END CERTIFICATE-----
"""

ceph_generated_key_4096 = """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""

ceph_generated_key_2048 = """
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCswSOXU7cXhku8
5dp8u41UBg5d/3/biWI2nIt7B/XKvZhDS2JlR9sg0/sMW2NxzOOM8o8R1orc8n+F
jJEivYkBJOc2UwlpHsCQ8Y6bHtQdmoLQge0TWovODzW56e6cFbqkAdEuA1z+CaWj
CkJ8hQH0YP4cNwRaqcDfYClH3iidCnPS2egfOeIJiyyAfsFvgnsJPeYrb3ajgRZL
JlNY9tpkKrPym2Gr2fD0U6hbJeDSASs6vhz6l1uoiWKyJ6aNlfhhS90F7WYXYCbY
3zxnkxDUadCbGhp3g2/kdzaPzDkTP4hQ6wWbtTLMZvJo67SkhtOzdA2N+KSDY41D
Gwz2u3WlAgMBAAECgf9AaydkRoSJj+z85fRYGKuavR2KtJRHq30HzJNKJhAy2IAp
0vJ7ltm9AZ3DOO5mCwD2tXxVZZd2LvcPrlsOD3ffzfa4raCZpgVjuGDrKFi1djcq
JtKjPYLkez2hPV8y1Od1VJqYUtAfTSrZPr8V/oMeifjVUYdOICYuOUsEC3EZk8wx
ZxtXLqqp5vlrwP0ZWueMB2GMvZmlysfh0uMIZmRGlQxWPILMZruhH0xwZIMXVnAd
Igs8y+CIsa6fNgWrou1k07XChOLWgo9elzJfBwETSeERVuYNuF5CtTPzWeI9MDrh
6BHWNnb9bZe/yjy8Py1TA4qPamzPGipgSOJKHQECgYEA56sv10J5iHXSwTscbKp8
YbG77pGkk6eWBRhCt8fnng+ShK0Mt2rO2YdvGpsSnQZooSfBsmWgkYoUEg2TPb1w
8N9NU5L8Wqsr4h07QnzPq6ybW7+KNhiCFBjWIzih8cKJJP2mw45KvOof0IQsunXA
EiPfGx4Ezfgwf0/rG6BWgmUCgYEAvuXw18pxHfYJbbR5x9vTIxfxwq8gTJekm0CI
bifo90T8miNgPxlCZRzUc0jYZfqei/UxoKqjwwtkcGN5TyOTBs7FvHpfU0ixPdYN
ud1fXxqk6KjZNqgFYnK4/9WThAfoFSjRfEtegylSJzGyBIg+biJbjDsNdPyvuBcc
eS8AUkECgYBDRttlz9AraMwDgX8Hr5rfZBYHehQpLQnMAPefF1aT+EG8deSzfzkC
wuno+A+3uhy4cCs3+3tdyJN7iqWv0Ev0J2T1WEIgsoTT7VlAPa6xVDbptf4VJ5je
7TeCkve0laHuNBsxvyjFI9iJXVj/7SISIoiv/0+14NV0o2jLZQy6YQKBgQC3u45r
0z++K4D1or+XWX9EhpY839tBfK6Ecr8c7rKt0ysgm63V7VTXBsF/1/vXYzjX0f2a
+sG1RzC7bzJhtgabhcYRWuKGwS8+Kdh6LJPPcFCKUYEGtv6/u1VNft2FNSrtuqSt
cckcile0u4LwE8WqsMzWEjwofdAOacgQ5ujzQQKBgQC0Ui8bB/Vs4+blmz5mjFdh
JllS3dRKx7Re5bY9XdkxlQd56jxyWIIyXDQ7Dk9GXaCjgGy5J7LaLCi2R1DZx28p
JhTdU/3sfz8NwTTFtR/m84rmirdecK9fYsU5wQzaqq/+IvVD6O8/46tK62YsYkQd
IwuZ9Cw+0P6sn81cI8FaeA==
-----END PRIVATE KEY-----
"""

TLSOBJECT_STORE_CERT_PREFIX = f'{TLSOBJECT_STORE_PREFIX}cert.'
TLSOBJECT_STORE_KEY_PREFIX = f'{TLSOBJECT_STORE_PREFIX}key.'


class TestCertMgr(object):

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_save_cert(self, _set_store, cephadm_module: CephadmOrchestrator):
        cephadm_module.cert_mgr._init_tlsobject_store()

        rgw_frontend_rgw_foo_host2_cert = 'fake-rgw-cert'
        nvmeof_client_cert = 'fake-nvmeof-client-cert'
        nvmeof_server_cert = 'fake-nvmeof-server-cert'
        nvmeof_root_ca_cert = 'fake-nvmeof-root-ca-cert'
        grafana_cert_host_1 = 'grafana-cert-host-1'
        grafana_cert_host_2 = 'grafana-cert-host-2'
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', rgw_frontend_rgw_foo_host2_cert, service_name='rgw.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_server_cert', nvmeof_server_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_client_cert', nvmeof_client_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_root_ca_cert', nvmeof_root_ca_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('grafana_cert', grafana_cert_host_1, host='host-1', user_made=True)
        cephadm_module.cert_mgr.save_cert('grafana_cert', grafana_cert_host_2, host='host-2', user_made=True)

        expected_calls = [
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}rgw_frontend_ssl_cert', json.dumps({'rgw.foo': Cert(rgw_frontend_rgw_foo_host2_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_server_cert', json.dumps({'nvmeof.foo': Cert(nvmeof_server_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_client_cert', json.dumps({'nvmeof.foo': Cert(nvmeof_client_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_root_ca_cert', json.dumps({'nvmeof.foo': Cert(nvmeof_root_ca_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}grafana_cert', json.dumps({'host-1': Cert(grafana_cert_host_1, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}grafana_cert', json.dumps({'host-1': Cert(grafana_cert_host_1, True).to_json(),
                                                                                'host-2': Cert(grafana_cert_host_2, True).to_json()}))
        ]
        _set_store.assert_has_calls(expected_calls)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_cert_ls(self, _set_store, cephadm_module: CephadmOrchestrator):
        cephadm_module.cert_mgr._init_tlsobject_store()

        def get_generated_cephadm_cert_info_1():
            return {
                "extensions": {
                    "basicConstraints": {"ca": False, "path_length": None},
                    "subjectAltName": {
                        "DNSNames": ["ceph-node-0.ceph-orch", "grafana_servers"],
                        "IPAddresses": ["192.168.100.100"],
                    },
                },
                "issuer": {"commonName": "cephadm-root", "organizationName": "Ceph"},
                "public_key": {"key_size": 4096, "key_type": "RSA"},
                "subject": {"commonName": "192.168.100.100"},
                "validity": {
                    "not_after": "2028-01-21T16:06:56",
                    "not_before": "2025-01-21T16:06:56",
                    "remaining_days": 1092,
                },
            }

        def get_generated_cephadm_cert_info_2():
            return {
                "extensions": {
                    "basicConstraints": {"ca": False, "path_length": None},
                    "subjectAltName": {
                        "DNSNames": ["ceph-node-2.ceph-orch", "grafana_servers"],
                        "IPAddresses": ["192.168.100.102"],
                    },
                },
                "issuer": {"commonName": "cephadm-root", "organizationName": "Ceph"},
                "public_key": {"key_size": 4096, "key_type": "RSA"},
                "subject": {"commonName": "192.168.100.102"},
                "validity": {
                    "not_after": "2028-01-24T12:21:22",
                    "not_before": "2025-01-24T12:21:22",
                    "remaining_days": 1094,
                },
            }

        def compare_certls_dicts(expected_ls):
            actual_ls = cephadm_module.cert_mgr.cert_ls()
            for key, value in expected_ls.items():
                if 'validity' in value:
                    validity = value['validity']
                    assert re.match(validity['not_after'], actual_ls[key]['validity']['not_after'])
                    assert re.match(validity['not_before'], actual_ls[key]['validity']['not_before'])
                else:
                    assert actual_ls[key] == value

        expected_ls = {
            'rgw_frontend_ssl_cert': {},
            'iscsi_ssl_cert': {},
            'ingress_ssl_cert': {},
            'mgmt_gw_cert': {},
            'oauth2_proxy_cert': {},
            'nvmeof_root_ca_cert': {},
            'grafana_cert': {},
            'nvmeof_client_cert': {},
            'nvmeof_server_cert': {},
            'cephadm_root_ca_cert': {
                'extensions': {
                    'basicConstraints': {'ca': True, 'path_length': None},
                    'subjectAltName': {'DNSNames': [], 'IPAddresses': ['::1']},
                },
                'issuer': {'commonName': 'cephadm-root', 'organizationName': 'Ceph'},
                'public_key': {'key_size': 4096, 'key_type': 'RSA'},
                'subject': {'commonName': 'cephadm-root', 'organizationName': 'Ceph'},
                'validity': {
                    'not_after': re.compile('.*'),
                    'not_before': re.compile('.*'),
                    'remaining_days': 3653,
                },
            },
        }

        compare_certls_dicts(expected_ls)

        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', ceph_generated_cert_1, service_name='rgw.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', ceph_generated_cert_2, service_name='rgw.bar', user_made=True)
        expected_ls['rgw_frontend_ssl_cert'] = {}
        expected_ls['rgw_frontend_ssl_cert']['rgw.foo'] = get_generated_cephadm_cert_info_1()
        expected_ls['rgw_frontend_ssl_cert']['rgw.bar'] = get_generated_cephadm_cert_info_2()
        compare_certls_dicts(expected_ls)

        cephadm_module.cert_mgr.save_cert('nvmeof_client_cert', ceph_generated_cert_1, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_server_cert', ceph_generated_cert_1, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_root_ca_cert', ceph_generated_cert_2, service_name='nvmeof.foo', user_made=True)
        expected_ls['nvmeof_client_cert'] = {}
        expected_ls['nvmeof_client_cert']['nvmeof.foo'] = get_generated_cephadm_cert_info_1()
        expected_ls['nvmeof_server_cert'] = {}
        expected_ls['nvmeof_server_cert']['nvmeof.foo'] = get_generated_cephadm_cert_info_1()
        expected_ls['nvmeof_root_ca_cert'] = {}
        expected_ls['nvmeof_root_ca_cert']['nvmeof.foo'] = get_generated_cephadm_cert_info_2()
        compare_certls_dicts(expected_ls)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_save_key(self, _set_store, cephadm_module: CephadmOrchestrator):
        cephadm_module.cert_mgr._init_tlsobject_store()

        grafana_host1_key = 'fake-grafana-host1-key'
        grafana_host2_key = 'fake-grafana-host2-key'
        nvmeof_client_key = 'nvmeof-client-key'
        nvmeof_server_key = 'nvmeof-server-key'
        nvmeof_encryption_key = 'nvmeof-encryption-key'
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host1_key, host='host1')
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host2_key, host='host2')
        cephadm_module.cert_mgr.save_key('nvmeof_client_key', nvmeof_client_key, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_server_key', nvmeof_server_key, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_encryption_key', nvmeof_encryption_key, service_name='nvmeof.foo')

        expected_calls = [
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}grafana_key', json.dumps({'host1': PrivKey(grafana_host1_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}grafana_key', json.dumps({'host1': PrivKey(grafana_host1_key).to_json(),
                                                                              'host2': PrivKey(grafana_host2_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_client_key', json.dumps({'nvmeof.foo': PrivKey(nvmeof_client_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_server_key', json.dumps({'nvmeof.foo': PrivKey(nvmeof_server_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_encryption_key', json.dumps({'nvmeof.foo': PrivKey(nvmeof_encryption_key).to_json()})),
        ]
        _set_store.assert_has_calls(expected_calls)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_key_ls(self, _set_store, cephadm_module: CephadmOrchestrator):
        cephadm_module.cert_mgr._init_tlsobject_store()

        expected_ls = {
            'grafana_key': {},
            'mgmt_gw_key': {},
            'oauth2_proxy_key': {},
            'iscsi_ssl_key': {},
            'ingress_ssl_key': {},
            'nvmeof_client_key': {},
            'nvmeof_server_key': {},
            'nvmeof_encryption_key': {},
        }
        assert cephadm_module.cert_mgr.key_ls() == expected_ls

        cephadm_module.cert_mgr.save_key('nvmeof_client_key', ceph_generated_key_4096, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_server_key', ceph_generated_key_4096, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_encryption_key', ceph_generated_key_2048, service_name='nvmeof.foo')
        expected_ls['nvmeof_server_key']['nvmeof.foo'] = {'key_size': 4096, 'key_type': 'RSA'}
        expected_ls['nvmeof_client_key']['nvmeof.foo'] = {'key_size': 4096, 'key_type': 'RSA'}
        expected_ls['nvmeof_encryption_key']['nvmeof.foo'] = {'key_size': 2048, 'key_type': 'RSA'}
        assert cephadm_module.cert_mgr.key_ls() == expected_ls

        cephadm_module.cert_mgr.save_key('ingress_ssl_key', 'invalid_key', service_name='ingress.foo')
        assert 'Error parsing key' in cephadm_module.cert_mgr.key_ls()['ingress_ssl_key']['ingress.foo']['Error']


    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_tlsobject_store_load(self, _get_store_prefix, cephadm_module: CephadmOrchestrator):
        cephadm_module.cert_mgr._init_tlsobject_store()

        rgw_frontend_rgw_foo_host2_cert = 'fake-rgw-cert'
        grafana_host1_key = 'fake-grafana-host1-cert'
        nvmeof_server_cert = 'nvmeof-server-cert'
        nvmeof_client_cert = 'nvmeof-client-cert'
        nvmeof_root_ca_cert = 'nvmeof-root-ca-cert'
        nvmeof_server_key = 'nvmeof-server-key'
        nvmeof_client_key = 'nvmeof-client-key'
        nvmeof_encryption_key = 'nvmeof-encryption-key'

        def _fake_prefix_store(key):
            if key == 'cert_store.cert.':
                return {
                    f'{TLSOBJECT_STORE_CERT_PREFIX}rgw_frontend_ssl_cert': json.dumps({'rgw.foo': Cert(rgw_frontend_rgw_foo_host2_cert, True).to_json()}),
                    f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_server_cert': json.dumps({'nvmeof.foo': Cert(nvmeof_server_cert, True).to_json()}),
                    f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_client_cert': json.dumps({'nvmeof.foo': Cert(nvmeof_client_cert, True).to_json()}),
                    f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_root_ca_cert': json.dumps({'nvmeof.foo': Cert(nvmeof_root_ca_cert, True).to_json()}),
                }
            elif key == 'cert_store.key.':
                return {
                    f'{TLSOBJECT_STORE_KEY_PREFIX}grafana_key': json.dumps({'host1': PrivKey(grafana_host1_key).to_json()}),
                    f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_server_key': json.dumps({'nvmeof.foo': PrivKey(nvmeof_server_key).to_json()}),
                    f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_client_key': json.dumps({'nvmeof.foo': PrivKey(nvmeof_client_key).to_json()}),
                    f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_encryption_key': json.dumps({'nvmeof.foo': PrivKey(nvmeof_encryption_key).to_json()}),
                }
            else:
                raise Exception(f'Get store with unexpected value {key}')

        _get_store_prefix.side_effect = _fake_prefix_store
        cephadm_module.cert_mgr.load()
        assert cephadm_module.cert_mgr.cert_store.known_entities['rgw_frontend_ssl_cert']['rgw.foo'] == Cert(rgw_frontend_rgw_foo_host2_cert, True)
        assert cephadm_module.cert_mgr.cert_store.known_entities['nvmeof_server_cert']['nvmeof.foo'] == Cert(nvmeof_server_cert, True)
        assert cephadm_module.cert_mgr.cert_store.known_entities['nvmeof_client_cert']['nvmeof.foo'] == Cert(nvmeof_client_cert, True)
        assert cephadm_module.cert_mgr.cert_store.known_entities['nvmeof_root_ca_cert']['nvmeof.foo'] == Cert(nvmeof_root_ca_cert, True)
        assert cephadm_module.cert_mgr.key_store.known_entities['grafana_key']['host1'] == PrivKey(grafana_host1_key)
        assert cephadm_module.cert_mgr.key_store.known_entities['nvmeof_server_key']['nvmeof.foo'] == PrivKey(nvmeof_server_key)
        assert cephadm_module.cert_mgr.key_store.known_entities['nvmeof_client_key']['nvmeof.foo'] == PrivKey(nvmeof_client_key)
        assert cephadm_module.cert_mgr.key_store.known_entities['nvmeof_encryption_key']['nvmeof.foo'] == PrivKey(nvmeof_encryption_key)

    def test_tlsobject_store_get_cert_key(self, cephadm_module: CephadmOrchestrator):
        cephadm_module.cert_mgr._init_tlsobject_store()

        rgw_frontend_rgw_foo_host2_cert = 'fake-rgw-cert'
        nvmeof_client_cert = 'fake-nvmeof-client-cert'
        nvmeof_server_cert = 'fake-nvmeof-server-cert'
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', rgw_frontend_rgw_foo_host2_cert, service_name='rgw.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_server_cert', nvmeof_server_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_client_cert', nvmeof_client_cert, service_name='nvmeof.foo', user_made=True)

        assert cephadm_module.cert_mgr.get_cert('rgw_frontend_ssl_cert', service_name='rgw.foo') == rgw_frontend_rgw_foo_host2_cert
        assert cephadm_module.cert_mgr.get_cert('nvmeof_server_cert', service_name='nvmeof.foo') == nvmeof_server_cert
        assert cephadm_module.cert_mgr.get_cert('nvmeof_client_cert', service_name='nvmeof.foo') == nvmeof_client_cert
        assert cephadm_module.cert_mgr.get_cert('grafana_cert', host='host1') is None
        assert cephadm_module.cert_mgr.get_cert('iscsi_ssl_cert', service_name='iscsi.foo') is None
        assert cephadm_module.cert_mgr.get_cert('nvmeof_root_ca_cert', service_name='nvmeof.foo') is None

        with pytest.raises(TLSObjectException, match='Attempted to access cert for unknown entity'):
            cephadm_module.cert_mgr.get_cert('unknown_entity')
        with pytest.raises(TLSObjectException, match='Need host to access cert for entity'):
            cephadm_module.cert_mgr.get_cert('grafana_cert')
        with pytest.raises(TLSObjectException, match='Need service name to access cert for entity'):
            cephadm_module.cert_mgr.get_cert('rgw_frontend_ssl_cert', host='foo')

        grafana_host1_key = 'fake-grafana-host1-cert'
        nvmeof_server_key = 'nvmeof-server-key'
        nvmeof_encryption_key = 'nvmeof-encryption-key'
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host1_key, host='host1')
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host1_key, host='host1')
        cephadm_module.cert_mgr.save_key('nvmeof_server_key', nvmeof_server_key, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_encryption_key', nvmeof_encryption_key, service_name='nvmeof.foo')

        assert cephadm_module.cert_mgr.get_key('grafana_key', host='host1') == grafana_host1_key
        assert cephadm_module.cert_mgr.get_key('nvmeof_server_key', service_name='nvmeof.foo') == nvmeof_server_key
        assert cephadm_module.cert_mgr.get_key('nvmeof_client_key', service_name='nvmeof.foo') is None
        assert cephadm_module.cert_mgr.get_key('nvmeof_encryption_key', service_name='nvmeof.foo') == nvmeof_encryption_key

        with pytest.raises(TLSObjectException, match='Attempted to access privkey for unknown entity'):
            cephadm_module.cert_mgr.get_key('unknown_entity')
        with pytest.raises(TLSObjectException, match='Need host to access privkey for entity'):
            cephadm_module.cert_mgr.get_key('grafana_key')
