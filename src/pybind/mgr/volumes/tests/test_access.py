import errno
import json

import pytest

from tests import mock  # noqa: F401 -- sets up ceph module mocks

from volumes.fs.exception import VolumeException
from volumes.fs.operations.access import allow_access, deny_access


ENTITY = 'client.alice'
EXISTING = [{
    'entity': ENTITY,
    'key': 'AQBY0/pViX/wBBAAUpPs9swy7rey1qPhzmDVGQ==',
    'caps': {'mds': 'allow rw path=/a', 'osd': 'allow rw pool=a',
             'mon': 'allow r'},
}]


class FakeMgr:
    """Answer mon commands from a table keyed by prefix, recording them."""

    def __init__(self, results):
        self.results = results
        self.sent = []

    def mon_command(self, cmd):
        self.sent.append(cmd['prefix'])
        return self.results.get(cmd['prefix'], (0, '', ''))


def test_allow_access_auth_caps_failure():
    # 'auth get' would still return the old key; the caller must not be
    # told that access was granted when 'auth caps' did not apply.
    mgr = FakeMgr({
        'auth caps': (-errno.EACCES, '', 'access denied'),
        'auth get': (0, json.dumps(EXISTING), ''),
    })
    with pytest.raises(VolumeException) as e:
        allow_access(mgr, ENTITY, 'allow rw path=/b', 'allow rw pool=b',
                     None, None, EXISTING)
    assert e.value.errno == -errno.EACCES
    assert mgr.sent == ['auth caps']


def test_allow_access_get_or_create_failure():
    mgr = FakeMgr({'auth get-or-create': (-errno.EINVAL, '', 'bad caps')})
    with pytest.raises(VolumeException) as e:
        allow_access(mgr, ENTITY, 'allow rw path=/b', 'allow rw pool=b',
                     None, None, None)
    assert e.value.errno == -errno.EINVAL


def test_allow_access_existing_caps():
    mgr = FakeMgr({'auth get': (0, json.dumps(EXISTING), '')})
    key = allow_access(mgr, ENTITY, 'allow rw path=/b', 'allow rw pool=b',
                       None, None, EXISTING)
    assert key == EXISTING[0]['key']
    assert mgr.sent == ['auth caps', 'auth get']


def test_deny_access_entity_gone():
    mgr = FakeMgr({'auth get': (-errno.ENOENT, '', 'not found')})
    deny_access(mgr, ENTITY, ['allow rw path=/a'], ['allow rw pool=a'])
    assert mgr.sent == ['auth get']


def test_deny_access_auth_get_failure():
    mgr = FakeMgr({'auth get': (-errno.EACCES, '', 'access denied')})
    with pytest.raises(VolumeException) as e:
        deny_access(mgr, ENTITY, ['allow rw path=/a'], ['allow rw pool=a'])
    assert e.value.errno == -errno.EACCES


def test_deny_access_auth_rm_failure():
    # removing the last mds/osd caps removes the entity altogether
    mgr = FakeMgr({
        'auth get': (0, json.dumps(EXISTING), ''),
        'auth rm': (-errno.EBUSY, '', 'busy'),
    })
    with pytest.raises(VolumeException) as e:
        deny_access(mgr, ENTITY, ['allow rw path=/a'], ['allow rw pool=a'])
    assert e.value.errno == -errno.EBUSY
    assert mgr.sent == ['auth get', 'auth rm']
