import errno
import json
from unittest.mock import patch

import pytest

from mgr_util import CapProfiles, CapProfileError, MARKER_PREFIX, PROFILE_FALLBACKS

FALLBACKS = {'foo': {'mon': 'allow r', 'osd': 'allow rw pool=foo'}}
CAPS = ['mon', 'allow *', 'mgr', 'profile bar', 'osd', 'profile foo', 'mon', 'profile foo']


class FakeMon:
    def __init__(self, mons=('a',), mgrs=('a',), osds=('a',), known=('foo',), validate=True,
                 destroyed=(), broken=None):
        self.rows = {'mon metadata': [{'name': 'a', 'ceph_version_short': v} for v in mons],
                     'mgr metadata': [{'name': 'a', 'ceph_version_short': v} for v in mgrs],
                     'osd metadata': [{'id': i, 'ceph_version_short': v} for i, v in enumerate(osds)],
                     'osd info': [{'osd': i, 'state': ['destroyed'] if i in destroyed else []}
                                  for i in range(len(osds))]}
        for row in (r for rows in self.rows.values() for r in rows):
            if row.get('ceph_version_short') is None:  # never booted
                row.pop('ceph_version_short', None)
        self.known = set(known)
        self.validate = validate
        self.broken = broken
        self.keys = {}
        self.entities = set()
        self.sent = []

    def mon_command(self, cmd, inbuf=None):
        self.sent.append(cmd)
        prefix = cmd['prefix']
        if prefix == self.broken:
            return -errno.ETIMEDOUT, '', 'timed out'
        if prefix in self.rows:
            # like the mon, answer in text unless asked for json
            return 0, json.dumps(self.rows[prefix]) if cmd.get('format') == 'json' else 'text', ''
        if prefix == 'config-key get':
            if cmd['key'] in self.keys:
                return 0, self.keys[cmd['key']], ''
            return -errno.ENOENT, '', 'no such key'
        if prefix == 'config-key set':
            self.keys[cmd['key']] = cmd['val']
            return 0, '', ''
        if prefix == 'auth get-or-create-key':
            profile = cmd['caps'][1].split()[1]
            if self.validate and profile not in self.known:
                return -errno.EINVAL, '', f"unrecognized profile '{profile}'"
            self.entities.add(cmd['entity'])
            return 0, 'AQ==', ''
        if prefix == 'auth rm':
            # the mon says yes whether or not the entity exists
            self.entities.discard(cmd['entity'])
            return 0, '', ''
        raise AssertionError(cmd)

    def probes(self, profile):
        return [c for c in self.sent
                if c['prefix'] == 'auth get-or-create-key' and c['caps'][1] == f'profile {profile}']


@pytest.mark.parametrize(
    "mon,expected",
    [
        (FakeMon(), True),
        (FakeMon(known=()), False),
        (FakeMon(osds=('a', 'b')), False),
        # a daemon that never booted could be anything, unless it was destroyed
        (FakeMon(mons=('a', None)), False),
        (FakeMon(osds=('a', 'b'), destroyed=(1,)), True),
        # no osd yet says nothing about the ones to come
        (FakeMon(osds=()), False),
        # the mon takes any profile: it is not validating caps
        (FakeMon(validate=False), False),
    ])
def test_supported(mon, expected):
    assert CapProfiles(mon).supported('foo') is expected
    assert mon.entities == set()
    assert (MARKER_PREFIX + 'foo' in mon.keys) is expected


def test_marker_is_one_way_and_shared():
    mon = FakeMon()
    assert CapProfiles(mon).supported('foo')
    assert mon.keys == {MARKER_PREFIX + 'foo': 'a'}
    # another instance on a later mixed cluster: the decision stands
    mon.rows['osd metadata'].append({'id': 9, 'ceph_version_short': 'old'})
    before = len(mon.sent)
    assert CapProfiles(mon).supported('foo')
    assert [c['prefix'] for c in mon.sent[before:]] == ['config-key get']


@pytest.mark.parametrize("broken", ['config-key get', 'osd metadata', 'auth rm'])
def test_uncertainty_is_not_a_fallback(broken):
    mon = FakeMon(broken=broken)
    with patch.dict(PROFILE_FALLBACKS, FALLBACKS, clear=True), pytest.raises(CapProfileError):
        CapProfiles(mon).resolve(['osd', 'profile foo'])
    assert mon.keys == {}


def test_probe_never_reuses_an_entity():
    mon = FakeMon()
    mon.entities.add('client.cap-profile-probe-foo')  # a probe whose answer never came
    assert CapProfiles(mon).supported('foo')
    assert mon.entities == {'client.cap-profile-probe-foo'}


def test_profile_waits_for_the_marker():
    mon = FakeMon(broken='config-key set')
    cp = CapProfiles(mon)
    assert not cp.supported('foo')
    mon.keys[MARKER_PREFIX + 'foo'] = 'a'  # the set may have gone through regardless
    assert cp.supported('foo')


@pytest.mark.parametrize("known", [('foo',), ()])
def test_resolve(known):
    mon = FakeMon(known=known)
    with patch.dict(PROFILE_FALLBACKS, FALLBACKS, clear=True):
        resolved = CapProfiles(mon).resolve(CAPS)
    # only caps with a fallback are swapped; the rest pass through
    assert resolved == (CAPS if known else ['mon', 'allow *', 'mgr', 'profile bar',
                                            'osd', 'allow rw pool=foo', 'mon', 'allow r'])
    assert len(mon.probes('foo')) == 1  # named twice, probed once
