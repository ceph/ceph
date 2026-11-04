import asyncio
import pytest
import json
from contextlib import contextmanager
from tests import mock
from cephadm.tuned_profiles import TunedProfileUtils, SYSCTL_DIR
from cephadm.utils import cephadmNoImage
from cephadm.inventory import TunedProfileStore
from ceph.utils import datetime_now
from ceph.deployment.service_spec import TunedProfileSpec, PlacementSpec
from cephadm.ssh import SSHManager
from orchestrator import HostSpec

from typing import List, Dict


class SaveError(Exception):
    pass


class FakeCache:
    def __init__(self,
                 hosts,
                 schedulable_hosts,
                 unreachable_hosts):
        self.hosts = hosts
        self.unreachable_hosts = [HostSpec(h) for h in unreachable_hosts]
        self.schedulable_hosts = [HostSpec(h) for h in schedulable_hosts]
        self.last_tuned_profile_update = {}
        self.tuned_profile_names = {}
        self.saved_hosts = []

    def save_host(self, host):
        self.saved_hosts.append(host)

    def get_hosts(self):
        return self.hosts

    def get_schedulable_hosts(self):
        return self.schedulable_hosts

    def get_unreachable_hosts(self):
        return self.unreachable_hosts

    def get_draining_hosts(self):
        return []

    def is_host_unreachable(self, hostname: str):
        return hostname in [h.hostname for h in self.get_unreachable_hosts()]

    def is_host_schedulable(self, hostname: str):
        return hostname in [h.hostname for h in self.get_schedulable_hosts()]

    def is_host_draining(self, hostname: str):
        return hostname in [h.hostname for h in self.get_draining_hosts()]

    @property
    def networks(self):
        return {h: {'a': {'b': ['c']}} for h in self.hosts}

    def host_needs_tuned_profile_update(self, host, profile_name):
        return profile_name == 'p2'


class FakeMgr:
    def __init__(self,
                 hosts: List[str],
                 schedulable_hosts: List[str],
                 unreachable_hosts: List[str],
                 profiles: Dict[str, TunedProfileSpec]):
        self.cache = FakeCache(hosts, schedulable_hosts, unreachable_hosts)
        self.tuned_profiles = TunedProfileStore(self)
        self.tuned_profiles.profiles = profiles
        self.ssh = SSHManager(self)
        self.offline_hosts = []
        self.log_refresh_metadata = False

    @contextmanager
    def async_timeout_handler(self, _host, _msg):
        yield

    def wait_async(self, coro, timeout=None):
        return asyncio.run(coro)

    def set_store(self, what: str, value: str):
        raise SaveError(f'{what}: {value}')

    def get_store(self, what: str):
        if what == 'tuned_profiles':
            return json.dumps({'x': TunedProfileSpec('x',
                                                     PlacementSpec(hosts=['x']),
                                                     {'x': 'x'}).to_json(),
                               'y': TunedProfileSpec('y',
                                                     PlacementSpec(hosts=['y']),
                                                     {'y': 'y'}).to_json()})
        return ''


class TestTunedProfiles:
    tspec1 = TunedProfileSpec('p1',
                              PlacementSpec(hosts=['a', 'b', 'c']),
                              {'setting1': 'value1',
                               'setting2': 'value2',
                               'setting with space': 'value with space'})
    tspec2 = TunedProfileSpec('p2',
                              PlacementSpec(hosts=['a', 'c']),
                              {'something': 'something_else',
                               'high': '5'})
    tspec3 = TunedProfileSpec('p3',
                              PlacementSpec(hosts=['c']),
                              {'wow': 'wow2',
                               'setting with space': 'value with space',
                               'down': 'low'})

    def profiles_to_calls(self, tp: TunedProfileUtils, profiles: List[TunedProfileSpec]) -> List[Dict[str, str]]:
        # this function takes a list of tuned profiles and returns a mapping from
        # profile names to the string that will be written to the actual config file on the host.
        res = []
        for p in profiles:
            p_str = tp._profile_to_str(p)
            res.append({p.profile_name: p_str})
        return res

    @mock.patch("cephadm.tuned_profiles.TunedProfileUtils._remove_stray_tuned_profiles")
    @mock.patch("cephadm.tuned_profiles.TunedProfileUtils._write_tuned_profiles")
    def test_write_all_tuned_profiles(self, _write_profiles, _rm_profiles):
        profiles = {'p1': self.tspec1, 'p2': self.tspec2, 'p3': self.tspec3}
        mgr = FakeMgr(['a', 'b', 'c'],
                      ['a', 'b', 'c'],
                      [],
                      profiles)
        tp = TunedProfileUtils(mgr)
        tp._write_all_tuned_profiles()
        # need to check that _write_tuned_profiles is correctly called with the
        # profiles that match the tuned profile placements and with the correct
        # strings that should be generated from the settings the profiles have.
        # the _profiles_to_calls helper allows us to generated the input we
        # should check against
        calls = [
            mock.call('a', self.profiles_to_calls(tp, [self.tspec1, self.tspec2])),
            mock.call('b', self.profiles_to_calls(tp, [self.tspec1])),
            mock.call('c', self.profiles_to_calls(tp, [self.tspec1, self.tspec2, self.tspec3]))
        ]
        _write_profiles.assert_has_calls(calls, any_order=True)

    @mock.patch('cephadm.tuned_profiles.CephadmServe._run_cephadm', new_callable=mock.AsyncMock)
    @mock.patch('cephadm.tuned_profiles.TunedProfileUtils._sysctl_dir_apply_system')
    @mock.patch('cephadm.tuned_profiles.TunedProfileUtils._sysctl_dir_list')
    def test_rm_stray_tuned_profiles(
            self, _sysctl_dir_list, _sysctl_dir_apply_system, _run_cephadm):
        profiles = {'p1': self.tspec1, 'p2': self.tspec2, 'p3': self.tspec3}
        # for this test, going to use host "a" and put 4 cephadm generated
        # profiles "p1" "p2", "p3" and "who" only two of which should be there ("p1", "p2")
        # as well as a file not generated by cephadm. Only the "p3" and "who"
        # profiles should be removed via cephadm remove-file. List/apply use cephadm.
        _sysctl_dir_list.return_value = '\n'.join(['p1-cephadm-tuned-profile.conf',
                                                   'p2-cephadm-tuned-profile.conf',
                                                   'p3-cephadm-tuned-profile.conf',
                                                   'who-cephadm-tuned-profile.conf',
                                                   'dont-touch-me'])
        _run_cephadm.return_value = ([''], [''], 0)
        mgr = FakeMgr(['a', 'b', 'c'],
                      ['a', 'b', 'c'],
                      [],
                      profiles)
        tp = TunedProfileUtils(mgr)
        tp._remove_stray_tuned_profiles('a', self.profiles_to_calls(tp, [self.tspec1, self.tspec2]))
        _sysctl_dir_list.assert_called_once_with('a')
        _sysctl_dir_apply_system.assert_called_once_with('a')
        rm_calls = [
            mock.call(
                'a', cephadmNoImage, 'remove-file',
                ['--path', f'{SYSCTL_DIR}/p3-cephadm-tuned-profile.conf']),
            mock.call(
                'a', cephadmNoImage, 'remove-file',
                ['--path', f'{SYSCTL_DIR}/who-cephadm-tuned-profile.conf']),
        ]
        _run_cephadm.assert_has_calls(rm_calls, any_order=True)
        assert _run_cephadm.call_count == 2

    @mock.patch('cephadm.tuned_profiles.TunedProfileUtils._sysctl_dir_apply_system')
    @mock.patch('cephadm.serve.CephadmServe._deploy_file_via_cephadm', new_callable=mock.AsyncMock)
    def test_write_tuned_profiles(self, _deploy_file_via_cephadm, _sysctl_dir_apply_system):
        profiles = {'p1': self.tspec1, 'p2': self.tspec2, 'p3': self.tspec3}
        # for this test we will use host "a" and have it so host_needs_tuned_profile_update
        # returns True for p2 and False for p1 (see FakeCache class). So we should see
        # one write for p2 and sysctl-dir --apply-system via cephadm.
        _deploy_file_via_cephadm.return_value = None
        mgr = FakeMgr(['a', 'b', 'c'],
                      ['a', 'b', 'c'],
                      [],
                      profiles)
        tp = TunedProfileUtils(mgr)
        tp._write_tuned_profiles('a', self.profiles_to_calls(tp, [self.tspec1, self.tspec2]))
        _sysctl_dir_apply_system.assert_called_once_with('a')
        _deploy_file_via_cephadm.assert_called_with(
            'a', f'{SYSCTL_DIR}/p2-cephadm-tuned-profile.conf', tp._profile_to_str(self.tspec2).encode('utf-8'))

    def test_dont_write_to_unreachable_hosts(self):
        profiles = {'p1': self.tspec1, 'p2': self.tspec2, 'p3': self.tspec3}

        # list host "a" and "b" as hosts that exist, "a" will be
        # a normal, schedulable host and "b" is considered unreachable
        mgr = FakeMgr(['a', 'b'],
                      ['a'],
                      ['b'],
                      profiles)
        tp = TunedProfileUtils(mgr)

        assert 'a' not in tp.mgr.cache.last_tuned_profile_update
        assert 'b' not in tp.mgr.cache.last_tuned_profile_update

        # with an online host, should proceed as normal. Providing
        # no actual profiles here though so the only actual action taken
        # is updating the entry in the last_tuned_profile_update dict
        tp._write_tuned_profiles('a', {})
        assert 'a' in tp.mgr.cache.last_tuned_profile_update

        # trying to write to an unreachable host should be a no-op
        # and return immediately. No entry for 'b' should be added
        # to the last_tuned_profile_update dict
        tp._write_tuned_profiles('b', {})
        assert 'b' not in tp.mgr.cache.last_tuned_profile_update

    @mock.patch("cephadm.tuned_profiles.TunedProfileUtils._remove_stray_tuned_profiles")
    @mock.patch("cephadm.tuned_profiles.TunedProfileUtils._write_tuned_profiles")
    def test_stray_scan_only_when_profile_set_changes(self, _write_profiles, _rm_profiles):
        profiles = {'p1': self.tspec1, 'p2': self.tspec2, 'p3': self.tspec3}
        mgr = FakeMgr(['a', 'b', 'c', 'd'],
                      ['a', 'b', 'c', 'd'],
                      [],
                      profiles)
        tp = TunedProfileUtils(mgr)

        # first pass: no record for any host, so every host is scanned once,
        # including "d" which has no profiles (it may have leftovers)
        tp._write_all_tuned_profiles()
        assert sorted(c.args[0] for c in _rm_profiles.call_args_list) == ['a', 'b', 'c', 'd']
        assert mgr.cache.tuned_profile_names == {
            'a': ['p1', 'p2'], 'b': ['p1'], 'c': ['p1', 'p2', 'p3'], 'd': []}
        assert sorted(mgr.cache.saved_hosts) == ['a', 'b', 'c', 'd']

        # nothing changed: no host is scanned or saved, writes still run
        _rm_profiles.reset_mock()
        _write_profiles.reset_mock()
        mgr.cache.saved_hosts = []
        tp._write_all_tuned_profiles()
        _rm_profiles.assert_not_called()
        assert _write_profiles.call_count == 4
        assert mgr.cache.saved_hosts == []

        # a settings change keeps the set of names, so no scan
        profiles['p1'] = TunedProfileSpec('p1',
                                          PlacementSpec(hosts=['a', 'b', 'c']),
                                          {'setting1': 'other'})
        tp._write_all_tuned_profiles()
        _rm_profiles.assert_not_called()

        # removing a profile only rescans the hosts it applied to
        del profiles['p2']
        tp._write_all_tuned_profiles()
        assert sorted(c.args[0] for c in _rm_profiles.call_args_list) == ['a', 'c']
        assert mgr.cache.tuned_profile_names['a'] == ['p1']
        assert mgr.cache.tuned_profile_names['c'] == ['p1', 'p3']

        # a placement change only rescans the hosts whose set changed
        _rm_profiles.reset_mock()
        profiles['p3'] = TunedProfileSpec('p3', PlacementSpec(hosts=['d']), {'x': 'y'})
        tp._write_all_tuned_profiles()
        assert sorted(c.args[0] for c in _rm_profiles.call_args_list) == ['c', 'd']

    @mock.patch("cephadm.tuned_profiles.TunedProfileUtils._remove_stray_tuned_profiles")
    @mock.patch("cephadm.tuned_profiles.TunedProfileUtils._write_tuned_profiles")
    def test_stray_scan_skips_unreachable_and_retries_failures(self, _write_profiles, _rm_profiles):
        profiles = {'p1': TunedProfileSpec('p1', PlacementSpec(host_pattern='*'), {'k': 'v'})}
        mgr = FakeMgr(['a', 'b'],
                      ['a'],
                      ['b'],
                      profiles)
        tp = TunedProfileUtils(mgr)

        # unreachable host: not scanned and not recorded, so it is scanned
        # once it comes back
        tp._write_all_tuned_profiles()
        assert [c.args[0] for c in _rm_profiles.call_args_list] == ['a']
        assert 'b' not in mgr.cache.tuned_profile_names

        # a failed write leaves no record, so the next pass scans again
        _write_profiles.side_effect = Exception('boom')
        del mgr.cache.tuned_profile_names['a']
        _rm_profiles.reset_mock()
        with pytest.raises(Exception):
            tp._reconcile_host('a', self.profiles_to_calls(tp, [self.tspec1]))
        assert 'a' not in mgr.cache.tuned_profile_names
        _write_profiles.side_effect = None
        tp._reconcile_host('a', self.profiles_to_calls(tp, [self.tspec1]))
        assert _rm_profiles.call_count == 2
        assert mgr.cache.tuned_profile_names['a'] == ['p1']

    def test_store(self):
        mgr = FakeMgr(['a', 'b', 'c'],
                      ['a', 'b', 'c'],
                      [],
                      {})
        tps = TunedProfileStore(mgr)
        save_str_p1 = 'tuned_profiles: ' + json.dumps({'p1': self.tspec1.to_json()})
        tspec1_updated = self.tspec1.copy()
        tspec1_updated.settings.update({'new-setting': 'new-value'})
        save_str_p1_updated = 'tuned_profiles: ' + json.dumps({'p1': tspec1_updated.to_json()})
        save_str_p1_updated_p2 = 'tuned_profiles: ' + \
            json.dumps({'p1': tspec1_updated.to_json(), 'p2': self.tspec2.to_json()})
        tspec2_updated = self.tspec2.copy()
        tspec2_updated.settings.pop('something')
        save_str_p1_updated_p2_updated = 'tuned_profiles: ' + \
            json.dumps({'p1': tspec1_updated.to_json(), 'p2': tspec2_updated.to_json()})
        save_str_p2_updated = 'tuned_profiles: ' + json.dumps({'p2': tspec2_updated.to_json()})
        with pytest.raises(SaveError) as e:
            tps.add_profile(self.tspec1)
        assert str(e.value) == save_str_p1
        assert 'p1' in tps
        with pytest.raises(SaveError) as e:
            tps.add_setting('p1', 'new-setting', 'new-value')
        assert str(e.value) == save_str_p1_updated
        assert 'new-setting' in tps.list_profiles()[0].settings
        with pytest.raises(SaveError) as e:
            tps.add_profile(self.tspec2)
        assert str(e.value) == save_str_p1_updated_p2
        assert 'p2' in tps
        assert 'something' in tps.list_profiles()[1].settings
        with pytest.raises(SaveError) as e:
            tps.rm_setting('p2', 'something')
        assert 'something' not in tps.list_profiles()[1].settings
        assert str(e.value) == save_str_p1_updated_p2_updated
        with pytest.raises(SaveError) as e:
            tps.rm_profile('p1')
        assert str(e.value) == save_str_p2_updated
        assert 'p1' not in tps
        assert 'p2' in tps
        assert len(tps.list_profiles()) == 1
        assert tps.list_profiles()[0].profile_name == 'p2'

        cur_last_updated = tps.last_updated('p2')
        new_last_updated = datetime_now()
        assert cur_last_updated != new_last_updated
        tps.set_last_updated('p2', new_last_updated)
        assert tps.last_updated('p2') == new_last_updated

        # check FakeMgr get_store func to see what is expected to be found in Key Store here
        tps.load()
        assert 'x' in tps
        assert 'y' in tps
        assert [p for p in tps.list_profiles() if p.profile_name == 'x'][0].settings == {'x': 'x'}
        assert [p for p in tps.list_profiles() if p.profile_name == 'y'][0].settings == {'y': 'y'}
