import pytest
import json
from tests import mock
from cephadm.tuned_profiles import TunedProfileUtils, SYSCTL_DIR
from cephadm.inventory import TunedProfileStore
from ceph.utils import datetime_now
from ceph.deployment.service_spec import TunedProfileSpec, PlacementSpec
from cephadm.ssh import SSHManager, RemoteCommand, Executables
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

    @mock.patch('cephadm.ssh.SSHManager.check_execute_command')
    def test_rm_stray_tuned_profiles(self, _check_execute_command):
        profiles = {'p1': self.tspec1, 'p2': self.tspec2, 'p3': self.tspec3}
        # for this test, going to use host "a" and put 4 cephadm generated
        # profiles "p1" "p2", "p3" and "who" only two of which should be there ("p1", "p2")
        # as well as a file not generated by cephadm. Only the "p3" and "who"
        # profiles should be removed from the host. This should total to 4
        # calls to check_execute_command, 1 "ls", 2 "rm", and 1 "sysctl --system"
        _check_execute_command.return_value = '\n'.join(['p1-cephadm-tuned-profile.conf',
                                                         'p2-cephadm-tuned-profile.conf',
                                                         'p3-cephadm-tuned-profile.conf',
                                                         'who-cephadm-tuned-profile.conf',
                                                         'dont-touch-me'])
        mgr = FakeMgr(['a', 'b', 'c'],
                      ['a', 'b', 'c'],
                      [],
                      profiles)
        tp = TunedProfileUtils(mgr)
        tp._remove_stray_tuned_profiles('a', self.profiles_to_calls(tp, [self.tspec1, self.tspec2]))
        calls = [
            mock.call(
                'a', RemoteCommand(Executables.LS, [SYSCTL_DIR]), log_command=False
            ),
            mock.call(
                'a',
                RemoteCommand(
                    Executables.RM,
                    ['-f', f'{SYSCTL_DIR}/p3-cephadm-tuned-profile.conf']
                )
            ),
            mock.call(
                'a',
                RemoteCommand(
                    Executables.RM,
                    ['-f', f'{SYSCTL_DIR}/who-cephadm-tuned-profile.conf']
                )
            ),
            mock.call(
                'a', RemoteCommand(Executables.SYSCTL, ['--system'])
            ),
        ]
        _check_execute_command.assert_has_calls(calls, any_order=True)

    @mock.patch('cephadm.ssh.SSHManager.check_execute_command')
    @mock.patch('cephadm.ssh.SSHManager.write_remote_file')
    def test_write_tuned_profiles(self, _write_remote_file, _check_execute_command):
        profiles = {'p1': self.tspec1, 'p2': self.tspec2, 'p3': self.tspec3}
        # for this test we will use host "a" and have it so host_needs_tuned_profile_update
        # returns True for p2 and False for p1 (see FakeCache class). So we should see
        # 2 ssh calls, one to write p2, one to run sysctl --system
        _check_execute_command.return_value = 'success'
        _write_remote_file.return_value = 'success'
        mgr = FakeMgr(['a', 'b', 'c'],
                      ['a', 'b', 'c'],
                      [],
                      profiles)
        tp = TunedProfileUtils(mgr)
        tp._write_tuned_profiles('a', self.profiles_to_calls(tp, [self.tspec1, self.tspec2]))
        _check_execute_command.assert_called_with(
            'a', RemoteCommand(Executables.SYSCTL, ['--system'])
        )
        _write_remote_file.assert_called_with(
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
