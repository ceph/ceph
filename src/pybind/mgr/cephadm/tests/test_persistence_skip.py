import datetime
import json
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from ceph.deployment.inventory import Device
from cephadm.agent_metrics import AgentMetadataStats
from cephadm.inventory import HostCache


ENTRY_SIZE = 65536


def _fake_mgr() -> MagicMock:
    # A plain stand-in instead of with_cephadm_module: these tests only touch
    # the cache objects, and a real module leaks thread pools between tests.
    mgr = MagicMock()
    mgr.get_foreign_ceph_option.return_value = ENTRY_SIZE
    mgr.agent_metadata_stats = AgentMetadataStats()
    return mgr


def _devices(paths: List[str], created: Any = None) -> List[Device]:
    return [Device(path=p, available=True, created=created) for p in paths]


def _device_writes(mgr: MagicMock) -> List[str]:
    return [c.args[0] for c in mgr.set_store.call_args_list if '.devices.' in c.args[0]]


def _skipped(mgr: MagicMock, scope: str, category: str) -> int:
    snap = mgr.agent_metadata_stats.snapshot('mgr.x')
    return snap['persistence'][scope][category]['skipped']


class TestDeviceWrites:

    def test_unchanged_devices_are_not_rewritten(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices(['/dev/sda', '/dev/sdb']))
        cache.save_host('host1')
        assert _device_writes(mgr) == ['host.host1.devices.0']

        mgr.set_store.reset_mock()
        cache.update_host_devices('host1', _devices(['/dev/sda', '/dev/sdb']))
        cache.save_host('host1')
        assert _device_writes(mgr) == []
        assert _skipped(mgr, 'background', 'devices') == 1
        # the host entry itself is still written
        assert [c.args[0] for c in mgr.set_store.call_args_list] == ['host.host1']

    def test_new_created_timestamp_alone_is_not_a_change(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        t1 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        t2 = t1 + datetime.timedelta(minutes=5)
        cache.update_host_devices('host1', _devices(['/dev/sda'], created=t1))
        cache.save_host_devices('host1')
        mgr.set_store.reset_mock()

        cache.update_host_devices('host1', _devices(['/dev/sda'], created=t2))
        cache.save_host_devices('host1')
        assert _device_writes(mgr) == []

    def test_changed_devices_are_written(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices(['/dev/sda']))
        cache.save_host_devices('host1')
        mgr.set_store.reset_mock()

        cache.update_host_devices('host1', _devices(['/dev/sda', '/dev/sdb']))
        cache.save_host_devices('host1')
        assert _device_writes(mgr) == ['host.host1.devices.0']
        stored = json.loads(mgr.set_store.call_args.args[1])
        assert [d['path'] for d in stored['devices']] == ['/dev/sda', '/dev/sdb']

        # a field change on an existing device counts too
        mgr.set_store.reset_mock()
        devs = _devices(['/dev/sda', '/dev/sdb'])
        devs[1].available = False
        cache.update_host_devices('host1', devs)
        cache.save_host_devices('host1')
        assert _device_writes(mgr) == ['host.host1.devices.0']

    def test_entry_size_change_rewrites(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices(['/dev/sda']))
        cache.save_host_devices('host1')
        mgr.set_store.reset_mock()

        mgr.get_foreign_ceph_option.return_value = ENTRY_SIZE * 2
        cache.save_host_devices('host1')
        assert _device_writes(mgr) == ['host.host1.devices.0']

    def test_failed_write_is_retried(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices(['/dev/sda']))
        mgr.set_store.side_effect = RuntimeError('mon unavailable')
        with pytest.raises(RuntimeError):
            cache.save_host_devices('host1')

        mgr.set_store.side_effect = None
        mgr.set_store.reset_mock()
        cache.save_host_devices('host1')
        assert _device_writes(mgr) == ['host.host1.devices.0']

    def test_sharded_failure_is_retried(self) -> None:
        mgr = _fake_mgr()
        mgr.get_foreign_ceph_option.return_value = 3072
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices([f'/dev/sd{c}' for c in 'abcdefghij']))

        calls = {'n': 0}

        def fail_second(key: str, value: Any) -> None:
            calls['n'] += 1
            if calls['n'] == 2:
                raise RuntimeError('mon unavailable')

        mgr.set_store.side_effect = fail_second
        with pytest.raises(RuntimeError):
            cache.save_host_devices('host1')

        mgr.set_store.side_effect = None
        mgr.set_store.reset_mock()
        cache.save_host_devices('host1')
        writes = _device_writes(mgr)
        assert len(writes) > 1
        assert writes[0] == 'host.host1.devices.0'

    def test_rm_host_forgets_the_digest(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices(['/dev/sda']))
        cache.save_host_devices('host1')
        cache.rm_host('host1')
        mgr.set_store.reset_mock()

        cache.update_host_devices('host1', _devices(['/dev/sda']))
        cache.save_host_devices('host1')
        assert _device_writes(mgr) == ['host.host1.devices.0']

    def test_hosts_are_tracked_separately(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices(['/dev/sda']))
        cache.save_host_devices('host1')
        mgr.set_store.reset_mock()

        cache.update_host_devices('host2', _devices(['/dev/sda']))
        cache.save_host_devices('host2')
        assert _device_writes(mgr) == ['host.host2.devices.0']

    def test_skips_inside_agent_request_are_attributed_to_it(self) -> None:
        mgr = _fake_mgr()
        cache = HostCache(mgr)
        cache.update_host_devices('host1', _devices(['/dev/sda']))
        cache.save_host_devices('host1')

        stats = mgr.agent_metadata_stats
        stats.begin_agent_request()
        try:
            cache.save_host_devices('host1')
        finally:
            stats.finish_agent_request()
        assert _skipped(mgr, 'agent_request', 'devices') == 1
        assert _skipped(mgr, 'background', 'devices') == 0
        assert 'skipped' in stats.format_plain('mgr.x')
