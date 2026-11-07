import sys
from unittest import mock
from unittest.mock import MagicMock

class DummyBasePyOSDMap:
    pass

class DummyBasePyOSDMapIncremental:
    pass

class DummyBasePyCRUSH:
    pass

if 'ceph_module' in sys.modules:
    cm = sys.modules['ceph_module']
else:
    cm = mock.MagicMock()
    sys.modules['ceph_module'] = cm

cm.BasePyOSDMap = DummyBasePyOSDMap
cm.BasePyOSDMapIncremental = DummyBasePyOSDMapIncremental
cm.BasePyCRUSH = DummyBasePyCRUSH

# Unload mgr_module if it was imported previously with a mock BasePyOSDMap
sys.modules.pop('mgr_module', None)
from mgr_module import OSDMap


def test_get_pools_with_dump_pools():
    osdmap = OSDMap()
    mock_pools_list = [
        {'pool': 1, 'pool_name': 'device_health_metrics', 'size': 3},
        {'pool': 2, 'pool_name': 'rbd', 'size': 3},
    ]
    osdmap._dump_pools = MagicMock(return_value=mock_pools_list)

    # First call calls _dump_pools
    pools = osdmap.get_pools()
    assert 1 in pools
    assert 2 in pools
    assert pools[1]['pool_name'] == 'device_health_metrics'
    assert pools[2]['pool_name'] == 'rbd'
    assert osdmap._dump_pools.call_count == 1

    # Second call uses cache and does not call _dump_pools again
    cached_pools = osdmap.get_pools()
    assert cached_pools is pools
    assert osdmap._dump_pools.call_count == 1


def test_get_pools_by_name_with_dump_pools():
    osdmap = OSDMap()
    mock_pools_list = [
        {'pool': 1, 'pool_name': 'device_health_metrics', 'size': 3},
        {'pool': 2, 'pool_name': 'rbd', 'size': 3},
    ]
    osdmap._dump_pools = MagicMock(return_value=mock_pools_list)

    # First call
    pools_by_name = osdmap.get_pools_by_name()
    assert 'device_health_metrics' in pools_by_name
    assert 'rbd' in pools_by_name
    assert pools_by_name['device_health_metrics']['pool'] == 1
    assert pools_by_name['rbd']['pool'] == 2
    assert osdmap._dump_pools.call_count == 1

    # Second call uses cache
    cached_pools_by_name = osdmap.get_pools_by_name()
    assert cached_pools_by_name is pools_by_name
    assert osdmap._dump_pools.call_count == 1


def test_get_pools_fallback_to_dump():
    osdmap = OSDMap()
    # No _dump_pools attribute, only _dump
    mock_dump = {
        'pools': [
            {'pool': 10, 'pool_name': 'testpool', 'size': 2}
        ]
    }
    osdmap._dump = MagicMock(return_value=mock_dump)

    pools = osdmap.get_pools()
    assert 10 in pools
    assert pools[10]['pool_name'] == 'testpool'
    assert osdmap._dump.call_count == 1

    # Cached
    pools2 = osdmap.get_pools()
    assert pools2 is pools
    assert osdmap._dump.call_count == 1


def test_get_ec_profile():
    osdmap = OSDMap()
    osdmap._get_ec_profile = MagicMock(return_value={'k': '2', 'm': '1', 'plugin': 'jerasure'})

    profile = osdmap.get_ec_profile('default')
    assert profile == {'k': '2', 'm': '1', 'plugin': 'jerasure'}
    osdmap._get_ec_profile.assert_called_once_with('default')


def test_get_ec_profile_fallback():
    osdmap = OSDMap()
    osdmap._dump = MagicMock(return_value={
        'erasure_code_profiles': {
            'ec-profile': {'k': '4', 'm': '2'}
        }
    })

    profile = osdmap.get_ec_profile('ec-profile')
    assert profile == {'k': '4', 'm': '2'}
    assert osdmap.get_ec_profile('nonexistent') is None


def test_get_require_osd_release():
    osdmap = OSDMap()
    osdmap._get_require_osd_release = MagicMock(return_value='squid')

    assert osdmap.get_require_osd_release() == 'squid'
    osdmap._get_require_osd_release.assert_called_once()


def test_get_require_osd_release_fallback():
    osdmap = OSDMap()
    osdmap._dump = MagicMock(return_value={'require_osd_release': 'reef'})

    assert osdmap.get_require_osd_release() == 'reef'
