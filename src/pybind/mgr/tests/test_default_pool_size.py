from unittest import mock

import pytest

from mgr_module import MgrModule


def options_module(options):
    m = mock.Mock(spec=MgrModule)
    m.get_ceph_option.side_effect = lambda name: options[name]
    return m


@pytest.mark.parametrize(
    "num_zones, replica, size, expected",
    [
        (1, 0, 3, 3),   # replica 0 leaves it to the legacy osd_pool_default_size
        (1, 0, 2, 2),
        (1, 2, 3, 2),   # osd_pool_default_replica wins over the legacy name
        (2, 2, 3, 4),   # num_zones times the replicas per zone
        (2, 0, 3, 6),
        (3, 1, 3, 3),
    ])
def test_default_pool_size(num_zones, replica, size, expected):
    m = options_module({
        "osd_pool_default_num_zones": num_zones,
        "osd_pool_default_replica": replica,
        "osd_pool_default_size": size,
    })
    assert MgrModule.default_pool_size(m) == expected
