# Disable autopep8 for this file:

# fmt: off

import pytest

from cephadm.autotune import MemoryAutotuner
from orchestrator import DaemonDescription


@pytest.mark.parametrize("total,daemons,config,result",
    [   # noqa: E128
        (
            128 * 1024 * 1024 * 1024,
            [],
            {},
            None,
        ),
        (
            128 * 1024 * 1024 * 1024,
            [
                DaemonDescription('osd', '1', 'host1'),
                DaemonDescription('osd', '2', 'host1'),
            ],
            {},
            64 * 1024 * 1024 * 1024,
        ),
        (
            128 * 1024 * 1024 * 1024,
            [
                DaemonDescription('osd', '1', 'host1'),
                DaemonDescription('osd', '2', 'host1'),
                DaemonDescription('osd', '3', 'host1'),
            ],
            {
                'osd.3': 16 * 1024 * 1024 * 1024,
            },
            56 * 1024 * 1024 * 1024,
        ),
        (
            128 * 1024 * 1024 * 1024,
            [
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('osd', '1', 'host1'),
                DaemonDescription('osd', '2', 'host1'),
            ],
            {},
            62 * 1024 * 1024 * 1024,
        ),
        (
            128 * 1024 * 1024 * 1024,
            [
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('osd', '1', 'host1'),
                DaemonDescription('osd', '2', 'host1'),
                DaemonDescription('nvmeof', 'a', 'host1'),
            ],
            {},
            60 * 1024 * 1024 * 1024,
        ),
        (  # Taken from an actual user case
            int(32827840 * 1024 * 0.7),
            [
                DaemonDescription('crash', 'a', 'host1'),
                DaemonDescription('grafana', 'a', 'host1'),
                DaemonDescription('mds', 'a', 'host1'),
                DaemonDescription('mds', 'b', 'host1'),
                DaemonDescription('mds', 'c', 'host1'),
                DaemonDescription('mgr', 'a', 'host1'),
                DaemonDescription('mon', 'a', 'host1'),
                DaemonDescription('node-exporter', 'a', 'host1'),
                DaemonDescription('osd', '1', 'host1'),
                DaemonDescription('osd', '2', 'host1'),
                DaemonDescription('osd', '3', 'host1'),
                DaemonDescription('osd', '4', 'host1'),
                DaemonDescription('prometheus', 'a', 'host1'),
            ],
            {
                'mds.a': 4 * 1024 * 1024 * 1024,  # 4294967296
                'mds.b': 4 * 1024 * 1024 * 1024,
                'mds.c': 4 * 1024 * 1024 * 1024,
            },
            480485376,
        ),
    ])
def test_autotune(total, daemons, config, result):
    def fake_getter(who, opt):
        if opt == 'osd_memory_target_autotune':
            if who in config:
                return False
            else:
                return True
        if opt == 'osd_memory_target':
            return config.get(who, 4 * 1024 * 1024 * 1024)
        if opt == 'mds_cache_memory_limit':
            if who in config:
                return config.get(who, 16 * 1024 * 1024 * 1024)
            return 16 * 1024 * 1024 * 1024

    a = MemoryAutotuner(
        total_mem=total,
        daemons=daemons,
        config_get=fake_getter,
    )
    val, osds = a.tune()
    assert val == result
