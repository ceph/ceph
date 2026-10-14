from unittest.mock import patch

from orchestrator import HostSpec

from cephadm.agent import HostData
from .fixtures import with_cephadm_module


def _setup_mgr(hosts):
    ctx = with_cephadm_module()
    mgr = ctx.__enter__()
    for i, host in enumerate(hosts):
        mgr.inventory.add_host(HostSpec(hostname=host, addr=f'1::{i + 1}'))
        mgr.cache.prime_empty_host(host)
        mgr.agent_cache.agent_keys[host] = 'key'
        mgr.agent_cache.agent_counter[host] = 1
    return ctx, mgr


def _report(mgr, host):
    return {
        'host': host, 'keyring': 'key', 'port': 1234, 'ack': '1',
        'ls': [{'name': f'crash.{host}', 'style': 'cephadm:v1', 'fsid': mgr._cluster_fsid}],
    }


def test_all_hosts_up_to_date_when_every_flag_is_set():
    ctx, mgr = _setup_mgr(['h1', 'h2'])
    try:
        for h in ('h1', 'h2'):
            mgr.cache.metadata_up_to_date[h] = True
        with patch.object(mgr.cache, 'get_unreachable_hosts',
                          wraps=mgr.cache.get_unreachable_hosts) as unreachable:
            assert mgr.cache.all_host_metadata_up_to_date() is True
        unreachable.assert_not_called()
    finally:
        ctx.__exit__(None, None, None)


def test_out_of_date_reachable_host_makes_cluster_out_of_date():
    ctx, mgr = _setup_mgr(['h1', 'h2', 'h3'])
    try:
        mgr.cache.metadata_up_to_date['h1'] = True
        mgr.cache.metadata_up_to_date['h2'] = False
        mgr.cache.metadata_up_to_date['h3'] = False
        with patch.object(mgr.cache, 'get_unreachable_hosts',
                          wraps=mgr.cache.get_unreachable_hosts) as unreachable:
            assert mgr.cache.all_host_metadata_up_to_date() is False
        assert unreachable.call_count == 1
    finally:
        ctx.__exit__(None, None, None)


def test_out_of_date_offline_hosts_are_ignored():
    ctx, mgr = _setup_mgr(['h1', 'h2', 'h3'])
    try:
        mgr.cache.metadata_up_to_date['h1'] = True
        mgr.cache.metadata_up_to_date['h2'] = False
        mgr.cache.metadata_up_to_date['h3'] = False
        mgr.offline_hosts.update({'h2', 'h3'})
        with patch.object(mgr.cache, 'get_unreachable_hosts',
                          wraps=mgr.cache.get_unreachable_hosts) as unreachable:
            assert mgr.cache.all_host_metadata_up_to_date() is True
        assert unreachable.call_count == 1
    finally:
        ctx.__exit__(None, None, None)


def test_report_from_up_to_date_host_skips_cluster_freshness_check():
    """A host that was already up to date cannot complete the set of up-to-date hosts."""
    ctx, mgr = _setup_mgr(['h1', 'h2'])
    try:
        endpoint = HostData(mgr)
        mgr.cache.metadata_up_to_date['h1'] = True
        mgr.cache.metadata_up_to_date['h2'] = False
        with patch.object(mgr.cache, 'all_host_metadata_up_to_date',
                          wraps=mgr.cache.all_host_metadata_up_to_date) as freshness:
            assert endpoint.handle_metadata(_report(mgr, 'h1')).processed is True
        freshness.assert_not_called()
    finally:
        ctx.__exit__(None, None, None)


def test_last_out_of_date_host_kicks_serve_loop():
    """The report that makes every host up to date kicks the serve loop."""
    ctx, mgr = _setup_mgr(['h1', 'h2'])
    try:
        endpoint = HostData(mgr)
        mgr.cache.metadata_up_to_date['h1'] = False
        mgr.cache.metadata_up_to_date['h2'] = False
        assert endpoint.handle_metadata(_report(mgr, 'h1')).processed is True
        assert mgr.cache.all_host_metadata_up_to_date() is False

        report = _report(mgr, 'h2')
        # The daemons of h2 are already known, so only the freshness transition can kick.
        mgr._process_ls_output('h2', report['ls'])
        with patch.object(mgr, '_kick_serve_loop') as kick:
            assert endpoint.handle_metadata(report).processed is True
        kick.assert_called_once()
        assert mgr.cache.all_host_metadata_up_to_date() is True
    finally:
        ctx.__exit__(None, None, None)
