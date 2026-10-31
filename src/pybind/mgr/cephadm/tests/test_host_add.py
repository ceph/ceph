import asyncio
from unittest import mock

import pytest

from cephadm.ssh import HostConnectionError
from orchestrator import HostSpec, OrchestratorError
from .fixtures import wait


ADDRS = {f'host{i}': f'10.0.0.{i}' for i in range(1, 8)}


def _resolve(addr):
    return addr


def _run_cephadm(failing=None, timing_out=None, unreachable=None, hanging=None,
                 delay=0.0, stats=None):
    """Fake check-host: fails for hosts in `failing`, times out for `timing_out`,
    cannot connect to `unreachable` (as ssh.py does) and never ends for `hanging`."""
    failing = failing or set()
    timing_out = timing_out or set()
    unreachable = unreachable or set()
    hanging = hanging or set()

    async def run(self, host, entity, cmd, args, **kwargs):
        assert cmd == 'check-host'
        if stats is not None:
            stats['calls'].append(host)
            stats['running'] += 1
            stats['max_running'] = max(stats['max_running'], stats['running'])
        try:
            await asyncio.sleep(delay)
            if host in hanging:
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    if stats is not None:
                        stats['cancelled'].append(host)
                    raise
            if host in timing_out:
                raise asyncio.TimeoutError()
            if host in unreachable:
                self.mgr.offline_hosts.add(host)
                raise HostConnectionError(f'Failed to connect to {host}', host, ADDRS[host])
            if host in failing:
                return [''], ['ERROR: hostname "other" does not match expected hostname'], 1
            return [''], [''], 0
        finally:
            if stats is not None:
                stats['running'] -= 1
    return run


def _specs(*hosts):
    return [HostSpec(h, ADDRS[h]) for h in hosts]


@mock.patch('cephadm.utils.resolve_ip', side_effect=_resolve)
class TestApplyHosts:

    def test_hosts_are_checked_concurrently(self, _resolve_ip, cephadm_module):
        """Every host is checked once, with at most max_parallel_host_checks at a time."""
        stats = {'calls': [], 'running': 0, 'max_running': 0}
        hosts = [f'host{i}' for i in range(1, 8)]
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(delay=0.01, stats=stats)), \
                mock.patch.object(cephadm_module, 'max_parallel_host_checks', 3), \
                mock.patch.object(cephadm_module, '_check_valid_addr') as one_by_one:
            results = wait(cephadm_module, cephadm_module.apply(_specs(*hosts)))
        assert results == [f"Added host '{h}' with addr '{ADDRS[h]}'" for h in hosts]
        assert sorted(stats['calls']) == sorted(hosts)
        assert stats['max_running'] == 3
        one_by_one.assert_not_called()
        assert set(cephadm_module.inventory.keys()) == set(hosts)

    def test_max_parallel_host_checks_option(self, _resolve_ip, cephadm_module):
        """The option defaults to 10 and limits how many hosts are checked at once."""
        assert cephadm_module.max_parallel_host_checks == 10
        cephadm_module.set_module_option('max_parallel_host_checks', 2)
        cephadm_module.config_notify()
        stats = {'calls': [], 'running': 0, 'max_running': 0}
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(delay=0.01, stats=stats)):
            wait(cephadm_module, cephadm_module.apply(_specs('host1', 'host2', 'host3', 'host4')))
        assert stats['max_running'] == 2
        assert len(stats['calls']) == 4

    def test_failed_check_only_affects_its_host(self, _resolve_ip, cephadm_module):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(failing={'host2'})):
            results = wait(cephadm_module, cephadm_module.apply(_specs('host1', 'host2', 'host3')))
        assert results[0] == "Added host 'host1' with addr '10.0.0.1'"
        assert results[1].startswith('Failed to apply spec for')
        assert "Host host2 (10.0.0.2) failed check(s)" in results[1]
        assert results[2] == "Added host 'host3' with addr '10.0.0.3'"
        assert 'host2' not in cephadm_module.inventory
        assert 'host2' not in cephadm_module.offline_hosts

    def test_failed_check_stops_apply_without_continue_on_error(self, _resolve_ip, cephadm_module):
        """apply() stops at the first failed host, but all hosts were checked
        up front, including the ones after it (unlike the sequential checks)."""
        stats = {'calls': [], 'running': 0, 'max_running': 0}
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(failing={'host2'}, stats=stats)):
            specs = _specs('host1', 'host2', 'host3')
            with pytest.raises(OrchestratorError, match='host2'):
                wait(cephadm_module, cephadm_module.apply(specs, continue_on_error=False))
        assert sorted(stats['calls']) == ['host1', 'host2', 'host3']
        assert 'host1' in cephadm_module.inventory
        assert 'host2' not in cephadm_module.inventory
        assert 'host3' not in cephadm_module.inventory

    def test_unreachable_hosts_are_not_left_offline(self, _resolve_ip, cephadm_module):
        """A new host whose check failed to connect is not left in offline_hosts,
        even when apply() stops before reaching it."""
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(unreachable={'host2', 'host3'})):
            specs = _specs('host1', 'host2', 'host3')
            with pytest.raises(OrchestratorError, match='host2'):
                wait(cephadm_module, cephadm_module.apply(specs, continue_on_error=False))
        assert set(cephadm_module.inventory.keys()) == {'host1'}
        assert not cephadm_module.offline_hosts

    def test_unreachable_known_host_stays_offline(self, _resolve_ip, cephadm_module):
        """A host already in the inventory that failed to connect stays offline, as before."""
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm()):
            wait(cephadm_module, cephadm_module.apply(_specs('host1')))
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(unreachable={'host1'})):
            results = wait(cephadm_module, cephadm_module.apply(_specs('host1', 'host2')))
        assert 'Failed to connect to host1' in results[0]
        assert results[1] == "Added host 'host2' with addr '10.0.0.2'"
        assert cephadm_module.offline_hosts == {'host1'}

    def test_timed_out_check_is_reported_as_before(self, _resolve_ip, cephadm_module):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(timing_out={'host3'})):
            results = wait(cephadm_module, cephadm_module.apply(_specs('host1', 'host2', 'host3')))
        assert 'Command "cephadm check-host --expect-hostname host3" timed out on host host3' in results[2]
        assert results[0].startswith('Added host') and results[1].startswith('Added host')
        assert cephadm_module.health_checks['CEPHADM_HOST_TIMEOUT_ERROR']['detail'] == ['host3']

    def test_hanging_check_is_cancelled_by_its_timeout(self, _resolve_ip, cephadm_module):
        """A check that never ends is cancelled when its own timeout expires,
        and the other hosts are still added."""
        wait_for = asyncio.wait_for

        def short_wait_for(aw, timeout):
            return wait_for(aw, 0.1)

        stats = {'calls': [], 'running': 0, 'max_running': 0, 'cancelled': []}
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm',
                        _run_cephadm(hanging={'host2'}, stats=stats)), \
                mock.patch('asyncio.wait_for', short_wait_for):
            results = wait(cephadm_module, cephadm_module.apply(_specs('host1', 'host2', 'host3')))
        assert stats['cancelled'] == ['host2']
        assert 'Command "cephadm check-host --expect-hostname host2" timed out on host host2' in results[1]
        assert results[0] == "Added host 'host1' with addr '10.0.0.1'"
        assert results[2] == "Added host 'host3' with addr '10.0.0.3'"
        assert cephadm_module.health_checks['CEPHADM_HOST_TIMEOUT_ERROR']['detail'] == ['host2']

    def test_batch_has_no_overall_timeout(self, _resolve_ip, cephadm_module):
        """Only each check is bounded: the batch is waited for without an overall timeout."""
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm()), \
                mock.patch.object(cephadm_module.event_loop, 'get_result',
                                  wraps=cephadm_module.event_loop.get_result) as get_result, \
                mock.patch.object(cephadm_module, '_check_valid_addr') as one_by_one:
            wait(cephadm_module, cephadm_module.apply(_specs('host1', 'host2', 'host3')))
        assert [c.args[1] for c in get_result.call_args_list] == [None]
        one_by_one.assert_not_called()

    def test_unresolvable_host_is_reported_as_before(self, _resolve_ip, cephadm_module):
        def _resolve_or_fail(addr):
            if addr == ADDRS['host2']:
                raise OrchestratorError(f'Cannot resolve ip for host {addr}')
            return addr
        _resolve_ip.side_effect = _resolve_or_fail
        with mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm()):
            results = wait(cephadm_module, cephadm_module.apply(_specs('host1', 'host2')))
        assert results[0] == "Added host 'host1' with addr '10.0.0.1'"
        assert 'Cannot resolve ip for host 10.0.0.2' in results[1]
        assert 'You may need to supply an address' in results[1]

    def test_single_host_spec_is_checked_as_before(self, _resolve_ip, cephadm_module):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm()), \
                mock.patch.object(cephadm_module, '_check_hosts') as batch:
            results = wait(cephadm_module, cephadm_module.apply(_specs('host1')))
        batch.assert_not_called()
        assert results == ["Added host 'host1' with addr '10.0.0.1'"]

    def test_repeated_hostname_is_checked_as_before(self, _resolve_ip, cephadm_module):
        """A hostname listed twice is not checked in the batch, but by _add_host() each time."""
        specs = _specs('host1', 'host2') + [HostSpec('host2', '10.0.0.22')]
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm()), \
                mock.patch.object(cephadm_module, '_check_valid_addr',
                                  wraps=cephadm_module._check_valid_addr) as one_by_one:
            results = wait(cephadm_module, cephadm_module.apply(specs))
        assert [c.args[0] for c in one_by_one.call_args_list] == ['host2', 'host2']
        assert results[2] == "Added host 'host2' with addr '10.0.0.22'"

    def test_readded_host_with_new_addr_resets_connection_once(self, _resolve_ip, cephadm_module):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm()):
            wait(cephadm_module, cephadm_module.apply(_specs('host1')))
            with mock.patch.object(cephadm_module.ssh, 'reset_con') as reset:
                results = wait(cephadm_module, cephadm_module.apply(
                    [HostSpec('host1', '10.0.0.11')] + _specs('host2')))
        reset.assert_called_once_with('host1')
        assert results[0] == "Added host 'host1' with addr '10.0.0.11'"
        assert cephadm_module.inventory.get_addr('host1') == '10.0.0.11'

    def test_no_overwrite_skips_known_hosts_and_adds_new_ones(self, _resolve_ip, cephadm_module):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm()):
            wait(cephadm_module, cephadm_module.apply(_specs('host1')))
            with mock.patch.object(cephadm_module, '_check_hosts',
                                   wraps=cephadm_module._check_hosts) as batch:
                results = wait(cephadm_module, cephadm_module.apply(
                    _specs('host1', 'host2', 'host3'), no_overwrite=True))
        assert [s.hostname for s in batch.call_args.args[0]] == ['host2', 'host3']
        assert results == [
            'Skipped host1 host spec. To change host spec omit --no-overwrite flag',
            "Added host 'host2' with addr '10.0.0.2'",
            "Added host 'host3' with addr '10.0.0.3'",
        ]
