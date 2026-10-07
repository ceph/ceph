from unittest.mock import patch

from orchestrator import HostSpec

from cephadm.agent import HostData
from .fixtures import with_cephadm_module


def _setup_mgr():
    ctx = with_cephadm_module()
    mgr = ctx.__enter__()
    host = 'delta-host'
    mgr.inventory.add_host(HostSpec(hostname=host, addr='1::4'))
    mgr.cache.prime_empty_host(host)
    mgr.agent_cache.agent_keys[host] = 'key'
    mgr.agent_cache.agent_counter[host] = 1
    return ctx, mgr, host


def _base(host):
    return {'host': host, 'keyring': 'key', 'port': 1234, 'ack': '1'}


def test_unchanged_requires_mgr_baseline_and_does_not_save_host():
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['unchanged'] = ['facts', 'networks']
        with patch.object(mgr.cache, 'save_host') as save_host:
            result = endpoint.handle_metadata(data)
        assert result.processed is True
        assert set(result.resync) == {'facts', 'networks'}
        save_host.assert_not_called()

        mgr.agent_helpers.delta_baselines.record_received(host, ['facts', 'networks'])
        with patch.object(mgr.cache, 'save_host') as save_host:
            result = endpoint.handle_metadata(data)
        assert result.processed is True
        assert result.resync == []
        save_host.assert_not_called()
        assert host in mgr.cache.last_facts_update
        assert host in mgr.cache.last_network_update
    finally:
        ctx.__exit__(None, None, None)


def test_unchanged_ls_marks_metadata_up_to_date_only_with_baseline():
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['unchanged'] = ['ls']
        mgr.cache.metadata_up_to_date[host] = False
        result = endpoint.handle_metadata(data)
        assert result.resync == ['ls']
        assert mgr.cache.metadata_up_to_date[host] is False

        mgr.agent_helpers.delta_baselines.record_received(host, ['ls'])
        result = endpoint.handle_metadata(data)
        assert result.resync == []
        assert mgr.cache.metadata_up_to_date[host] is True
    finally:
        ctx.__exit__(None, None, None)


def test_handler_failure_is_structured_false():
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['facts'] = '{bad json'
        result = endpoint.handle_metadata(data)
        assert result.processed is False
        assert 'Failed to update metadata' in result.message
    finally:
        ctx.__exit__(None, None, None)


def test_full_section_received_establishes_the_baseline():
    """A section delivered in full can later be reported as unchanged."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['facts'] = '{"hostname": "delta-host"}'
        assert endpoint.handle_metadata(data).processed is True

        data = _base(host)
        data['unchanged'] = ['facts']
        result = endpoint.handle_metadata(data)
        assert result.processed is True
        assert result.resync == []
    finally:
        ctx.__exit__(None, None, None)


def test_failed_processing_does_not_establish_a_baseline():
    """A section that fails to process is resynced if later reported unchanged."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['facts'] = '{bad json'
        assert endpoint.handle_metadata(data).processed is False

        data = _base(host)
        data['unchanged'] = ['facts']
        assert endpoint.handle_metadata(data).resync == ['facts']
    finally:
        ctx.__exit__(None, None, None)


def test_stale_ack_report_ignores_unchanged_sections():
    """Unchanged sections in a report with an old ack are neither honoured nor resynced."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        mgr.agent_helpers.delta_baselines.record_received(host, ['facts'])
        mgr.agent_cache.agent_counter[host] = 2
        mgr.cache.last_facts_update.pop(host, None)
        data = _base(host)  # ack 1
        data['unchanged'] = ['facts', 'networks']
        with patch.object(mgr.agent_helpers, '_request_agent_acks'):
            result = endpoint.handle_metadata(data)
        assert result.processed is True
        assert result.resync == []
        assert host not in mgr.cache.last_facts_update
    finally:
        ctx.__exit__(None, None, None)


def test_agent_down_and_host_back_online_forget_the_baseline():
    """Baselines are dropped when the agent is marked down or the host returns online."""
    ctx, mgr, host = _setup_mgr()
    try:
        baselines = mgr.agent_helpers.delta_baselines
        baselines.record_received(host, ['ls'])
        mgr.agent_helpers._update_agent_down_healthcheck([host])
        assert baselines.split_unchanged(host, {'ls'}) == (set(), ['ls'])

        baselines.record_received(host, ['ls'])
        mgr.offline_hosts.add(host)
        mgr.offline_hosts_remove(host)
        assert baselines.split_unchanged(host, {'ls'}) == (set(), ['ls'])
    finally:
        ctx.__exit__(None, None, None)


def test_http_response_carries_success_and_resync():
    """The /data response reports success and resync as protocol fields."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['unchanged'] = ['facts']
        with patch('cephadm.agent.cherrypy.request'), \
                patch.object(HostData, 'get_data', return_value=data):
            results = endpoint.index()
        assert results['success'] is True
        assert results['resync'] == ['facts']

        bad = _base(host)
        bad['keyring'] = 'wrong'
        with patch('cephadm.agent.cherrypy.request'), \
                patch.object(HostData, 'get_data', return_value=bad):
            results = endpoint.index()
        assert results['success'] is False
        assert results['result'].startswith('Bad metadata')
    finally:
        ctx.__exit__(None, None, None)
