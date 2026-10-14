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


def _run_report(mgr, endpoint, data):
    """Process one report inside an agent request, as the HTTP hooks do."""
    stats = mgr.agent_metadata_stats
    stats.begin_agent_request()
    try:
        return endpoint.handle_metadata(data)
    finally:
        stats.finish_agent_request()


def test_stats_count_accepted_skips_and_avoided_save_host():
    ctx, mgr, host = _setup_mgr()
    try:
        stats = mgr.agent_metadata_stats
        stats.reset()
        endpoint = HostData(mgr)
        mgr.agent_helpers.delta_baselines.record_received(host, ['ls', 'volume'])
        data = _base(host)
        data['unchanged'] = ['ls', 'volume']
        assert _run_report(mgr, endpoint, data).processed is True

        delta = stats.snapshot('mgr.a')['delta']
        assert delta['delta_reports'] == 1
        assert delta['full_reports'] == 0
        assert delta['ls_skipped'] == 1
        assert delta['devices_skipped'] == 1
        assert delta['save_host_avoided'] == 1
    finally:
        ctx.__exit__(None, None, None)


def test_stats_do_not_count_resynced_sections_as_skips():
    ctx, mgr, host = _setup_mgr()
    try:
        stats = mgr.agent_metadata_stats
        stats.reset()
        endpoint = HostData(mgr)
        data = _base(host)
        data['unchanged'] = ['ls', 'volume']   # no baseline: resync requested
        result = _run_report(mgr, endpoint, data)
        assert set(result.resync) == {'ls', 'volume'}

        delta = stats.snapshot('mgr.a')['delta']
        assert delta['delta_reports'] == 1
        assert delta['ls_skipped'] == 0
        assert delta['devices_skipped'] == 0
        assert delta['save_host_avoided'] == 0
    finally:
        ctx.__exit__(None, None, None)


def test_stats_time_phases_of_a_full_report():
    ctx, mgr, host = _setup_mgr()
    try:
        stats = mgr.agent_metadata_stats
        stats.reset()
        endpoint = HostData(mgr)
        data = _base(host)
        data['ls'] = [{'name': 'mon.a', 'style': 'cephadm:v1', 'fsid': mgr._cluster_fsid}]
        data['volume'] = '[]'
        assert _run_report(mgr, endpoint, data).processed is True

        result = stats.snapshot('mgr.a')
        assert result['delta']['full_reports'] == 1
        assert result['delta']['save_host_avoided'] == 0
        processing = result['processing']
        assert processing['ls']['calls'] == 1
        assert processing['scans']['calls'] == 1
        assert processing['other']['calls'] == 1
    finally:
        ctx.__exit__(None, None, None)


def test_first_contact_report_is_not_counted():
    ctx, mgr, host = _setup_mgr()
    try:
        stats = mgr.agent_metadata_stats
        stats.reset()
        del mgr.agent_cache.agent_counter[host]
        with patch.object(mgr.agent_helpers, '_request_agent_acks'):
            result = _run_report(mgr, HostData(mgr), _base(host))
        assert result.processed is False
        delta = stats.snapshot('mgr.a')['delta']
        assert delta['full_reports'] == 0
        assert delta['delta_reports'] == 0
    finally:
        ctx.__exit__(None, None, None)


def test_daemon_scans_are_skipped_when_ls_is_not_sent():
    """Without ls, the host's daemons cannot change: no error-daemon scans."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        mgr.agent_helpers.delta_baselines.record_received(host, ['ls', 'facts'])
        for data in (
            dict(_base(host), unchanged=['ls']),
            dict(_base(host), facts='{"hostname": "delta-host"}'),
        ):
            with patch.object(mgr.cache, 'get_error_daemons') as scan, \
                    patch.object(mgr, 'update_failed_daemon_health_check') as health:
                assert endpoint.handle_metadata(data).processed is True
            scan.assert_not_called()
            health.assert_not_called()
    finally:
        ctx.__exit__(None, None, None)


def _ls(mgr, *daemons):
    return [dict({'style': 'cephadm:v1', 'fsid': mgr._cluster_fsid}, name=name, state=state)
            for name, state in daemons]


def test_daemon_change_from_sent_ls_kicks_serve_loop():
    """With ls sent, a new daemon on the host still kicks the serve loop."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['ls'] = [{'name': 'mon.a', 'style': 'cephadm:v1', 'fsid': mgr._cluster_fsid}]
        with patch.object(mgr, '_kick_serve_loop') as kick, \
                patch.object(mgr.cache, 'get_error_daemons') as scan:
            assert endpoint.handle_metadata(data).processed is True
        scan.assert_not_called()
        kick.assert_called()
    finally:
        ctx.__exit__(None, None, None)


def test_new_error_daemon_updates_health_check_and_kicks_serve_loop():
    """An error daemon reported by the host refreshes the health check and kicks the serve loop."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['ls'] = _ls(mgr, ('mon.a', 'running'))
        assert endpoint.handle_metadata(data).processed is True

        data['ls'] = _ls(mgr, ('mon.a', 'error'))
        with patch.object(mgr, '_kick_serve_loop') as kick:
            assert endpoint.handle_metadata(data).processed is True
        kick.assert_called()
        assert 'CEPHADM_FAILED_DAEMON' in mgr.health_checks

        data['ls'] = _ls(mgr, ('mon.a', 'running'))
        with patch.object(mgr, '_kick_serve_loop') as kick:
            assert endpoint.handle_metadata(data).processed is True
        kick.assert_called()
        assert 'CEPHADM_FAILED_DAEMON' not in mgr.health_checks
    finally:
        ctx.__exit__(None, None, None)


def test_ls_without_daemon_changes_skips_health_check_and_kick():
    """A resent ls with the same daemons and states changes nothing in the cluster state."""
    ctx, mgr, host = _setup_mgr()
    try:
        endpoint = HostData(mgr)
        data = _base(host)
        data['ls'] = _ls(mgr, ('mon.a', 'error'), ('mgr.b', 'running'))
        assert endpoint.handle_metadata(data).processed is True

        with patch.object(mgr, '_kick_serve_loop') as kick, \
                patch.object(mgr, 'update_failed_daemon_health_check') as health:
            assert endpoint.handle_metadata(data).processed is True
        health.assert_not_called()
        kick.assert_not_called()
    finally:
        ctx.__exit__(None, None, None)


def test_error_daemons_of_other_hosts_do_not_affect_report():
    """Error daemons on other hosts are not looked at when this host reports."""
    ctx, mgr, host = _setup_mgr()
    try:
        other = 'other-host'
        mgr.inventory.add_host(HostSpec(hostname=other, addr='1::5'))
        mgr._process_ls_output(other, _ls(mgr, ('osd.1', 'error')))

        endpoint = HostData(mgr)
        data = _base(host)
        data['ls'] = _ls(mgr, ('mon.a', 'running'))
        assert endpoint.handle_metadata(data).processed is True

        with patch.object(mgr, 'update_failed_daemon_health_check') as health, \
                patch.object(mgr.cache, 'get_error_daemons') as scan:
            assert endpoint.handle_metadata(data).processed is True
        scan.assert_not_called()
        health.assert_not_called()
    finally:
        ctx.__exit__(None, None, None)


def test_scan_phase_is_not_timed_for_reports_without_ls():
    ctx, mgr, host = _setup_mgr()
    try:
        stats = mgr.agent_metadata_stats
        stats.reset()
        endpoint = HostData(mgr)
        mgr.agent_helpers.delta_baselines.record_received(host, ['ls'])
        data = _base(host)
        data['unchanged'] = ['ls']
        assert _run_report(mgr, endpoint, data).processed is True
        processing = stats.snapshot('mgr.a')['processing']
        assert processing['scans']['calls'] == 0
        assert processing['other']['calls'] == 1
    finally:
        ctx.__exit__(None, None, None)
