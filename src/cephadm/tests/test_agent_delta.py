import json

from cephadmlib.agent_delta import (
    DeltaReason,
    MEMORY_TOLERANCE_BYTES,
    MetadataDeltaTracker,
    ls_by_name,
    ls_change_reason,
)

MIB = 1024 * 1024


def _delivered(tracker, ack, sections):
    """Build a report for sections and commit it as processed by the mgr."""
    fields, pending = tracker.build(ack, sections)
    tracker.commit(pending)
    return fields


def test_first_report_for_an_ack_sends_every_section():
    """Every available section is sent in full the first time it is reported."""
    tracker = MetadataDeltaTracker()
    sections = {
        'ls': [{'name': 'osd.0', 'memory_usage': 100}],
        'networks': {'10.0.0.0/24': {'eth0': ['10.0.0.2']}},
    }
    fields, pending = tracker.build(7, sections)
    assert fields['ls'] == sections['ls']
    assert fields['networks'] == sections['networks']
    assert 'unchanged' not in fields
    assert {name: p.reason for name, p in pending.items()} == {
        'ls': DeltaReason.FULL_SYNC,
        'networks': DeltaReason.FULL_SYNC,
    }


def test_build_alone_leaves_the_baseline_untouched():
    """Without a commit, the same content is sent again on the next report."""
    tracker = MetadataDeltaTracker()
    sections = {'networks': {'10.0.0.0/24': {'eth0': ['10.0.0.2']}}}
    tracker.build(7, sections)
    assert tracker.synced_ack == {}
    assert tracker.baselines == {}
    fields, _ = tracker.build(7, sections)
    assert 'networks' in fields


def test_committed_unchanged_sections_are_listed_not_sent():
    """After a commit, identical sections are reported only as unchanged."""
    tracker = MetadataDeltaTracker()
    sections = {
        'ls': [{'name': 'osd.0', 'memory_usage': 100}],
        'networks': {'10.0.0.0/24': {'eth0': ['10.0.0.2']}},
    }
    _delivered(tracker, 7, sections)
    fields, pending = tracker.build(7, sections)
    assert 'ls' not in fields and 'networks' not in fields
    assert set(fields['unchanged']) == {'ls', 'networks'}
    assert not any(p.sent for p in pending.values())


def test_new_ack_full_syncs_each_section_independently():
    """A new ack makes each section full-sync when it is next available."""
    tracker = MetadataDeltaTracker()
    ls = [{'name': 'osd.0', 'memory_usage': 100}]
    networks = {'10.0.0.0/24': {'eth0': ['10.0.0.2']}}
    _delivered(tracker, 7, {'ls': ls, 'networks': networks})
    fields = _delivered(tracker, 8, {'ls': ls})
    assert fields['ls'] == ls
    assert tracker.synced_ack == {'ls': 8, 'networks': 7}


def test_resync_sends_the_section_in_full_on_the_next_report():
    """A resync request from the mgr clears the section's baseline."""
    tracker = MetadataDeltaTracker()
    facts = json.dumps({'hostname': 'host1'})
    _delivered(tracker, 1, {'facts': facts})
    tracker.resync(['facts', 'not-a-section'])
    fields, pending = tracker.build(1, {'facts': facts})
    assert fields['facts'] == facts
    assert pending['facts'].reason is DeltaReason.FULL_SYNC


def test_missing_baseline_for_a_synced_section_sends_it():
    """A section with a synced ack but no baseline is sent as structural."""
    tracker = MetadataDeltaTracker()
    tracker.synced_ack['ls'] = 7
    ls = [{'name': 'osd.0', 'memory_usage': 100}]
    fields, pending = tracker.build(7, {'ls': ls})
    assert fields['ls'] == ls
    assert pending['ls'].reason is DeltaReason.STRUCTURAL


def test_facts_compare_equal_when_only_volatile_fields_differ():
    """Facts ignore observations and sysctl values but keep sysctl names."""
    tracker = MetadataDeltaTracker()
    facts1 = {
        'hostname': 'host1',
        'timestamp': 1.0,
        'system_uptime': 10.0,
        'memory_free_kb': 100,
        'memory_available_kb': 200,
        'cpu_load': {'1min': 1.0},
        'tcp_ports_used': [22, 6800],
        'sysctl_options': {'kernel.random.uuid': 'aaa', 'vm.swappiness': '60'},
    }
    _delivered(tracker, 1, {'facts': json.dumps(facts1)})
    facts2 = dict(
        facts1,
        timestamp=2.0,
        system_uptime=20.0,
        memory_free_kb=90,
        memory_available_kb=180,
        cpu_load={'1min': 3.0},
        tcp_ports_used=[6800, 22],
        sysctl_options={'kernel.random.uuid': 'bbb', 'vm.swappiness': '10'},
    )
    fields, _ = tracker.build(1, {'facts': json.dumps(facts2)})
    assert fields['unchanged'] == ['facts']

    facts3 = dict(facts2, sysctl_options={'vm.swappiness': '10'})
    fields, _ = tracker.build(1, {'facts': json.dumps(facts3)})
    assert 'facts' in fields


def test_ls_comparison_is_independent_of_daemon_order():
    """Reordering daemons in ls is not a change."""
    tracker = MetadataDeltaTracker()
    ls = [
        {'name': 'osd.0', 'memory_usage': 100, 'ports': [6800]},
        {'name': 'mon.a', 'memory_usage': 200, 'ports': [3300, 6789]},
    ]
    _delivered(tracker, 3, {'ls': ls})
    fields, _ = tracker.build(3, {'ls': [dict(ls[1]), dict(ls[0])]})
    assert fields['unchanged'] == ['ls']


def test_ls_stats_within_tolerance_are_unchanged():
    """memory_usage within the tolerance and equal None stats are unchanged."""
    previous = ls_by_name(
        [
            {'name': 'osd.0', 'memory_usage': 100 * MIB},
            {'name': 'osd.1', 'memory_usage': None, 'cpu_percentage': None},
        ]
    )
    current = ls_by_name(
        [
            {'name': 'osd.0', 'memory_usage': 109 * MIB},
            {'name': 'osd.1', 'memory_usage': None, 'cpu_percentage': None},
        ]
    )
    assert ls_change_reason(previous, current) is None


def test_ls_stats_beyond_tolerance_or_appearing_are_changes():
    """Stats past the tolerance, or going from None to a value, are changes."""
    previous = ls_by_name(
        [{'name': 'osd.0', 'memory_usage': None, 'cpu_percentage': '1.0%'}]
    )
    appeared = ls_by_name(
        [{'name': 'osd.0', 'memory_usage': 128 * MIB, 'cpu_percentage': '1.0%'}]
    )
    assert ls_change_reason(previous, appeared) is DeltaReason.MEMORY
    cpu = ls_by_name(
        [{'name': 'osd.0', 'memory_usage': None, 'cpu_percentage': '3.0%'}]
    )
    assert ls_change_reason(previous, cpu) is DeltaReason.CPU


def test_ls_structural_change_takes_precedence_over_stats():
    """Any non-stat change in any daemon classifies ls as structural."""
    previous = ls_by_name(
        [
            {'name': 'osd.0', 'memory_usage': 100 * MIB, 'state': 'running'},
            {'name': 'osd.1', 'memory_usage': 200 * MIB, 'state': 'running'},
        ]
    )
    current = ls_by_name(
        [
            {'name': 'osd.0', 'memory_usage': 300 * MIB, 'state': 'running'},
            {'name': 'osd.1', 'memory_usage': 200 * MIB, 'state': 'stopped'},
        ]
    )
    assert ls_change_reason(previous, current) is DeltaReason.STRUCTURAL


def test_unchanged_ls_keeps_its_baseline_so_drift_accumulates():
    """Small memory steps are measured against the last delivered value."""
    tracker = MetadataDeltaTracker()
    step = MEMORY_TOLERANCE_BYTES * 6 // 10
    _delivered(tracker, 1, {'ls': [{'name': 'osd.0', 'memory_usage': 0}]})
    fields = _delivered(
        tracker, 1, {'ls': [{'name': 'osd.0', 'memory_usage': step}]}
    )
    assert fields['unchanged'] == ['ls']
    fields = _delivered(
        tracker, 1, {'ls': [{'name': 'osd.0', 'memory_usage': 2 * step}]}
    )
    assert 'ls' in fields


def test_ls_reasons_are_counted_once_per_delivered_report():
    """ls reasons are counted on commit only, and reported cumulatively."""
    tracker = MetadataDeltaTracker()
    ls = [{'name': 'osd.0', 'memory_usage': 100 * MIB, 'state': 'running'}]
    fields = _delivered(tracker, 1, {'ls': ls})
    assert fields['ls_delta_stats'] == {r.value: 0 for r in DeltaReason}

    stopped = [dict(ls[0], state='stopped')]
    for _ in range(3):
        tracker.build(1, {'ls': stopped})  # e.g. retries after failed POSTs
    _delivered(tracker, 1, {'ls': stopped})
    _delivered(tracker, 1, {'ls': stopped})

    fields, _ = tracker.build(1, {'ls': stopped})
    assert fields['ls_delta_stats'] == {
        'full_sync': 1,
        'unchanged': 1,
        'structural': 1,
        'memory_usage': 0,
        'cpu_percentage': 0,
    }


def test_delta_reason_wire_values():
    """DeltaReason values are the ls_delta_stats keys the mgr aggregates."""
    assert {reason.value for reason in DeltaReason} == {
        'full_sync',
        'unchanged',
        'structural',
        'memory_usage',
        'cpu_percentage',
    }


def test_ls_delta_stats_serialize_as_plain_strings():
    """ls_delta_stats is sent with plain string keys."""
    fields, _ = MetadataDeltaTracker().build(1, {})
    encoded = json.loads(json.dumps(fields['ls_delta_stats']))
    assert all(type(key) is str for key in fields['ls_delta_stats'])
    assert set(encoded) == {reason.value for reason in DeltaReason}
