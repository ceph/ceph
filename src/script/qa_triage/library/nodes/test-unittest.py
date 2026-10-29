import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-10-12_23:32:34-rados-wip-bharath20-testing-20261012.132505-main-distro-default-trial/631092': 'test/unittest/dashboard-port-reset',
    'skanta-2026-10-17_23:23:20-rados-wip-bharath4-testing-20261016.224944-main-distro-default-trial/650737': 'test/unittest/disable-stretch-mode-replay',
    'jayaprakash-2026-03-06_10:20:34-rados-wip-jaya-bs-testing-06-03-2025-distro-default-trial/90378': 'test/unittest/dashboard-port-reset',
}


@node('test/unittest', 'Why did the test fail?')
def test_unittest(ctx):
    test = ctx.facts.get('test', '')
    if test.startswith('tasks.mgr.dashboard.') and 'Connection refused' in ctx.facts.get('error', ''):
        refused = ctx.search('teuthology.log',
                             r"HTTPSConnectionPool\(host='[^']+', port=77\d\d\): Max retries exceeded with url: /api/mgr/",
                             'Max retries exceeded', max_hits=1)
        missing = ctx.search('remote/*/log/ceph-mgr.*.log.gz',
                             r'get_typed_config \S* ?\[\w+/\]ssl_server_port not found|ceph_get_module_option \[\w+/\]ssl_server_port not found',
                             'ssl_server_port not found', max_hits=1)
        if refused and missing:
            mgr = re.search(r'ceph-(mgr\.\w+)\.log', missing[0].file)
            who = mgr.group(1) if mgr else 'a mgr'
            return done('dashboard-port-reset',
                        f'{test} was refused on its assigned dashboard port: the test framework\'s '
                        f'"config reset" deleted the per-mgr ssl_server_port keys, and after the mgrs '
                        f'respawned {who} found no ssl_server_port{at(missing[0])} and started the '
                        'dashboard on the default port 8443',
                        refused[0], missing[0],
                        key='dashboard restarted on default port 8443 after config reset removed ssl_server_port')
        if refused:
            port8443 = ctx.search('teuthology.log',
                                  r'Port 8443 not free on',
                                  'Port 8443 not free', max_hits=1)
            if port8443:
                return done('dashboard-port-reset',
                            f'{test} was refused on its assigned dashboard port: the test framework\'s '
                            '"config reset" deleted the per-mgr ssl_server_port keys, and after the mgrs '
                            f'respawned a mgr found port 8443 not free{at(port8443[0])} and started '
                            'the dashboard on the default port 8443 instead of the assigned port',
                            refused[0], port8443[0],
                            key='dashboard restarted on default port 8443 after config reset removed ssl_server_port')
    if test == 'tasks.stretch_mode_disable_enable.TestStretchMode.test_disable_stretch_mode':
        already_disabled = ctx.search('teuthology.log',
                                      r'Error EINVAL: stretch mode is already disabled',
                                      'stretch mode is already disabled', max_hits=1)
        if already_disabled:
            return done('disable-stretch-mode-replay',
                        'disable_stretch_mode returned EINVAL "stretch mode is already disabled" '
                        'instead of 0: the monitor committed the stretch-mode change but the '
                        'Paxos reply was lost; on retry the handler checks pending_map '
                        'and returns EINVAL instead of 0',
                        already_disabled[0],
                        key='disable_stretch_mode: EINVAL stretch mode is already disabled on replay')
    return unknown('none of the failures this node knows')
