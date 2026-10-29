import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at, short

EXAMPLES = {
    'skanta-2026-07-12_04:39:06-rados-wip-bharath12-testing-2026-07-09-1411-distro-default-trial/356703': 'hung/command/ceph-daemon-config-set-hang',
    'yuriw-2026-10-21_19:23:54-rados-wip-yuri14-testing-20261021.151926-tentacle-distro-default-trial/664526': 'hung/command/cephadm-rm-cluster',
    'yuriw-2026-10-21_19:23:54-rados-wip-yuri14-testing-20261021.151926-tentacle-distro-default-trial/664996': 'hung/command/thrasher-min-size-pgs-not-active',
    'skanta-2026-09-25_03:59:04-rados-wip-bharath29-testing-20260924.231744-main-distro-default-trial/581671': 'hung/command/dispatch-delay-queued-for-pg',
    'skanta-2026-09-12_14:24:38-rados-wip-bharath12-testing-20260911.231931-main-distro-default-trial/553732': 'hung/command/objectstore-synthetic-csum-hang',
    'skanta-2026-08-25_00:08:14-rados-wip-bharath11-testing-20260824.222329-main-distro-default-trial/510700': 'hung/command/osd-down-never-returned',
    'skanta-2026-07-19_00:55:49-rados-wip-bharath5-testing-2026-07-19-0500-distro-default-trial/376260': 'hung/command/ceph-daemon-config-set-hang',
    'jayaprakash-2026-07-15_18:38:17-rados-wip-jaya-bs-testing2-15-07-2026-distro-default-trial/366546': 'hung/command/host-drain-osd-not-scheduled',
    'skanta-2026-04-13_14:09:46-rados-wip-bharath11-testing-2026-04-13-1517-distro-default-trial/148457': 'hung/command/dispatch-delay-queued-for-pg',
    'jayaprakash-2026-03-06_10:20:34-rados-wip-jaya-bs-testing-06-03-2025-distro-default-trial/90431': 'hung/command/workunit-clone-git-ceph-com-504',
    'nmordech-2026-03-17_06:34:53-upgrade-wip-rocky10-branch-of-the-day-2026-03-16-1773712510-distro-default-trial/104134': 'hung/command/osd-start-limit-hit',
    'pdonnell-2026-05-27_21:13:45-upgrade-wip-pdonnell-testing-20260527.030902-main-distro-default-trial/243486': 'hung/command/osd-start-limit-hit',
    'pdonnell-2026-05-27_21:13:45-upgrade-wip-pdonnell-testing-20260527.030902-main-distro-default-trial/243543': 'hung/command/osd-start-limit-hit',
    'pdonnell-2026-05-27_21:13:45-upgrade-wip-pdonnell-testing-20260527.030902-main-distro-default-trial/243569': 'hung/command/osd-start-limit-hit',
    'pdonnell-2026-05-27_21:13:45-upgrade-wip-pdonnell-testing-20260527.030902-main-distro-default-trial/243742': 'hung/command/osd-start-limit-hit',
    'pdonnell-2026-05-27_21:13:45-upgrade-wip-pdonnell-testing-20260527.030902-main-distro-default-trial/243773': 'hung/command/osd-start-limit-hit',
    'pdonnell-2026-05-27_21:13:45-upgrade-wip-pdonnell-testing-20260527.030902-main-distro-default-trial/243805': 'hung/command/osd-start-limit-hit',
}


@node('hung/command', 'Why did the command hang?')
def hung_command(ctx):
    command = ctx.facts.get('command', '')
    if command.startswith('cephadm rm-cluster'):
        cmd = ctx.last('command')
        return done('cephadm-rm-cluster',
                    f'cephadm rm-cluster hung during teardown{at(cmd)}',
                    cmd, key='cephadm rm-cluster hung')
    frame = ctx.search('teuthology.log', r'in test_pool_min_size$', 'in test_pool_min_size')
    waited = ctx.search('teuthology.log',
                        r"MaxWhileTries: 'check for active or peered' reached maximum tries",
                        "'check for active or peered'")
    if frame and waited:
        return done('thrasher-min-size-pgs-not-active',
                    f'the OSDThrasher failed in test_pool_min_size{at(waited[0])}: PGs were not '
                    'active or peered after killing OSDs, so the watchdog killed all daemons '
                    'and the job then hung until the max job timeout',
                    frame[0], waited[0],
                    key='OSDThrasher test_pool_min_size: PGs not active or peered',
                    thrasher_failure='test_pool_min_size')
    hung_task = ctx.facts.get('hung_task', '')
    if hung_task == 'rados' and re.search(r'dump_(?:ops_in_flight|blocked_ops|historic_ops)', command):
        delay = ctx.search('teuthology.log',
                           r'dispatch[ _]delay[ _]probability: 0?\.0*[1-9]',
                           'dispatch delay probability', 'dispatch_delay_probability')
        queued = ctx.search('teuthology.log',
                            r'"flag_point":\s*"queued for pg"',
                            'queued for pg')
        if delay and queued:
            return done('dispatch-delay-queued-for-pg',
                        f'the rados task hung: client ops stuck "queued for pg"{at(queued[0])} '
                        f'because osd-dispatch-delay (probability {delay[0].text.split()[-1]}) '
                        'starved the op queue under morepggrow peering churn',
                        delay[0], queued[0],
                        key='rados hung: dispatch-delay ops queued for pg')
    if hung_task == 'rados' and command.startswith('ceph osd dump'):
        waiting = ctx.search('teuthology.log', r'ceph_manager\.ceph:waiting for all up',
                             'waiting for all up', max_hits=1)
        if waiting:
            marked = ctx.search('remote/*/log/ceph.log.gz',
                                r'cluster \[INF\] Marking osd\.\d+ out \(has been down for \d+ seconds\)',
                                'Marking osd.', max_hits=200)
            failed = ctx.search('remote/*/log/ceph.log.gz',
                                r'cluster \[INF\] osd\.\d+ failed \(root=',
                                ' failed (root=', max_hits=200)
            for out in marked:
                found = re.search(r'Marking (osd\.\d+) out', out.text)
                if not found:
                    continue
                who = found.group(1)
                for down in failed:
                    if down.file != out.file or f'cluster [INF] {who} failed' not in down.text:
                        continue
                    boots = ctx.search(down.file,
                                       r'cluster \[INF\] ' + re.escape(who) + r' \S+ boot',
                                       ' boot', max_hits=200)
                    if any(boot.line > down.line for boot in boots):
                        continue
                    return done('osd-down-never-returned',
                                f'{who} was marked down{at(down)} and never came back up, so '
                                f'the wait for all OSDs up{at(waiting[0])} polled ceph osd dump '
                                'until the max job timeout',
                                down, out, waiting[0],
                                key='thrashosds: waiting for all up, OSD marked down and never returned',
                                osd=who)
    if hung_task == 'exec' and command.startswith('mkdir $TESTDIR/archive/ostest'):
        started = ctx.search('teuthology.log',
                             r'\[ RUN      \] \S*SyntheticMatrixCsumAlgorithm',
                             'SyntheticMatrixCsumAlgorithm', max_hits=1)
        finished = ctx.search('teuthology.log',
                              r'\[\s+(?:OK|FAILED)\s+\] \S*SyntheticMatrixCsumAlgorithm',
                              'SyntheticMatrixCsumAlgorithm', max_hits=1)
        if started and not finished:
            return done('objectstore-synthetic-csum-hang',
                        f'ceph_test_objectstore hung in SyntheticMatrixCsumAlgorithm{at(started[0])}: '
                        'the test started and never reported OK or FAILED, so the job ran '
                        'until the max job timeout',
                        started[0], ctx.last('killed'),
                        key='ceph_test_objectstore: SyntheticMatrixCsumAlgorithm hung',
                        test='SyntheticMatrixCsumAlgorithm')
    cmd = ctx.last('command')
    if hung_task == 'exec' and cmd and 'ceph daemon osd' in cmd.text and 'config set' in cmd.text:
        return done('ceph-daemon-config-set-hang',
                    f'the exec task hung running ceph daemon config set{at(cmd)}: '
                    'the admin socket client had no timeout and blocked indefinitely '
                    'on a restarting OSD',
                    cmd, ctx.last('killed'),
                    key='exec hung: ceph daemon config set on restarting OSD',
                    command=command)
    if hung_task == 'cephadm.shell':
        drain = ctx.search('teuthology.log', r'stderr:\+ ceph orch host drain (\S+)',
                           '+ ceph orch host drain', max_hits=1)
        killed = ctx.last('killed')
        if drain and killed:
            host = re.search(r'host drain (\S+)', drain[0].text).group(1)
            looping = re.compile(r'stderr:(osd\.\d+)\s+' + re.escape(host) +
                                 r"\s.*'!=' 'No daemons reported'")
            left = [h for h in ctx.between(max(1, killed.line - 400), killed.line)
                    if looping.search(h.text)]
            if left:
                who = looping.search(left[-1].text).group(1)
                logs = 'remote/*/log/*/ceph.cephadm.log*'
                labelled = ctx.search(logs, r'Added label _no_schedule to host ' +
                                      re.escape(host) + r'(?!\S)',
                                      'Added label _no_schedule to host', max_hits=6)
                reconfigured = ctx.search(logs, r'Reconfiguring daemon ' + re.escape(who) +
                                          ' on ' + re.escape(host) + r'(?!\S)',
                                          f'Reconfiguring daemon {who} on ',
                                          max_hits=6)
                removed = ctx.search(logs, r'Removing daemon ' + re.escape(who) + ' from ' +
                                     re.escape(host) + r'(?!\S)',
                                     f'Removing daemon {who} from ', max_hits=1)
                later = [r for r in reconfigured
                         if any(r.file == lab.file and r.line > lab.line for lab in labelled)]
                if later and not removed:
                    deployed = ctx.search(logs, r'Deploying daemon ' + re.escape(who) + ' on ' +
                                          re.escape(host) + r'(?!\S)',
                                          f'Deploying daemon {who} on ', max_hits=1)
                    since = f', deployed{at(deployed[0])},' if deployed else ''
                    return done('host-drain-osd-not-scheduled',
                                f'"ceph orch host drain {host}"{at(drain[0])} never scheduled '
                                f'{who}{since} for removal: it was not yet in cephadm\'s daemon '
                                f'cache, so cephadm only reconfigured it{at(later[0])} and the '
                                f'test waited for {host} to have no daemons until the max job '
                                'timeout',
                                drain[0], deployed[0] if deployed else None, later[0],
                                left[-1], killed,
                                key='host drain: OSD not scheduled for removal, only reconfigured',
                                daemon=who)
    if hung_task == 'workunit' and command.startswith('find ') and 'rmdir' in command:
        err504 = ctx.search('teuthology.log',
                            r'git\.ceph\.com.*returned error: 504',
                            'git.ceph.com', 'error: 504')
        if err504:
            return done('workunit-clone-git-ceph-com-504',
                        f'the workunit task failed to clone the ceph repo from git.ceph.com'
                        f'{at(err504[0])}: git.ceph.com returned HTTP 504, so both git clone '
                        'attempts failed with status 128; the workunit left its scratch '
                        'directory behind, causing rmdir to fail during teardown',
                        err504[0],
                        key='workunit: git clone git.ceph.com: 504')
    failed = ctx.search('teuthology.log',
                        r"Command failed on \S+ with status 1: 'sudo systemctl start ceph-\S+@(osd\.\d+)'",
                        'sudo systemctl start ceph-')
    if failed:
        who = re.search(r'@(osd\.\d+)', failed[0].text).group(1)
        limited = ctx.search('teuthology.log',
                             r"ceph-\S+@" + re.escape(who) + r"\.service: Failed with result 'start-limit-hit'",
                             'start-limit-hit')
        if limited:
            return done('osd-start-limit-hit',
                        f'the thrasher could not revive {who}{at(failed[0])}: systemd refused to '
                        f'start its unit again ({short(limited[0].text, 120)}), so the OSD stayed '
                        'down and the job hung until the max job timeout',
                        failed[0], limited[0],
                        key='thrasher: systemctl start of OSD unit failed, start-limit-hit',
                        osd=who)
    return unknown('none of the failures this node knows')
