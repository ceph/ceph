import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'nmordech-2026-02-25_11:35:39-rados:cephadm-wip-rocky10-branch-of-the-day-2026-02-24-1771941190-distro-default-trial/69855': 'cluster-log/health/cephadm-failed-daemon-transient',
    'skanta-2026-10-12_23:32:34-rados-wip-bharath20-testing-20261012.132505-main-distro-default-trial/631307': 'cluster-log/health/discard-queue-overload',
    'skanta-2026-09-03_10:37:47-rados-wip-bharath10-testing-20260903.050751-main-distro-default-trial/534585': 'cluster-log/health/max-mds-above-mds-count',
    'skanta-2026-08-24_07:43:11-rados-wip-bharath7-testing-20260824.042435-main-distro-default-trial/508965': 'cluster-log/health/dead-ping-not-retried',
    'skanta-2026-07-02_01:10:52-rados-wip-bharath2-testing-2026-07-01-2021-distro-default-trial/335079': 'cluster-log/health/cephadm-failed-daemon-transient',
    'jenkins-build-2026-09-17_06:05:09-rados-main-distro-default-trial/563205': 'cluster-log/health/radosbench-populate-overfill',
    'jenkins-build-2026-09-17_06:05:09-rados-main-distro-default-trial/563307': 'cluster-log/health/radosbench-populate-overfill',
    'skanta-2026-06-04_09:53:35-rados-wip-bharath5-testing-2026-06-02-1032-distro-default-trial/263707': 'cluster-log/health/cephadm-refresh-failed-transient',
    'lflores-2026-04-17_22:03:40-rados-wip-lflores-testing-2-2026-04-08-1424-tentacle-distro-default-trial/159228': 'cluster-log/health/mon-restart-reconnect-delay',
    'lflores-2026-01-23_19:07:45-rados-wip-rocky10-branch-of-the-day-2026-01-23-1769128778-distro-default-trial/15326': 'cluster-log/health/radosbench-populate-overfill',
    'yuriw-2026-01-23_21:16:01-rados-wip-yuri13-testing-2026-01-23-1852-reef-distro-default-trial/15685': 'cluster-log/health/cephadm-stray-daemon-transient',
}

LETTERS = 'abcdefghijklmnopqrstuvwxyz'


@node('cluster-log/health', 'What was behind the health check?')
def cluster_log_health(ctx):
    check = ctx.facts.get('check', '')
    reporter = ctx.facts.get('reporter', '')
    if check.endswith('DEVICE_DISCARD_QUEUE') and reporter.startswith('mon.'):
        slow = ctx.search(f'remote/*/log/ceph-{reporter}.log.gz',
                          r'(osd\.\d+) Slow discard on (\w+), queue: (\d+) items',
                          'Slow discard on', max_hits=1)
        if slow:
            found = re.search(r'(osd\.\d+) Slow discard on (\w+), queue: (\d+) items', slow[0].text)
            osd, device = found.group(1), found.group(2)
            return done('discard-queue-overload',
                        f'{osd} had an overloaded {device} discard queue ({found.group(3)} items), '
                        'which raised the health warning; the test does not ignore '
                        f'{check} in its log-ignorelist',
                        slow[0], key=f'{check}: discard queue overload',
                        osd=osd, device=device)
    if check == 'MDS_UP_LESS_THAN_MAX' and reporter.startswith('mon.'):
        setting = ctx.search('teuthology.log',
                             r'cephtool/test\.sh:\d+: test_mon_mds:\s+ceph fs set (\S+) max_mds (\d+)',
                             'test_mon_mds', max_hits=1)
        if setting:
            found = re.search(r'ceph fs set (\S+) max_mds (\d+)', setting[0].text)
            fs, max_mds = found.group(1), found.group(2)
            return done('max-mds-above-mds-count',
                        f'the cephtool workunit (test_mon_mds) set max_mds to {max_mds} on '
                        f'filesystem {fs}{at(setting[0])} while the cluster has fewer MDS daemons, '
                        f'so {reporter} raised {check}, which is expected; the test does not '
                        'ignore it in its log-ignorelist',
                        setting[0], key=f'{check}: cephtool test_mon_mds raises max_mds',
                        fs=fs, max_mds=max_mds)
    if check == 'MON_NETSPLIT' and reporter.startswith('mon.'):
        split = ctx.search(f'remote/*/log/ceph-{reporter}.log.gz',
                           r'health Netsplit detected between mon\.(\w+) and mon\.(\w+), elapsed time',
                           'Netsplit detected between mon.', max_hits=1)
        if split:
            found = re.search(r'between mon\.(\w+) and mon\.(\w+), elapsed time', split[0].text)
            first, second = found.group(1), found.group(2)
            if len(first) == 1 and len(second) == 1 and second in LETTERS:
                rank = LETTERS.index(second)
                dead = ctx.search(f'remote/*/log/ceph-mon.{first}.log.gz',
                                  rf'report_dead_connection peer_rank: {rank} ',
                                  'report_dead_connection', max_hits=1)
                if dead:
                    return done('dead-ping-not-retried',
                                f'{reporter} logged {check} between mon.{first} and mon.{second}'
                                f'{at(split[0])}, although both mons were running: mon.{first} kept '
                                f'reporting mon.{second} as a dead connection{at(dead[0])}, because a '
                                'mon that has marked a peer dead does not ping it again after a '
                                'messenger reconnect (tracker 79814), and the test does not ignore '
                                f'{check} in its log-ignorelist',
                                split[0], dead[0],
                                key=f'{check}: mon ping to dead peer not retried after reconnect')
    if check == 'MON_NETSPLIT' and reporter.startswith('mon.'):
        restart = ctx.search('teuthology.log',
                             r'tasks\.ceph\.mon\.(\w+):Restarting daemon',
                             'Restarting daemon', max_hits=1)
        pairs = ctx.search(f'remote/*/log/ceph-{reporter}.log.gz',
                           r'"message": "Netsplit detected between mon\.(\w+) and mon\.(\w+)"',
                           'Netsplit detected between mon.', max_hits=50) if restart else []
        if pairs:
            found = re.search(r'between mon\.(\w+) and mon\.(\w+)"', pairs[-1].text)
            first, second = found.group(1), found.group(2)
            if len(first) == 1 and len(second) == 1 and first in LETTERS and second in LETTERS:
                one = ctx.search(f'remote/*/log/ceph-mon.{first}.log.gz',
                                 rf'elector\(\d+\) begin_dead_ping to peer {LETTERS.index(second)}\b',
                                 'begin_dead_ping to peer', max_hits=1)
                other = ctx.search(f'remote/*/log/ceph-mon.{second}.log.gz',
                                   rf'elector\(\d+\) begin_dead_ping to peer {LETTERS.index(first)}\b',
                                   'begin_dead_ping to peer', max_hits=1)
                if one and other:
                    return done('mon-restart-reconnect-delay',
                                f'{reporter} logged {check} between mon.{first} and mon.{second}'
                                f'{at(pairs[-1])} while the job was restarting mons{at(restart[0])}: '
                                f'both mons were running but began dead-pinging each other{at(one[0])}, '
                                'because the restarted mon was slow to reconnect after the restart '
                                '(tracker 71344), and the test does not ignore '
                                f'{check} in its log-ignorelist',
                                restart[0], pairs[-1], one[0], other[0],
                                key=f'{check}: slow mon reconnect after a mon restart',
                                first=first, second=second)
    if check == 'CEPHADM_FAILED_DAEMON' and reporter.startswith('mon.'):
        cleared = ctx.search('remote/*/log/*/ceph.log.gz',
                             r'Health check cleared: CEPHADM_FAILED_DAEMON',
                             'Health check cleared: CEPHADM_FAILED_DAEMON',
                             max_hits=1) or \
                  ctx.search('teuthology.log',
                             r'Health check cleared: CEPHADM_FAILED_DAEMON',
                             'Health check cleared: CEPHADM_FAILED_DAEMON',
                             max_hits=1)
        if cleared:
            return done('cephadm-failed-daemon-transient',
                        f'{reporter} logged [WRN] CEPHADM_FAILED_DAEMON, which cleared on the '
                        f'next cephadm refresh{at(cleared[0])}: a daemon was transiently in '
                        "unknown state during startup because cephadm ls maps 'unknown' to "
                        '.error (tracker 65728), causing a spurious health warning that the '
                        'teardown cluster-log scan caught',
                        cleared[0],
                        key='CEPHADM_FAILED_DAEMON: transient unknown state during startup')
    if check == 'CEPHADM_REFRESH_FAILED' and reporter.startswith('mon.'):
        cleared = ctx.search('remote/*/log/*/ceph.log.gz',
                             r'Health check cleared: CEPHADM_REFRESH_FAILED',
                             'Health check cleared: CEPHADM_REFRESH_FAILED',
                             max_hits=1)
        if cleared:
            return done('cephadm-refresh-failed-transient',
                        f'{reporter} logged [WRN] CEPHADM_REFRESH_FAILED, which cleared on the '
                        f'next cephadm refresh{at(cleared[0])}: cephadm could not probe a daemon '
                        'that was still starting up, raising a spurious health warning that the '
                        'teardown cluster-log scan caught (tracker 77415)',
                        cleared[0],
                        key='CEPHADM_REFRESH_FAILED: transient probe failure during daemon startup')
    if check == 'CEPHADM_STRAY_DAEMON' and reporter.startswith('mon.'):
        cleared = ctx.search('teuthology.log',
                             r'Health check cleared: CEPHADM_STRAY_DAEMON',
                             'Health check cleared: CEPHADM_STRAY_DAEMON',
                             max_hits=1)
        if cleared:
            return done('cephadm-stray-daemon-transient',
                        f'{reporter} logged [WRN] CEPHADM_STRAY_DAEMON, which cleared{at(cleared[0])}: '
                        'a daemon was transiently not managed by cephadm during a reconfiguration '
                        '(tracker 64872), causing a spurious health warning that the teardown '
                        'cluster-log scan caught',
                        cleared[0],
                        key='CEPHADM_STRAY_DAEMON: transient stray daemon during reconfiguration')
    # thrash-backfill-full fills OSDs on purpose, so a nearfull warning there is
    # not explained by the populate run alone.
    if check in ('OSD_NEARFULL', 'POOL_NEARFULL') and \
            'thrash-backfill-full' not in ctx.job.description:
        populate = ctx.search('teuthology.log',
                              r"rados .*--pool=(\S+) bench --concurrent-ios=\d+ 60 write --no-cleanup'",
                              'write --no-cleanup', max_hits=1)
        uncapped = ctx.search('teuthology.log',
                              r'Maintaining \d+ concurrent writes of 4194304 bytes to objects of '
                              r'size 4194304 for up to 60 seconds or 0 objects',
                              'for up to 60 seconds or 0 objects', max_hits=1)
        if populate and uncapped:
            pool = re.search(r'--pool=(\S+)', populate[0].text).group(1)
            made = ctx.search('teuthology.log', r'Total writes made:\s+(\d+)',
                              'Total writes made:', max_hits=1)
            writes = re.search(r'Total writes made:\s+(\d+)', made[0].text).group(1) if made else ''
            return done('radosbench-populate-overfill',
                        f'the radosbench task populated pool {pool} for its read run'
                        f'{at(populate[0])} with an uncapped 60-second write of default 4 MB '
                        'objects, ignoring its objectsize'
                        f'{f" ({writes} writes)" if writes else ""}, which filled an OSD past '
                        f'the nearfull ratio, so {reporter or "the mon"} raised {check}; the test '
                        'does not ignore it in its log-ignorelist',
                        populate[0], uncapped[0],
                        key='nearfull: radosbench populate run writes uncapped 4 MB objects',
                        pool=pool)
        cbt = ctx.search('teuthology.log',
                         r'-p (\S+) bench -b \d+ \d+ write --concurrent-ios \d+ .*--no-cleanup',
                         'write --concurrent-ios', max_hits=1)
        if cbt:
            pool = re.search(r'-p (\S+) bench', cbt[0].text).group(1)
            usage = ctx.search('teuthology.log',
                               rf"Pool '{re.escape(pool)}' root_id -?\d+ using (0\.[89]\d*) of space",
                               'of space', max_hits=1)
            if usage:
                used = re.search(r'using (0\.[89]\d*) of space', usage[0].text).group(1)
                return done('radosbench-populate-overfill',
                            f'the cbt task ran rados bench write on pool {pool}{at(cbt[0])} '
                            f'with --no-cleanup for 60 s, which left the pool using {used[:4]} of '
                            f'the cluster space{at(usage[0])}, so an OSD crossed the nearfull ratio '
                            f'and {reporter or "the mon"} raised {check}; the test does not ignore '
                            'it in its log-ignorelist (tracker 74518)',
                            cbt[0], usage[0],
                            key='nearfull: cbt radosbench write fills the pool',
                            pool=pool)
    return unknown('none of the causes this node knows')
