import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-10-12_16:32:46-rados-wip-bharath7-testing-20261012.100201-main-distro-default-trial/629034': 'cluster-log/message/fastec-read-enoent',
    'pdonnell-2026-10-19_23:53:32-rados-wip-pdonnell-testing-20261019.181708-main-distro-default-trial/658865': 'cluster-log/message/laggy-socket-failures',
    'skanta-2026-09-24_03:23:39-rados-wip-bharath10-testing-20260924.010810-main-distro-default-trial/577494': 'cluster-log/message/out-of-order-full',
    'skanta-2026-07-12_15:01:05-rados-wip-bharath4-testing-2026-07-09-1342-distro-default-trial/358458': 'cluster-log/message/pg-not-ready-to-merge',
    'skanta-2026-04-10_13:01:53-rados-wip-bharath4-testing-2026-04-10-1224-distro-default-trial/143035': 'cluster-log/message/mgr-session-evicted',
    'skanta-2026-06-08_15:50:32-rados-wip-bharath2-testing-2026-06-05-1542-distro-default-trial/278156': 'cluster-log/message/mgr-session-evicted',
    'skanta-2026-06-08_15:50:32-rados-wip-bharath2-testing-2026-06-05-1542-distro-default-trial/278203': 'cluster-log/message/mgr-session-evicted',
    'skanta-2026-06-08_15:58:51-rados-wip-bharath21-testing-2026-06-05-2111-tentacle-distro-default-trial/278317': 'cluster-log/message/mgr-session-evicted',
    'skanta-2026-06-09_02:42:07-rados-wip-bharath2-testing-2026-06-09-0416-distro-default-trial/280469': 'cluster-log/message/mgr-session-evicted',
    'skanta-2026-06-09_02:42:07-rados-wip-bharath2-testing-2026-06-09-0416-distro-default-trial/280624': 'cluster-log/message/mgr-session-evicted',
    'skanta-2026-06-10_03:59:16-rados-wip-bharath6-testing-2026-06-10-0655-squid-distro-default-trial/282280': 'cluster-log/message/mgr-session-evicted',
    'lflores-2026-04-17_22:03:40-rados-wip-lflores-testing-2-2026-04-08-1424-tentacle-distro-default-trial/159143': 'cluster-log/message/backfill-reject-injected',
    'pdonnell-2026-06-13_16:54:44-rados-wip-pdonnell-testing-20260613.133611-tentacle-distro-default-trial/292360': 'cluster-log/message/backfill-reject-injected',
    'jenkins-build-2026-10-18_06:04:45-rados-tentacle-distro-default-trial/652514': 'cluster-log/message/backfill-reject-injected',
}


@node('cluster-log/message', 'Why did the daemon log an error reading the object?')
def cluster_log_message(ctx):
    message = ctx.facts.get('message', '')
    reporter = ctx.facts.get('reporter', '')
    if 'Error -2 reading object' in message and 'TestSDKCoverageReplay' in message and 'FastEC' in message:
        coroutine_log = ctx.search(f'remote/*/log/ceph-{reporter}.log.gz',
                                   r'spawning a coroutine for EC optimized CALL op',
                                   'EC optimized CALL')
        if coroutine_log:
            delete_log = ctx.search(f'remote/*/log/ceph-{reporter}.log.gz',
                                    r'do_osd_op.*\[delete\]',
                                    '[delete]')
            if delete_log:
                return done('fastec-read-enoent',
                            f'{reporter} logged Error -2 (ENOENT) reading the object during '
                            f'TestSDKCoverageReplay on a FastEC pool: the EC-optimized CLS CALL '
                            f'coroutine issues a sync_read before cls_cxx_remove, but the OSD '
                            f'applies the delete EC sub-write locally before the earlier '
                            f'sync_read EC sub-read arrives at the primary shard, so bluestore '
                            f'returns -2 (ENOENT) \u2014 a product bug where reads and writes in an '
                            f'EC-optimized CLS CALL are reordered',
                            coroutine_log[0],
                            key='EC-optimized CLS CALL: sub-read after local delete on FastEC pool during TestSDKCoverageReplay')
    if reporter.startswith('osd.') and re.search(r'PG \S+ marked LAGGY', message):
        lease = ctx.search(f'remote/*/log/ceph-{reporter}.log.gz',
                           r'check_laggy mnow \S+ > readable_until',
                           'check_laggy mnow', max_hits=1)
        injected = ctx.search('teuthology.log', r'ms inject socket failures: \d+',
                              'ms inject socket failures', max_hits=1)
        if lease and injected:
            return done('laggy-socket-failures',
                        f'{reporter} marked a PG LAGGY{at(lease[0])}: its read lease ran out while '
                        'the job injected messenger socket failures, which broke the heartbeat '
                        'connections; the warning is benign and the PG recovers, but the '
                        "test's log-ignorelist does not cover 'marked LAGGY'",
                        lease[0], injected[0],
                        key='PG marked LAGGY: read lease expired under injected socket failures',
                        reporter=reporter)
    if 'OSD_OUT_OF_ORDER_FULL' in message and reporter.startswith('mon.'):
        set_ratio = ctx.search(f'remote/*/log/*/ceph-{reporter}.log.gz',
                               r'osd set-full-ratio',
                               'osd set-full-ratio', max_hits=1)
        if set_ratio:
            return done('out-of-order-full',
                        f'{reporter} logged OSD_OUT_OF_ORDER_FULL: the thrashosds task '
                        'called thrash_cluster_full(), setting osd full-ratio to 0.001, '
                        'which put the full ratios out of order; the health check fired but '
                        "the test's log-ignorelist only suppresses the parenthesised "
                        '"(OSD_...)" form, not the bare "OSD_OUT_OF_ORDER_FULL:" form',
                        set_ratio[0],
                        key='OSD_OUT_OF_ORDER_FULL: full ratio(s) out of order from thrash_cluster_full',
                        reporter=reporter)
    if ('not ready to merge' in message and 'backing off pg_num decrease' in message
            and reporter.startswith('mon.')):
        pg_log = ctx.search('remote/*/log/ceph.log.gz',
                            r'not ready to merge; backing off pg_num decrease',
                            'not ready to merge', max_hits=1)
        if pg_log:
            return done('pg-not-ready-to-merge',
                        f'{reporter} warned that an OSD reported a PG not ready to merge '
                        'while backing off pg_num decrease on a classic pool: this warning '
                        'was introduced for Crimson/seastore pool merges in '
                        'OSDMonitor::prepare_pg_ready_to_merge but was not gated on '
                        'crimson-only pools, so it fires incorrectly for classic EC pools '
                        'too (tracker 77459); the cluster log check treats any un-ignored '
                        '[WRN] as a failure',
                        pg_log[0],
                        key='mon: osd reported pg not ready to merge; backing off pg_num decrease',
                        reporter=reporter)
    if reporter.startswith('mds.') and re.search(r'^evicting unresponsive client \S+:[xyz] \(\d+\)', message):
        evicted = ctx.search('remote/*/log/ceph.log.gz',
                             r'cluster \[WRN\] evicting unresponsive client \S+:[xyz] \(\d+\)',
                             'evicting unresponsive client', max_hits=1)
        if not evicted:
            evicted = ctx.search(f'remote/*/log/ceph-{reporter}.log.gz',
                                 r'log \[WRN\] : evicting unresponsive client \S+:[xyz] \(\d+\)',
                                 'evicting unresponsive client', max_hits=1)
        if not evicted:
            evicted = ctx.search('teuthology.log',
                                 r'evicting unresponsive client \S+:[xyz] \(\d+\)',
                                 'evicting unresponsive client', max_hits=1)
        line = evicted[0] if evicted else ctx.first('reason')
        where = at(line) if line else ''
        return done('mgr-session-evicted',
                    f'{reporter} evicted an unresponsive client named after a mgr id{where}: '
                    "the mgr's libcephfs session was left behind when the mgr was respawned or "
                    'restarted (a test changed the enabled mgr modules or restarted the mgr), '
                    'and the MDS evicted it about 300 seconds later; the [WRN] is not in the '
                    "test's log-ignorelist (tracker 73278)",
                    line,
                    key='MDS evicted the unresponsive libcephfs session of a restarted mgr',
                    reporter=reporter)
    if reporter.startswith('mon.') and re.match(r'pg \S+ is \S*backfill_toofull, acting', message):
        injected = ctx.search('teuthology.log',
                              r'osd[ _]debug[ _]reject[ _]backfill[ _]probability: 0?\.[1-9]',
                              'reject backfill probability', 'reject_backfill_probability',
                              max_hits=1)
        if injected:
            return done('backfill-reject-injected',
                        f'{reporter} warned that a PG is backfill_toofull{at(injected[0])}: the job '
                        'injects random backfill reservation rejections (osd debug reject backfill '
                        'probability), which put the PG in backfill_toofull although the OSDs have '
                        "space; the mon's per-PG detail line is not in the test's log-ignorelist "
                        '(tracker 70716)',
                        injected[0],
                        key='PG backfill_toofull from injected backfill reservation rejection',
                        reporter=reporter)
    return unknown('none of the failures this node knows')
