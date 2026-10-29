import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at, failure

EXAMPLES = {
    'skanta-2026-09-20_02:51:43-rados-wip-bharath22-testing-20260920.002814-main-distro-default-trial/570342': 'timeout/slow-ops/osd-auth-failure',
    'skanta-2026-10-17_23:46:38-rados-wip-bharath17-testing-20261016.230103-main-distro-default-trial/651179': 'timeout/slow-ops/no-recovery-progress',
    'skanta-2026-06-04_15:14:33-rados-wip-bharath7-testing-2026-06-04-1630-distro-default-trial/265644': 'timeout/slow-ops/no-recovery-progress',
    'skanta-2026-09-18_23:45:18-rados-wip-bharath3-testing-20260918.154843-main-distro-default-trial/568204': 'timeout/slow-ops/no-recovery-progress',
    'skanta-2026-04-26_13:06:36-rados-wip-bharath20-testing-2026-04-26-1249-squid-distro-default-trial/178964': 'timeout/slow-ops/slow-ping-time',
}


@node('timeout/slow-ops', 'What blocked the ops, so that the wait timed out?')
def timeout_slow_ops(ctx):
    wait = ctx.facts.get('wait', '')
    if wait == 'wait_until_healthy':
        # The health polls just before the wait gave up still show the slow
        # heartbeat checks as raised: the nodes cannot reach each other quickly.
        fail = failure(ctx)
        if not fail:
            return unknown('none of the failures this node knows')
        window = ctx.between(max(1, fail.line - 300), fail.line)
        ping = [h for h in window if re.search(
            r'"OSD_SLOW_PING_TIME_(BACK|FRONT)":\{"severity":"HEALTH_WARN"', h.text)]
        if ping:
            return done(
                'slow-ping-time',
                f'{wait} timed out because OSD_SLOW_PING_TIME_BACK/FRONT stayed raised in '
                f'every health poll up to the end{at(ping[-1])}: the OSDs\' heartbeat pings '
                'took far too long (slow nodes or network), and the slow ops were a symptom',
                ping[-1],
                key='wait_until_healthy: OSD_SLOW_PING_TIME still raised',
                wait=wait,
            )
        return unknown('none of the failures this node knows')
    if wait not in ('wait_for_recovery', 'wait_for_clean'):
        return unknown('none of the failures this node knows')
    bad_auth = ctx.search(
        'remote/*/log/ceph-osd.*.log.gz',
        r'could not find secret_id=\d+|could not get service secret for service osd secret_id=\d+|handle_auth_bad_method.*\(13\) Permission denied',
        'could not find secret_id',
        'could not get service secret',
        'handle_auth_bad_method',
    )
    if bad_auth:
        # Stray auth errors of revived OSDs are not the cause: the monitors must
        # also have marked several OSDs down for lack of beacons (a cascade of
        # lockouts), not just one.
        no_beacon = ctx.search(
            'remote/*/log/ceph-mon.*.log.gz',
            r'no beacon from osd',
            'no beacon from osd',
            max_hits=20,
        )
        marked = set()
        for hit in no_beacon:
            found = re.search(r'no beacon from (osd\.\d+)', hit.text)
            if found:
                marked.add(found.group(1))
        if len(marked) >= 2:
            return done(
                'osd-auth-failure',
                f'OSDs failed authentication with monitors{at(bad_auth[0])}: auth secret rotation '
                'prevented ticket renewal, causing OSDs to stop beacons and be marked down',
                bad_auth[0],
                no_beacon[0],
                key='OSD auth failure: secret_id not found',
                wait=wait,
            )
    no_progress = ctx.search(
        'teuthology.log',
        r'no progress seen, keeping timeout for now',
        'no progress seen',
    )
    if no_progress:
        return done(
            'no-recovery-progress',
            f'{wait} timed out: the cluster made no recovery progress despite slow ops'
            f'{at(no_progress[0])}',
            no_progress[0],
            key=f'{wait}: no recovery progress',
            wait=wait,
        )
    return unknown('none of the failures this node knows')
