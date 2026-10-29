import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

EXAMPLES = {
    'rkachach-2026-09-24_08:15:57-orch:cephadm-wip-rkachach-testing-2-2026-09-23-2352-distro-default-trial/579039': 'workunit/gtest/shard-bounced-direct-read',
    'skanta-2026-10-12_23:32:34-rados-wip-bharath20-testing-20261012.132505-main-distro-default-trial/631086': 'workunit/gtest/shard-bounced-direct-read',
    'skanta-2026-09-30_04:36:41-rados-wip-bharath14-testing-20260930.003013-main-distro-default-trial/593594': 'workunit/gtest/shard-bounced-direct-read',
    'skanta-2026-09-18_02:38:43-rados-wip-bharath24-testing-20260918.001059-main-distro-default-trial/565929': 'workunit/gtest/shard-bounced-direct-read',
    'jenkins-build-2026-06-25_06:03:13-smoke-main-distro-default-trial/314801': 'workunit/gtest/external-error-inject',
}

BOUNCE = 'unstable write on shard, bouncing to primary'
# At debug osd 5 the OSD does not log the bounce itself (dout 20), but at debug ms 1
# it logs the EAGAIN reply to the direct read (the one asking for internal versions)
# of one of the zero_trunc_* objects of LibRadosIoECPP.ZeroAndTruncateSameRequest.
BOUNCE_REPLY = (r'osd_op_reply\(\d+ zero_trunc_\w+ \[read [^\]]*get-internal-versions\]'
                r'[^)]*ondisk = -11 \(\(11\)')


def bounced(ctx):
    # cephadm stores OSD logs under a UUID subdirectory; try that layout first,
    # then fall back to the direct layout used by non-cephadm jobs.
    result = ctx.search('remote/*/log/*/ceph-osd.*.log.gz',
                        r'unstable write on shard, bouncing to primary',
                        BOUNCE, max_hits=1)
    if not result:
        result = ctx.search('remote/*/log/ceph-osd.*.log.gz',
                            r'unstable write on shard, bouncing to primary',
                            BOUNCE, max_hits=1)
    if not result:
        result = ctx.search('remote/*/log/*/ceph-osd.*.log.gz', BOUNCE_REPLY,
                            'zero_trunc_', max_hits=1)
    if not result:
        result = ctx.search('remote/*/log/ceph-osd.*.log.gz', BOUNCE_REPLY,
                            'zero_trunc_', max_hits=1)
    return result


@node('workunit/gtest', 'What made the gtest fail?')
def workunit_gtest(ctx):
    if ctx.facts.get('test', '').startswith('LibRadosIoECPPParamCombination/LibRadosIoECPP.'):
        counter = ctx.search('teuthology.log',
                             r"Perf counter 'objecter\.op_reply' has incorrect value",
                             "Perf counter 'objecter.op_reply' has incorrect value")
        if counter:
            bounce = bounced(ctx)
            if bounce:
                return done('shard-bounced-direct-read',
                            f'the EC direct read was bounced by a shard with an unstable write{at(bounce[0])}, '
                            'so the Objecter re-read the object through the primary and the test saw one '
                            'more objecter.op_reply than it expects: a race in the test, which reads right '
                            'after writing',
                            counter[0], bounce[0],
                            key='LibRadosIoECPP: op_reply count off after a shard bounced a direct read')
    # The suite injects errors itself while rados/test.sh runs without
    # --external-error-inject (78294).
    split = ctx.search('teuthology.log', r'Value of: AssertOperateWithSplitOp',
                       'Value of: AssertOperateWithSplitOp', max_hits=1)
    if split:
        sockets = ctx.search('teuthology.log', r'ms inject socket failures: \d+',
                             'ms inject socket failures', max_hits=1)
        if sockets and not bounced(ctx):
            return done('external-error-inject',
                        f'the split-op perf counter check (AssertOperateWithSplitOp) failed{at(split[0])} '
                        'while the suite injected socket failures and rados/test.sh ran without '
                        '--external-error-inject: the Objecter resent ops after dropped connections, '
                        'so the counts came out wrong',
                        split[0], sockets[0],
                        key='rados/test.sh: AssertOperateWithSplitOp counts off under injected socket failures',
                        injection='ms inject socket failures', check='AssertOperateWithSplitOp')
    omap = ctx.search('teuthology.log', r'api_omap_pp: \[  FAILED  \] OmapTest\.\w*Recovery\w*/',
                      'OmapTest.', max_hits=1)
    if omap:
        timeout = ctx.search('teuthology.log', r'api_omap_pp: Value of: res2 == 0',
                             'Value of: res2 == 0', max_hits=1)
        thrash = ctx.search('teuthology.log', r'INFO:tasks\.thrashosds\.thrasher:',
                            'tasks.thrashosds.thrasher:', max_hits=1)
        if timeout and thrash:
            name = re.search(r'OmapTest\.\w*Recovery\w*', omap[0].text).group(0)
            return done('external-error-inject',
                        f'{name} timed out waiting for its pg-upmap to take effect{at(timeout[0])} while '
                        f'thrashosds was thrashing the OSDs (its first action{at(thrash[0])}), and '
                        'rados/test.sh ran without --external-error-inject: the test swaps the PG\'s '
                        'primary with an upmap and waits 60 s for it, which the thrasher\'s pg_num, '
                        'primary-affinity and upmap changes keep from happening',
                        omap[0], timeout[0], thrash[0],
                        key='rados/test.sh: OmapTest recovery upmap wait timed out under thrashosds',
                        injection='thrashosds', check='wait_for_upmap')
    return unknown('none of the gtest failures this node knows')
