from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-10-03_03:26:16-rados-wip-bharath19-testing-20261002.123021-main-distro-default-trial/603575': 'crash/client/ceph_test_rados/dispatch-delay-op-timeout',
}


@node('crash/client/ceph_test_rados', 'What made ceph_test_rados abort?')
def crash_client_ceph_test_rados(ctx):
    timed_out = ctx.search('teuthology.log',
                           r'Error: oid \d+ .*returned (error code|different error codes:).*-110\b'
                           r'|stderr:err -110$|Assertion details: r = -110', '-110')
    if not timed_out:
        return unknown('ceph_test_rados did not abort on -110')
    delay = ctx.search('teuthology.log',
                       r'dispatch[ _]delay[ _]probability: 0?\.0*[1-9]',
                       'delay probability', 'delay_probability')
    compression = ctx.search('teuthology.log',
                             r'bluestore[ _]compression[ _]mode: (aggressive|force)',
                             'compression mode', 'compression_mode')
    if delay and compression:
        return done('dispatch-delay-op-timeout',
                    f'ceph_test_rados got -110 (ETIMEDOUT) from the client osd op timeout'
                    f'{at(timed_out[0])} and aborted: the OSDs had an injected dispatch delay '
                    'with compression\'s single op shard, which starves client ops on the op '
                    'queue under thrashing', timed_out[0], delay[0], compression[0],
                    key='ceph_test_rados: -110 with osd dispatch delay injection and compression',
                    program='ceph_test_rados', error='-110')
    return unknown('none of the failures this node knows')
