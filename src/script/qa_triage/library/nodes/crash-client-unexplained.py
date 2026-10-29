import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at, failure, short

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-04-30_03:32:50-rados-wip-bharath4-testing-2026-04-29-1417-tentacle-distro-default-trial/186841': 'crash/client/unexplained/dispatch-delay-op-timeout',
    'skanta-2026-10-17_23:46:38-rados-wip-bharath17-testing-20261016.230103-main-distro-default-trial/650895': 'crash/client/unexplained/gtest-failure-did-bind',
    'skanta-2026-10-03_10:12:06-rados-wip-bharath19-testing-20261002.123021-main-distro-default-trial/604392': 'crash/client/unexplained/dispatch-delay-op-timeout',
    'mattywilliams-2026-09-16_14:35:09-rbd-main-distro-default-trial/560984': 'crash/client/unexplained/ec-optimized-out-of-order-op',
    'pdonnell-2026-08-27_01:45:31-rados-wip-pdonnell-testing-20260826.223123-squid-distro-default-trial/518763': 'crash/client/unexplained/old-client-aes256k-keyring',
}

# An OSD's abort on "out of order op", with or without a cluster name before the
# daemon (tasks.ceph.cluster1.osd.1...), which the crash node does not read as a
# daemon's output.
OUT_OF_ORDER = (r'tasks\.ceph\.(?:([\w-]+)\.)?(osd\.\d+)\.[\w-]+\.stderr:.*/src/osd/'
                r'PrimaryLogPG\.cc: \d+: ceph_abort_msg\("out of order op"\)')
# The op order check's message, on an EC shard pg (pg[2.7s0(...).
BAD_ORDER = (r'(osd\.\d+) pg_epoch: \d+ pg\[(\d+\.[0-9a-f]+)s\d+\(.*\] bad op order, '
             r'already applied (\d+) > this (\d+)')
EC_OPTIMIZATIONS = r'allow_ec_optimizations( to)? true|flag[ _]ec[ _]optimizations:? true'
# A test of an old release's build failing in its own check, and the connect error.
OLD_TEST_FAILURE = r'/ceph-(\d+\.\d+\.\d+)/src/test/cls_rbd/test_cls_rbd\.cc:(\d+): Failure'
CONNECT_EIO = r'cluster\.connect failed with error -5\b'
AES256K = r'auth_preferred_cipher\W+(name\W+)?aes256k'


def ec_out_of_order_op(ctx):
    """done if an OSD aborted on "out of order op" on an EC pg with Fast EC on,
    before the client failed; None otherwise."""
    aborted = ctx.search('teuthology.log', OUT_OF_ORDER, 'ceph_abort_msg("out of order op")')
    if not aborted:
        return None
    fail = failure(ctx)
    if fail and aborted[0].line > fail.line:
        return None
    bad = ctx.search('teuthology.log', BAD_ORDER, 'bad op order')
    optimized = ctx.search('teuthology.log', EC_OPTIMIZATIONS, 'ec_optimizations',
                           'ec optimizations', max_hits=1)
    if not bad or not optimized:
        return None
    who = re.search(OUT_OF_ORDER, aborted[0].text)
    order = re.search(BAD_ORDER, bad[0].text)
    cluster, osd = who.group(1) or 'ceph', who.group(2)
    pg, applied, this = order.group(2), order.group(3), order.group(4)
    program = ctx.facts.get('program', 'the client')
    return done('ec-optimized-out-of-order-op',
                f'{osd} of {cluster} aborted on ceph_abort_msg("out of order op") in '
                f'PrimaryLogPG::execute_ctx{at(aborted[0])}: on EC pg {pg}, with '
                f'allow_ec_optimizations, op {this} reached it after {applied} was applied, '
                'reordered while ops waited behind a Fast EC class-call coroutine; with its '
                f'OSDs down, {program} then failed', bad[0], aborted[0], optimized[0],
                key='osd PrimaryLogPG.cc ceph_abort_msg("out of order op") on an EC pg '
                    'with allow_ec_optimizations',
                daemon=osd, cluster=cluster, source='osd/PrimaryLogPG.cc',
                message='ceph_abort_msg("out of order op")', pg=pg)


def old_client_aes256k_keyring(ctx):
    """done if ceph_test_cls_rbd, built from an old release, failed to connect with
    -EIO in its set-up on a cluster whose cipher is aes256k; None otherwise."""
    failed = ctx.search('teuthology.log', OLD_TEST_FAILURE, 'test_cls_rbd.cc')
    if not failed:
        return None
    connect = ctx.search('teuthology.log', CONNECT_EIO, 'cluster.connect failed with error -5')
    if not connect or connect[0].line < failed[0].line:
        return None
    cipher = ctx.search('teuthology.log', AES256K, 'auth_preferred_cipher', max_hits=1)
    if not cipher:
        return None
    found = re.search(OLD_TEST_FAILURE, failed[0].text)
    version, line = found.group(1), found.group(2)
    return done('old-client-aes256k-keyring',
                f'the ceph_test_cls_rbd of release {version} failed at test_cls_rbd.cc:{line}'
                f'{at(connect[0])} with "cluster.connect failed with error -5" (-EIO): the '
                'cluster uses the aes256k cipher, so the keyring the client reads holds a key '
                'type the old client cannot decode, and librados connect() returned the '
                'keyring load error before any I/O; the test binary then failed',
                failed[0], connect[0], cipher[0],
                key='ceph_test_cls_rbd of an old release: cluster.connect failed with error -5 '
                    'on an aes256k cluster',
                program='ceph_test_cls_rbd', error='-5', cipher='aes256k')


@node('crash/client/unexplained', 'Why did the program fail?')
def crash_client_unexplained(ctx):
    found = ec_out_of_order_op(ctx)
    if found:
        return found
    if ctx.facts.get('program') == 'ceph_test_cls_rbd':
        found = old_client_aes256k_keyring(ctx)
        if found:
            return found
    if ctx.facts.get('program') == 'ceph_test_msgr':
        failed = ctx.search('teuthology.log', r'src/test/msgr/test_msgr\.cc:(\d+): Failure',
                            'test_msgr.cc:')
        bound = ctx.search('teuthology.log', r'AsyncMessenger\.cc: \d+: FAILED ceph_assert\(!did_bind\)',
                           'did_bind')
        if failed and bound and failed[0].line < bound[0].line:
            line = re.search(r'test_msgr\.cc:(\d+): Failure', failed[0].text).group(1)
            expected = ctx.between(failed[0].line + 1, failed[0].line + 2)
            what = short(re.sub(r'^.*?\.stdout:', '', expected[-1].text)) if expected else ''
            runs = ctx.search('teuthology.log', r'\[ RUN      \] (\S+)', '[ RUN      ]',
                              max_hits=200)
            before = [h for h in runs if h.line < failed[0].line]
            test = re.search(r'\[ RUN      \] (\S+)', before[-1].text).group(1) if before else 'a test'
            return done('gtest-failure-did-bind',
                        f'{test} failed its own check at test_msgr.cc:{line}{at(failed[0])}'
                        + (f' ({what})' if what else '') + ', returned before shutting its '
                        'messengers down, and ~AsyncMessenger then aborted on '
                        'ceph_assert(!did_bind)', failed[0], bound[0],
                        key=f'ceph_test_msgr: {test} failed at test_msgr.cc:{line}, then did_bind assert',
                        test=test, source_line=line)
    if ctx.facts.get('program') == 'ceph_test_rados':
        timed_out = ctx.search('teuthology.log',
                               r'Error: oid \d+ .*returned (error code|different error codes:).*-110\b'
                               r'|stderr:err -110$|Assertion details: r = -110', '-110')
        delay = ctx.search('teuthology.log',
                           r'dispatch[ _]delay[ _]probability: 0?\.0*[1-9]',
                           'delay probability', 'delay_probability')
        compression = ctx.search('teuthology.log',
                                 r'bluestore[ _]compression[ _]mode: (aggressive|force)',
                                 'compression mode', 'compression_mode')
        if timed_out and delay and compression:
            return done('dispatch-delay-op-timeout',
                        f'ceph_test_rados got -110 (ETIMEDOUT) from the client osd op timeout'
                        f'{at(timed_out[0])} and aborted: the OSDs had an injected dispatch delay '
                        'with compression\'s single op shard, which starves client ops on the op '
                        'queue under thrashing', timed_out[0], delay[0], compression[0],
                        key='ceph_test_rados: -110 with osd dispatch delay injection and compression',
                        program='ceph_test_rados', error='-110')
        if delay and compression:
            return done('dispatch-delay-op-timeout',
                        'ceph_test_rados hung waiting for in-flight ops and was killed'
                        f'{at(delay[0])}: the OSDs had an injected dispatch delay with '
                        'compression\'s single op shard, which starved the op queue and left '
                        'client ops stuck; without an osd op timeout the test could not abort '
                        'on -110 but waited until the watchdog killed it',
                        delay[0], compression[0],
                        key='ceph_test_rados: hung with osd dispatch delay injection and compression',
                        program='ceph_test_rados')
    return unknown('none of the failures this node knows')
