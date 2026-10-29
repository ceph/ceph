import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'yuriw-2026-07-28_17:11:12-rados-wip-yuri3-testing-20260728.135610-main-distro-default-trial/406329': 'crash/client/unnamed/ceph-bluestore-tool-avl-assert',
    'jayaprakash-2026-06-26_11:26:01-rados-wip-jaya-bs-testing-25-06-2026-distro-default-trial/318075': 'crash/client/unnamed/balancer-osd-weight-assert',
    'skanta-2026-03-25_22:46:45-rados-wip-bharath8-testing-2026-03-25-1848-distro-default-trial/122164': 'crash/client/unnamed/ceph-kvstore-tool-close-db-segfault',
    'skanta-2026-04-09_03:57:47-rados-wip-bharath16-testing-2026-04-08-1405-distro-default-trial/141133': 'crash/client/unnamed/ceph-bluestore-tool-avl-inconsistent-tree',
}

# The daemon a core came from, in the command line file(1) gives for it: a standalone
# test runs "ceph-mgr --id x", the ceph task "ceph-mgr -f --cluster ceph -i x". Its
# output is not in teuthology.log, so what it said is in its own log.
CORE_DAEMON = re.compile(r"core looks like: .*? from '\S*ceph-(mon|mgr|osd|mds)\b.*?(?:--id|-i) (\w+)")


@node('crash/client/unnamed', 'Which program crashed?')
def crash_client_unnamed(ctx):
    avl_assert = ctx.search('teuthology.log',
                            r'AvlAllocator\.cc.*FAILED ceph_assert\(offset \+ length <= uint64_t\(device_size\)\)',
                            'AvlAllocator.cc', 'offset + length')
    if avl_assert:
        expand = ctx.search('teuthology.log',
                            r'ceph-bluestore-tool\b.*bluefs-bdev-expand',
                            'ceph-bluestore-tool', 'bluefs-bdev-expand')
        return done('ceph-bluestore-tool-avl-assert',
                    'ceph-bluestore-tool bluefs-bdev-expand crashed: '
                    'FAILED ceph_assert(offset + length <= uint64_t(device_size)) in AvlAllocator.cc'
                    + (at(expand[0]) if expand else ''),
                    avl_assert[0], expand[0] if expand else None,
                    key='ceph-bluestore-tool bluefs-bdev-expand: AvlAllocator assert offset+length')
    segv = ctx.search('teuthology.log', r'stderr:\*\*\* Caught signal \(Segmentation fault\)',
                      'Caught signal (Segmentation fault)')
    if segv:
        thread = ctx.search('teuthology.log', r'thread_name:ceph-kvstore-to', 'thread_name:ceph-kvstore-to')
        iterator = ctx.search('teuthology.log', r'\(RocksDBStore::get_wholespace_iterator\(',
                              'get_wholespace_iterator')
        umount = ctx.search('teuthology.log', r'\(BlueStore::umount\(\)', 'BlueStore::umount')
        if thread and iterator and umount:
            return done('ceph-kvstore-tool-close-db-segfault',
                        f'ceph-kvstore-tool got a segmentation fault{at(segv[0])} while closing its '
                        'BlueStore: BlueStore::umount() > _close_db() > '
                        'RocksDBStore::get_wholespace_iterator, on a db that was opened with '
                        'open_db_environment()',
                        segv[0], iterator[0], umount[0],
                        key='ceph-kvstore-tool: SIGSEGV in BlueStore::umount > RocksDBStore::get_wholespace_iterator',
                        program='ceph-kvstore-tool', signal='Segmentation fault',
                        function='RocksDBStore::get_wholespace_iterator')
    avl_tree = ctx.search('teuthology.log',
                          r'AvlAllocator\.cc.*FAILED ceph_assert\(rs_before == range_tree\.end\(\) \|\| rs_before->end <= start\)',
                          'AvlAllocator.cc', 'rs_before')
    if avl_tree:
        expand = ctx.search('teuthology.log',
                            r'ceph-bluestore-tool\b.*bluefs-bdev-expand',
                            'ceph-bluestore-tool', 'bluefs-bdev-expand')
        frame = ctx.search('teuthology.log', r'BlueStore::expand_devices',
                           'BlueStore::expand_devices')
        if expand and frame:
            return done('ceph-bluestore-tool-avl-inconsistent-tree',
                        'ceph-bluestore-tool bluefs-bdev-expand crashed: FAILED '
                        'ceph_assert(rs_before == range_tree.end() || rs_before->end <= start) '
                        'in AvlAllocator::_add_to_tree, called from BlueStore::expand_devices'
                        + at(expand[0]) + ': the expanded space was added to the allocator '
                        'again after allocmap recovery had already marked it free',
                        avl_tree[0], expand[0], frame[0],
                        key='ceph-bluestore-tool bluefs-bdev-expand: AvlAllocator assert rs_before inconsistent tree')
    for core in ctx.search('teuthology.log', CORE_DAEMON.pattern, 'core looks like: '):
        kind, name = CORE_DAEMON.search(core.text).groups()
        who = f'{kind}.{name}'
        log = f'remote/*/log/*{who}.log*'
        weight_assert = ctx.search(log, r'OSDMap\.cc: \d+: FAILED ceph_assert\(osd_weight\.count\(oid\)\)',
                                   'FAILED ceph_assert(osd_weight.count(oid))')
        if not weight_assert:
            continue
        where = ctx.search(log, r"In function '.*OSDMap::calc_deviations\(",
                           "In function '", 'OSDMap::calc_deviations(')
        caller = ctx.search(log, r'\(OSDMap::calc_pg_upmaps\(.*\+0x[0-9a-f]+\) \[0x',
                            'OSDMap::calc_pg_upmaps(')
        time = at(where[0]) if where else ''
        via = ', from OSDMap::calc_pg_upmaps in the balancer' if caller else ''
        return done('balancer-osd-weight-assert',
                    f'{who} dumped core{time}: FAILED ceph_assert(osd_weight.count(oid)) in '
                    f'OSDMap::calc_deviations{via}: a PG still maps to an OSD that the upmap '
                    'calculation left out of its weights, such as one with crush weight 0',
                    core, where[0] if where else None, weight_assert[0],
                    caller[0] if caller else None,
                    key=f'{kind} osd/OSDMap.cc FAILED ceph_assert(osd_weight.count(oid)) in '
                        'OSDMap::calc_deviations',
                    daemon=who, daemon_type=kind, source='osd/OSDMap.cc',
                    message='FAILED ceph_assert(osd_weight.count(oid))',
                    function='OSDMap::calc_deviations')
    return unknown('none of the programs this node knows crashed here')
