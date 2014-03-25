import logging

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def rados(ctx, remote, cmd, wait=True, check_status=False):
    testdir = teuthology.get_testdir(ctx)
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'rados',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        check_status=check_status,
        wait=wait,
        )
    if wait:
        return proc.exitstatus
    else:
        return proc

def create_ec_pool(remote, name, profile_name, pgnum, m=1, k=2):
    remote.run(args=[
        'ceph', 'osd', 'erasure-code-profile', 'set',
        profile_name, 'm=' + str(m), 'k=' + str(k),
        'ruleset-failure-domain=osd',
        ])
    remote.run(args=[
        'ceph', 'osd', 'pool', 'create', name,
        str(pgnum), str(pgnum), 'erasure', profile_name,
        ])

def create_replicated_pool(remote, name, pgnum):
    remote.run(args=[
        'ceph', 'osd', 'pool', 'create', name, str(pgnum), str(pgnum),
        ])
