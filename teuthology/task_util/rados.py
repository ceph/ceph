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
