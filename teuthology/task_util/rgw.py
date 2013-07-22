from cStringIO import StringIO
import logging
import json

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False):
    log.info('radosgw-admin: %s' % cmd)
    testdir = teuthology.get_testdir(ctx)
    pre = [
        '{tdir}/adjust-ulimits'.format(tdir=testdir),
        'ceph-coverage'.format(tdir=testdir),
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'radosgw-admin'.format(tdir=testdir),
        '--log-to-stderr',
        '--format', 'json',
        ]
    pre.extend(cmd)
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    proc = remote.run(
        args=pre,
        check_status=check_status,
        stdout=StringIO(),
        stderr=StringIO(),
        stdin=stdin,
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.info(' json result: %s' % j)
        except ValueError:
            j = out
            log.info(' raw result: %s' % j)
    return (r, j)
