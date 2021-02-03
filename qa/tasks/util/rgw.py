import logging
import json
import time

from io import StringIO

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False,
             format='json', decode=True, log_level=logging.DEBUG):
    log.info('rgwadmin: {client} : {cmd}'.format(client=client,cmd=cmd))
    testdir = teuthology.get_testdir(ctx)
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    client_with_id = daemon_type + '.' + client_id
    pre = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'radosgw-admin',
        '--log-to-stderr',
        '--format', format,
        '-n',  client_with_id,
        '--cluster', cluster_name,
        ]
    pre.extend(cmd)
    log.log(log_level, 'rgwadmin: cmd=%s' % pre)
    (remote,) = ctx.cluster.only(client).remotes.keys()
    proc = remote.run(
        args=pre,
        check_status=check_status,
        stdout=StringIO(),
        stderr=StringIO(),
        stdin=stdin,
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    if not decode:
        return (r, out)
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.log(log_level, ' json result: %s' % j)
        except ValueError:
            j = out
            log.log(log_level, ' raw result: %s' % j)
    return (r, j)

def get_user_summary(out, user):
    """Extract the summary for a given user"""
    user_summary = None
    for summary in out['summary']:
        if summary.get('user') == user:
            user_summary = summary

    if not user_summary:
        raise AssertionError('No summary info found for user: %s' % user)

    return user_summary

def get_user_successful_ops(out, user):
    summary = out['summary']
    if len(summary) == 0:
        return 0
    return get_user_summary(out, user)['total']['successful_ops']

def wait_for_radosgw(url, remote):
    """ poll the given url until it starts accepting connections

    add_daemon() doesn't wait until radosgw finishes startup, so this is used
    to avoid racing with later tasks that expect radosgw to be up and listening
    """
    # TODO: use '--retry-connrefused --retry 8' when teuthology is running on
    # Centos 8 and other OS's with an updated version of curl
    curl_cmd = ['curl',
                url]
    exit_status = 0
    num_retries = 8
    for seconds in range(num_retries):
        proc = remote.run(
            args=curl_cmd,
            check_status=False,
            stdout=StringIO(),
            stderr=StringIO(),
            stdin=StringIO(),
            )
        exit_status = proc.exitstatus
        if exit_status == 0:
            break
        time.sleep(2**seconds)

    assert exit_status == 0
