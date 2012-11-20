import contextlib
import logging
import os
from datetime import datetime

from teuthology import misc as teuthology
from teuthology.parallel import parallel
from teuthology.orchestra import run as tor

log = logging.getLogger(__name__)

def _exec_role(remote, role, sudo, ls):
    log.info('Running commands on role %s host %s', role, remote.name)
    cid=role.split('.')[1]
    args = ['bash', '-s']
    if sudo:
        args.insert(0, 'sudo')
    r = remote.run( args=args, stdin=tor.PIPE, wait=False)
    r.stdin.writelines(['set -e\n'])
    r.stdin.flush()
    r.stdin.writelines(['cd /tmp/cephtest/mnt.{cid}\n'.format(cid=cid)])
    r.stdin.flush()
    for l in ls:
        r.stdin.writelines([l, '\n'])
        r.stdin.flush()
    r.stdin.writelines(['\n'])
    r.stdin.flush()
    r.stdin.close()
    tor.wait([r])

def task(ctx, config):
    """
    Execute commands on multiple roles in parallel

        tasks:
        - ceph:
        - ceph-fuse: [client.0, client.1]
        - pexec:
            client.0:
              - while true; do echo foo >> bar; done
            client.1:
              - sleep 1
              - tail -f bar
        - interactive:

    """
    log.info('Executing custom commands...')
    assert isinstance(config, dict), "task pexec got invalid config"

    sudo = False
    if 'sudo' in config:
        sudo = config['sudo']
        del config['sudo']

    if 'all' in config and len(config) == 1:
        a = config['all']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles)

    with parallel() as p:
        for role, ls in config.iteritems():
            (remote,) = ctx.cluster.only(role).remotes.iterkeys()
            p.spawn(_exec_role, remote, role, sudo, ls)
