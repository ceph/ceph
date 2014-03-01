"""
Handle parallel execution on remote hosts
"""
import logging

from teuthology import misc as teuthology
from teuthology.parallel import parallel
from teuthology.orchestra import run as tor

log = logging.getLogger(__name__)

from gevent import queue as queue
from gevent import event as event

def _init_barrier(barrier_queue, remote):
    """current just queues a remote host""" 
    barrier_queue.put(remote)

def _do_barrier(barrier, barrier_queue, remote):
    """special case for barrier"""
    barrier_queue.get()
    if barrier_queue.empty():
        barrier.set()
        barrier.clear()
    else:
        barrier.wait()

    barrier_queue.put(remote)
    if barrier_queue.full():
        barrier.set()
        barrier.clear()
    else:
        barrier.wait()

def _exec_host(barrier, barrier_queue, remote, sudo, testdir, ls):
    """Execute command remotely"""
    log.info('Running commands on host %s', remote.name)
    args = [
        'TESTDIR={tdir}'.format(tdir=testdir),
        'bash',
        '-s'
        ]
    if sudo:
        args.insert(0, 'sudo')
    
    r = remote.run( args=args, stdin=tor.PIPE, wait=False)
    r.stdin.writelines(['set -e\n'])
    r.stdin.flush()
    for l in ls:
        l.replace('$TESTDIR', testdir)
        if l == "barrier":
            _do_barrier(barrier, barrier_queue, remote)
            continue

        r.stdin.writelines([l, '\n'])
        r.stdin.flush()
    r.stdin.writelines(['\n'])
    r.stdin.flush()
    r.stdin.close()
    tor.wait([r])

def _generate_remotes(ctx, config):
    """Return remote roles and the type of role specified in config"""
    if 'all' in config and len(config) == 1:
        ls = config['all']
        for remote in ctx.cluster.remotes.iterkeys():
            yield (remote, ls)
    elif 'clients' in config:
        ls = config['clients']
        for role in teuthology.all_roles_of_type(ctx.cluster, 'client'):
            remote = teuthology.get_single_remote_value(ctx,
                    'client.{r}'.format(r=role))
            yield (remote, ls)
        del config['clients']
        for role, ls in config.iteritems():
            remote = teuthology.get_single_remote_value(ctx, role)
            yield (remote, ls)
    else:
        for role, ls in config.iteritems():
            remote = teuthology.get_single_remote_value(ctx, role)
            yield (remote, ls)

def task(ctx, config):
    """
    Execute commands on multiple hosts in parallel

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

    Execute commands on all hosts in the cluster in parallel.  This
    is useful if there are many hosts and you want to run the same
    command on all:

        tasks:
        - pexec:
            all:
              - grep FAIL /var/log/ceph/*

    Or if you want to run in parallel on all clients:

        tasks:
        - pexec:
            clients:
              - dd if=/dev/zero of={testdir}/mnt.* count=1024 bs=1024

    You can also ensure that parallel commands are synchronized with the
    special 'barrier' statement:

    tasks:
    - pexec:
        clients:
          - cd {testdir}/mnt.*
          - while true; do
          -   barrier
          -   dd if=/dev/zero of=./foo count=1024 bs=1024
          - done

    The above writes to the file foo on all clients over and over, but ensures that
    all clients perform each write command in sync.  If one client takes longer to
    write, all the other clients will wait.

    """
    log.info('Executing custom commands...')
    assert isinstance(config, dict), "task pexec got invalid config"

    sudo = False
    if 'sudo' in config:
        sudo = config['sudo']
        del config['sudo']

    testdir = teuthology.get_testdir(ctx)

    remotes = list(_generate_remotes(ctx, config))
    count = len(remotes)
    barrier_queue = queue.Queue(count)
    barrier = event.Event()

    for remote in remotes:
        _init_barrier(barrier_queue, remote[0])
    with parallel() as p:
        for remote in remotes:
            p.spawn(_exec_host, barrier, barrier_queue, remote[0], sudo, testdir, remote[1])
