
"""
watch_notify_same_primary task
"""
from cStringIO import StringIO
import contextlib
import logging

from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Run watch_notify_same_primary

    The config should be as follows:

    watch_notify_same_primary:
        clients: [client list]

    The client list should contain 1 client

    The test requires 3 osds.

    example:

    tasks:
    - ceph:
    - watch_notify_same_primary:
        clients: [client.0]
    - interactive:
    """
    log.info('Beginning watch_notify_same_primary...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    clients = config.get('clients', ['client.0'])
    assert len(clients) == 1
    role = clients[0]
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    ctx.manager.raw_cluster_cmd('osd', 'set', 'noout')

    pool = ctx.manager.create_pool_with_unique_name()
    def obj(n): return "foo-{num}".format(num=n)
    def start_watch(n):
        remote.run(
            args = [
                "rados",
                "-p", pool,
                "put",
                obj(n),
                "/etc/resolv.conf"],
            logger=log.getChild('watch.{id}'.format(id=n)))
        return remote.run(
            args = [
                "rados",
                "-p", pool,
                "watch",
                obj(n)],
            stdin=run.PIPE,
            stdout=StringIO(),
            stderr=StringIO(),
            wait=False)
    watches = [start_watch(i) for i in range(20)]

    def notify(n, msg):
        remote.run(
            args = [
                "rados",
                "-p", pool,
                "notify",
                obj(n),
                msg],
            logger=log.getChild('notify.{id}'.format(id=n)))

    [notify(n, 'notify1') for n in range(len(watches))]

    ctx.manager.kill_osd(0)
    ctx.manager.mark_down_osd(0)

    [notify(n, 'notify2') for n in range(len(watches))]

    try:
        yield
    finally:
        log.info('joining watch_notify_stress')
        for watch in watches:
            watch.stdin.write("\n")

        run.wait(watches)

        for watch in watches:
            lines = watch.stdout.getvalue().split("\n")
            got1 = False
            got2 = False
            for l in lines:
                if 'notify1' in l:
                    got1 = True
                if 'notify2' in l:
                    got2 = True
            log.info(lines)
            assert got1 and got2

        ctx.manager.revive_osd(0)
        ctx.manager.remove_pool(pool)
