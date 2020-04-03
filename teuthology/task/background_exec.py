"""
Background task
"""

import contextlib
import logging

from teuthology import misc
from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Run a background task.

    Run the given command on a client, similar to exec.  However, when
    we hit the finally because the subsequent task is ready to exit, kill
    the child process.

    We do not do any error code checking here since we are forcefully killing
    off the child when we are done.

    If the command a list, we simply join it with ;'s.

    Example::

       tasks:
       - install:
       - background_exec:
           client.0: while true ; do date ; sleep 1 ; done
           client.1:
             - while true
             - do id
             - sleep 1
             - done
       - exec:
           client.0:
             - sleep 10

    """
    assert isinstance(config, dict), "task background got invalid config"

    testdir = misc.get_testdir(ctx)

    tasks = {}
    for role, cmd in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Running background command on role %s host %s', role,
                 remote.name)
        if isinstance(cmd, list):
            cmd = '; '.join(cmd)
        cmd.replace('$TESTDIR', testdir)
        tasks[remote.name] = remote.run(
            args=[
                'sudo',
                'TESTDIR=%s' % testdir,
                'daemon-helper', 'kill', '--kill-group',
                'bash', '-c', cmd,
            ],
            wait=False,
            stdin=run.PIPE,
            check_status=False,
            logger=log.getChild(remote.name)
        )

    try:
        yield

    finally:
        for name, task in tasks.items():
            log.info('Stopping background command on %s', name)
            task.stdin.close()
        run.wait(tasks.values())
