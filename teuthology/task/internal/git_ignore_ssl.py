import contextlib
import logging

from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def git_ignore_ssl(ctx, config):
    """
    Ignore ssl error's while cloning from untrusted http
    """

    log.info("ignoring ssl errors while cloning http repo")
    ctx.cluster.run(
            args=[
                  'sudo', 'git', 'config', run.Raw('--system'),
                  'http.sslverify', 'false'
                ],
        )
    yield
