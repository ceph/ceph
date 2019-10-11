"""
Parallel contextmanager test
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def sequential_test(ctx, config):
    """Example contextmanager that executes a command on remote hosts sequentially."""
    for role in config:
        """Create a cluster composed of all hosts with the given role, and run the command on them sequentially."""
        log.info('Executing command on all hosts sequentially with role "%s"' % role)
        ctx.cluster.only(role).run(args=['sleep', '5', run.Raw(';'), 'date', run.Raw(';'), 'hostname'])
    yield

@contextlib.contextmanager
def parallel_test(ctx, config):
    """Example contextmanager that executes a command on remote hosts in parallel."""
    for role in config:
        """Create a cluster composed of all hosts with the given role, and run the command on them concurrently."""
        log.info('Executing command on all hosts concurrently with role "%s"' % role)
        cluster = ctx.cluster.only(role)
        nodes = {}
        for remote in cluster.remotes.keys():
            """Call run for each remote host, but use 'wait=False' to have it return immediately."""
            proc = remote.run(args=['sleep', '5', run.Raw(';'), 'date', run.Raw(';'), 'hostname'], wait=False,)
            nodes[remote.name] = proc
        for name, proc in nodes.items():
            """Wait for each process to finish before yielding and allowing other contextmanagers to run."""
            proc.wait()
    yield

@contextlib.contextmanager
def task(ctx, config):
    """This is the main body of the task that gets run."""

    """Take car of some yaml parsing here"""
    if config is not None and not isinstance(config, list) and not isinstance(config, dict):
        assert(False), "task parallel_example only supports a list or dictionary for configuration"
    if config is None:
        config = ['client.{id}'.format(id=id_)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    """Run Multiple contextmanagers sequentially by nesting them."""
    with contextutil.nested(
        lambda: parallel_test(ctx=ctx, config=clients),
        lambda: sequential_test(ctx=ctx, config=clients),
        ):
        yield
