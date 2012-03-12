import contextlib
import logging
import yaml

from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def sequential_test(ctx, config):
    """Example contextmanager that executes a command on remote hosts sequentially."""
    log.info('running sequential test...')
    for client in config:
        """Create a cluster composed of a single remote host associated with a client, and run the command on it."""
        ctx.cluster.only(client).run(args=['sleep', '5', run.Raw(';'), 'date', run.Raw(';'), 'hostname'])
    yield

@contextlib.contextmanager
def parallel_test(ctx, config):
    """Example contextmanager that executes a command on remote hosts in parallel."""
    log.info('running parallel test...')
    """Create a cluster of remote hosts composed of all hosts that have a role starting with 'client'"""
    cluster = ctx.cluster.only(lambda role: role.startswith('client'))
    nodes = {}
    for remote in cluster.remotes.iterkeys():
        """Call run for each remote host, but use 'wait=False' to have it return immediately."""
        proc = remote.run(args=['sleep', '5', run.Raw(';'), 'date', run.Raw(';'), 'hostname'], wait=False,)
        nodes[remote.name] = proc
    for name, proc in nodes.iteritems():
        """Wait for each process to finish before yielding and allowing other contextmanagers to run."""
        proc.exitstatus.get()
    yield

@contextlib.contextmanager
def task(ctx, config):
    """This is the main body of the task that gets run."""

    """Take car of some yaml parsing here"""
    if config is not None and not isinstance(config, list) and not isinstance(config, dict):
        assert(false), "task parallel_example only supports a list or dictionary for configuration"
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
