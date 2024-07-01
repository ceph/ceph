"""
Deploy and configure RabbitMQ for Teuthology
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def install_rabbitmq(ctx, config):
    """
    Downloading the RabbitMQ package.
    """
    assert isinstance(config, dict)
    log.info('Installing RabbitMQ...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        ctx.cluster.only(client).run(args=[
             'sudo', 'dnf', '-y', 'install', 'epel-release'
        ])

        link1 = 'https://packagecloud.io/install/repositories/rabbitmq/erlang/script.rpm.sh'

        ctx.cluster.only(client).run(args=[
             'curl', '-s', link1, run.Raw('|'), 'sudo', 'bash'
        ])

        ctx.cluster.only(client).run(args=[
             'sudo', 'dnf', '-y', 'install', 'erlang'
        ])

        link2 = 'https://packagecloud.io/install/repositories/rabbitmq/rabbitmq-server/script.rpm.sh'

        ctx.cluster.only(client).run(args=[
             'curl', '-s', link2, run.Raw('|'), 'sudo', 'bash'
        ])

        ctx.cluster.only(client).run(args=[
             'sudo', 'dnf', '-y', 'install', 'rabbitmq-server'
        ])

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of RabbitMQ...')

        for (client, _) in config.items():
            ctx.cluster.only(client).run(args=[
                 'sudo', 'dnf', '-y', 'remove', 'rabbitmq-server.noarch'
            ])


@contextlib.contextmanager
def run_rabbitmq(ctx, config):
    """
    This includes two parts:
    1. Starting Daemon
    2. Starting RabbitMQ service
    """
    assert isinstance(config, dict)
    log.info('Bringing up Daemon and RabbitMQ service...')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        ctx.cluster.only(client).run(args=[
             'sudo', 'systemctl', 'enable', 'rabbitmq-server.service'
            ],
        )

        ctx.cluster.only(client).run(args=[
             'sudo', '/sbin/service', 'rabbitmq-server', 'start'
            ],
        )

        '''
        # To check whether rabbitmq-server is running or not
        ctx.cluster.only(client).run(args=[
             'sudo', '/sbin/service', 'rabbitmq-server', 'status'
            ],
        )
        '''

    try:
        yield
    finally:
        log.info('Stopping RabbitMQ Service...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            ctx.cluster.only(client).run(args=[
                 'sudo', '/sbin/service', 'rabbitmq-server', 'stop'
                ],
            )


@contextlib.contextmanager
def task(ctx,config):
    """
    To run rabbitmq the prerequisite is to run the tox task. Following is the way how to run
    tox and then rabbitmq::
    tasks:
    - rabbitmq:
        client.0:
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task rabbitmq only supports a list or dictionary for configuration"

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    log.debug('RabbitMQ config is %s', config)

    with contextutil.nested(
        lambda: install_rabbitmq(ctx=ctx, config=config),
        lambda: run_rabbitmq(ctx=ctx, config=config),
        ):
        yield
