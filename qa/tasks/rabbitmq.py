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

    os_version = teuthology.get_distro_version(ctx)
    os_major_version = int(os_version.split('.')[0])

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        ctx.cluster.only(client).run(args=[
             'sudo', 'dnf', '-y', 'install', 'epel-release'
        ])

        if os_major_version >= 10:
            # packagecloud repos don't support EL10, use RabbitMQ yum
            # repos with el/9 packages which are compatible with EL10
            repo_el_version = '9'
            erlang_repo = (
                '[rabbitmq-erlang]\n'
                'name=rabbitmq-erlang\n'
                'baseurl=https://yum1.rabbitmq.com/erlang/el/{v}/$basearch\n'
                '        https://yum2.rabbitmq.com/erlang/el/{v}/$basearch\n'
                'repo_gpgcheck=0\n'
                'enabled=1\n'
                'gpgcheck=0\n'
                'sslverify=1\n'
                'sslcacert=/etc/pki/tls/certs/ca-bundle.crt\n'
            ).format(v=repo_el_version)
            ctx.cluster.only(client).run(args=[
                'sudo', 'bash', '-c',
                'echo -e \'{repo}\' > /etc/yum.repos.d/rabbitmq-erlang.repo'.format(
                    repo=erlang_repo),
            ])

            rabbitmq_repo = (
                '[rabbitmq-server]\n'
                'name=rabbitmq-server\n'
                'baseurl=https://yum2.rabbitmq.com/rabbitmq/el/{v}/noarch\n'
                '        https://yum1.rabbitmq.com/rabbitmq/el/{v}/noarch\n'
                'repo_gpgcheck=0\n'
                'enabled=1\n'
                'gpgcheck=0\n'
                'sslverify=1\n'
                'sslcacert=/etc/pki/tls/certs/ca-bundle.crt\n'
            ).format(v=repo_el_version)
            ctx.cluster.only(client).run(args=[
                'sudo', 'bash', '-c',
                'echo -e \'{repo}\' > /etc/yum.repos.d/rabbitmq-server.repo'.format(
                    repo=rabbitmq_repo),
            ])
        else:
            link1 = 'https://packagecloud.io/install/repositories/rabbitmq/erlang/script.rpm.sh'
            ctx.cluster.only(client).run(args=[
                 'curl', '-s', link1, run.Raw('|'), 'sudo', 'bash'
            ])

            link2 = 'https://packagecloud.io/install/repositories/rabbitmq/rabbitmq-server/script.rpm.sh'
            ctx.cluster.only(client).run(args=[
                 'curl', '-s', link2, run.Raw('|'), 'sudo', 'bash'
            ])

        ctx.cluster.only(client).run(args=[
             'sudo', 'dnf', '-y', 'install', 'erlang'
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
             'echo', 'loopback_users.guest = false', run.Raw('|'), 'sudo', 'tee', '-a', '/etc/rabbitmq/rabbitmq.conf'
            ],
        )

        ctx.cluster.only(client).run(args=[
             'sudo', 'systemctl', 'enable', 'rabbitmq-server'
            ],
        )

        ctx.cluster.only(client).run(args=[
             'sudo', 'systemctl', 'start', 'rabbitmq-server'
            ],
        )

        # To check whether rabbitmq-server is running or not
        ctx.cluster.only(client).run(args=[
             'sudo', 'systemctl', 'status', 'rabbitmq-server'
            ],
        )

    try:
        yield
    finally:
        log.info('Stopping RabbitMQ Service...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            ctx.cluster.only(client).run(args=[
                 'sudo', 'systemctl', 'stop', 'rabbitmq-server'
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
