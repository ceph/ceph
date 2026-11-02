"""
Deploy and configure RabbitMQ for Teuthology
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)

# where to get the RabbitMQ packages from on deb based distros
DEB_SOURCES = ('ubuntu', 'cloudsmith')
DEB_DEFAULT_SOURCE = 'ubuntu'

CLOUDSMITH_KEYS = {
    'erlang': 'https://github.com/rabbitmq/signing-keys/releases/download/3.0/'
              'cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key',
    'server': 'https://github.com/rabbitmq/signing-keys/releases/download/3.0/'
              'cloudsmith.rabbitmq-server.9F4587F226208342.key',
}

# the erlang packages that rabbitmq-server needs, as listed in the RabbitMQ
# apt installation guide (the "erlang" metapackage is not provided there)
CLOUDSMITH_ERLANG_PKGS = [
    'erlang-base', 'erlang-asn1', 'erlang-crypto', 'erlang-eldap',
    'erlang-ftp', 'erlang-inets', 'erlang-mnesia', 'erlang-os-mon',
    'erlang-parsetools', 'erlang-public-key', 'erlang-runtime-tools',
    'erlang-snmp', 'erlang-ssl', 'erlang-syntax-tools', 'erlang-tftp',
    'erlang-tools', 'erlang-xmerl',
]


def _deb_source(client_config):
    """
    Return the package source selected for a client, defaulting to the
    distro packages.
    """
    source = (client_config or {}).get('source', DEB_DEFAULT_SOURCE)
    assert source in DEB_SOURCES, \
        "rabbitmq 'source' must be one of {}, got {!r}".format(DEB_SOURCES, source)
    return source


def _setup_cloudsmith_apt(ctx, client):
    """
    Add the RabbitMQ team's Cloudsmith apt repositories (erlang and
    rabbitmq-server), following the RabbitMQ "Install on Debian/Ubuntu" guide.
    """
    cluster = ctx.cluster.only(client)
    cluster.run(args=[
        'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', 'install',
        'curl', 'gnupg', 'apt-transport-https', 'lsb-release',
    ])
    for name, url in CLOUDSMITH_KEYS.items():
        cluster.run(args=[
            'curl', '-1sLf', url, run.Raw('|'), 'sudo', 'gpg', '--dearmor',
            '--yes', '-o', '/usr/share/keyrings/rabbitmq.{}.gpg'.format(name),
        ])

    sources = (
        'deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/rabbitmq.erlang.gpg] '
        'https://deb1.rabbitmq.com/rabbitmq-erlang/ubuntu/$codename $codename main\n'
        'deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/rabbitmq.erlang.gpg] '
        'https://deb2.rabbitmq.com/rabbitmq-erlang/ubuntu/$codename $codename main\n'
        'deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/rabbitmq.server.gpg] '
        'https://deb1.rabbitmq.com/rabbitmq-server/ubuntu/$codename $codename main\n'
        'deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/rabbitmq.server.gpg] '
        'https://deb2.rabbitmq.com/rabbitmq-server/ubuntu/$codename $codename main\n'
    )
    cluster.run(args=[
        'sudo', 'bash', '-c',
        'codename=$(lsb_release -cs) && '
        'printf "{}" > /etc/apt/sources.list.d/rabbitmq.list'.format(sources),
    ])


@contextlib.contextmanager
def install_rabbitmq(ctx, config):
    """
    Downloading the RabbitMQ package.
    """
    assert isinstance(config, dict)
    log.info('Installing RabbitMQ...')

    for (client, client_config) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        if remote.os.package_type == 'deb':
            pkgs = ['rabbitmq-server']
            if _deb_source(client_config) == 'cloudsmith':
                log.info('Using the RabbitMQ Cloudsmith apt repositories')
                ctx.cluster.only(client).run(args=[
                     'sudo', 'apt-get', 'update'
                ])
                _setup_cloudsmith_apt(ctx, client)
                pkgs = CLOUDSMITH_ERLANG_PKGS + pkgs
            else:
                log.info('Using the distro RabbitMQ packages')
            ctx.cluster.only(client).run(args=[
                 'sudo', 'apt-get', 'update'
            ])
            ctx.cluster.only(client).run(args=[
                 'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y',
                 'install'] + pkgs
            )
            continue

        os_version = teuthology.get_distro_version(ctx)
        os_major_version = int(os_version.split('.')[0])

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

        for (client, client_config) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            if remote.os.package_type == 'deb':
                ctx.cluster.only(client).run(args=[
                     'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get',
                     '-y', 'purge', 'rabbitmq-server'
                ])
                if _deb_source(client_config) == 'cloudsmith':
                    ctx.cluster.only(client).run(args=[
                         'sudo', 'rm', '-f',
                         '/etc/apt/sources.list.d/rabbitmq.list',
                         '/usr/share/keyrings/rabbitmq.erlang.gpg',
                         '/usr/share/keyrings/rabbitmq.server.gpg',
                    ])
            else:
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
             'sudo', 'systemctl', 'restart', 'rabbitmq-server'
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

    On deb based distros (ubuntu) the packages come from the distro archive by
    default. To use the RabbitMQ team's Cloudsmith apt repositories instead
    (newer erlang/rabbitmq-server), set ``source`` per client::

    tasks:
    - rabbitmq:
        client.0:
          source: cloudsmith

    ``source`` is ignored on rpm based distros.
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
