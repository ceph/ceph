"""
Deploy and configure Keystone for Teuthology
"""
import argparse
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user
from teuthology.packaging import install_package
from teuthology.packaging import remove_package

log = logging.getLogger(__name__)


@contextlib.contextmanager
def install_packages(ctx, config):
    """
    Download the packaged dependencies of Keystone.
    Remove install packages upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Installing packages for Keystone...')

    deps = {
	'deb': [ 'libffi-dev', 'libssl-dev', 'libldap2-dev', 'libsasl2-dev' ],
	'rpm': [ 'libffi-devel', 'openssl-devel' ],
    }
    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()
        for dep in deps[remote.os.package_type]:
            install_package(dep, remote)
    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Keystone...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.iterkeys()
            for dep in deps[remote.os.package_type]:
                remove_package(dep, remote)

def get_keystone_dir(ctx):
    return '{tdir}/keystone'.format(tdir=teuthology.get_testdir(ctx))

def run_in_keystone_dir(ctx, client, args):
    ctx.cluster.only(client).run(
        args=[ 'cd', get_keystone_dir(ctx), run.Raw('&&'), ] + args,
    )

def run_in_keystone_venv(ctx, client, args):
    run_in_keystone_dir(ctx, client,
                        [   'source',
                            '.tox/venv/bin/activate',
                            run.Raw('&&')
                        ] + args)

def get_keystone_venved_cmd(ctx, cmd, args):
    kbindir = get_keystone_dir(ctx) + '/.tox/venv/bin/'
    return [ kbindir + 'python', kbindir + cmd ] + args

def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

@contextlib.contextmanager
def download(ctx, config):
    """
    Download the Keystone from github.
    Remove downloaded file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading keystone...')
    keystonedir = get_keystone_dir(ctx)

    for (client, cconf) in config.items():
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', cconf.get('force-branch', 'master'),
                'https://github.com/openstack/keystone.git',
                keystonedir,
                ],
            )

        sha1 = cconf.get('sha1')
        if sha1 is not None:
            run_in_keystone_dir(ctx, client, [
                    'git', 'reset', '--hard', sha1,
                ],
            )

        # hax for http://tracker.ceph.com/issues/23659
        run_in_keystone_dir(ctx, client, [
                'sed', '-i',
                's/pysaml2<4.0.3,>=2.4.0/pysaml2>=4.5.0/',
                'requirements.txt'
            ],
        )
    try:
        yield
    finally:
        log.info('Removing keystone...')
        for client in config:
            ctx.cluster.only(client).run(
                args=[ 'rm', '-rf', keystonedir ],
            )

@contextlib.contextmanager
def setup_venv(ctx, config):
    """
    Setup the virtualenv for Keystone using tox.
    """
    assert isinstance(config, dict)
    log.info('Setting up virtualenv for keystone...')
    for (client, _) in config.items():
        run_in_keystone_dir(ctx, client,
            [   'source',
		'{tvdir}/bin/activate'.format(tvdir=get_toxvenv_dir(ctx)),
                run.Raw('&&'),
                'tox', '-e', 'venv', '--notest'
            ])

        run_in_keystone_venv(ctx, client,
            [   'pip', 'install', 'python-openstackclient' ])
    try:
        yield
    finally:
	pass

@contextlib.contextmanager
def configure_instance(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring keystone...')

    keyrepo_dir = '{kdir}/etc/fernet-keys'.format(kdir=get_keystone_dir(ctx))
    for (client, _) in config.items():
        # prepare the config file
        run_in_keystone_dir(ctx, client,
            [
                'cp', '-f',
                'etc/keystone.conf.sample',
                'etc/keystone.conf'
            ])
        run_in_keystone_dir(ctx, client,
            [
                'sed',
                '-e', 's/#admin_token =.*/admin_token = ADMIN/',
                '-i', 'etc/keystone.conf'
            ])
        run_in_keystone_dir(ctx, client,
            [
                'sed',
                '-e', 's^#key_repository =.*^key_repository = {kr}^'.format(kr = keyrepo_dir),
                '-i', 'etc/keystone.conf'
            ])

        # prepare key repository for Fetnet token authenticator
        run_in_keystone_dir(ctx, client, [ 'mkdir', '-p', keyrepo_dir ])
        run_in_keystone_venv(ctx, client, [ 'keystone-manage', 'fernet_setup' ])

        # sync database
        run_in_keystone_venv(ctx, client, [ 'keystone-manage', 'db_sync' ])
    yield

@contextlib.contextmanager
def run_keystone(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring keystone...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()
        cluster_name, _, client_id = teuthology.split_role(client)

        # start the public endpoint
        client_public_with_id = 'keystone.public' + '.' + client_id
        client_public_with_cluster = cluster_name + '.' + client_public_with_id

        public_host, public_port = ctx.keystone.public_endpoints[client]
        run_cmd = get_keystone_venved_cmd(ctx, 'keystone-wsgi-public',
            [   '--host', public_host, '--port', str(public_port),
                # Let's put the Keystone in background, wait for EOF
                # and after receiving it, send SIGTERM to the daemon.
                # This crazy hack is because Keystone, in contrast to
                # our other daemons, doesn't quit on stdin.close().
                # Teuthology relies on this behaviour.
		run.Raw('& { read; kill %1; }')
            ]
        )
        ctx.daemons.add_daemon(
            remote, 'keystone', client_public_with_id,
            cluster=cluster_name,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            cwd=get_keystone_dir(ctx),
            wait=False,
            check_status=False,
        )

        # start the admin endpoint
        client_admin_with_id = 'keystone.admin' + '.' + client_id

        admin_host, admin_port = ctx.keystone.admin_endpoints[client]
        run_cmd = get_keystone_venved_cmd(ctx, 'keystone-wsgi-admin',
            [   '--host', admin_host, '--port', str(admin_port),
                run.Raw('& { read; kill %1; }')
            ]
        )
        ctx.daemons.add_daemon(
            remote, 'keystone', client_admin_with_id,
            cluster=cluster_name,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            cwd=get_keystone_dir(ctx),
            wait=False,
            check_status=False,
        )

        # sleep driven synchronization
        run_in_keystone_venv(ctx, client, [ 'sleep', '15' ])
    try:
        yield
    finally:
        log.info('Stopping Keystone admin instance')
        ctx.daemons.get_daemon('keystone', client_admin_with_id,
                               cluster_name).stop()

        log.info('Stopping Keystone public instance')
        ctx.daemons.get_daemon('keystone', client_public_with_id,
                               cluster_name).stop()


def dict_to_args(special, items):
    """
    Transform
        [(key1, val1), (special, val_special), (key3, val3) ]
    into:
        [ '--key1', 'val1', '--key3', 'val3', 'val_special' ]
    """
    args=[]
    for (k, v) in items:
        if k == special:
            special_val = v
        else:
            args.append('--{k}'.format(k=k))
            args.append(v)
    if special_val:
        args.append(special_val)
    return args

def run_section_cmds(ctx, cclient, section_cmd, special,
                     section_config_list):
    admin_host, admin_port = ctx.keystone.admin_endpoints[cclient]

    auth_section = [
        ( 'os-token', 'ADMIN' ),
        ( 'os-url', 'http://{host}:{port}/v2.0'.format(host=admin_host,
                                                       port=admin_port) ),
    ]

    for section_item in section_config_list:
        run_in_keystone_venv(ctx, cclient,
            [ 'openstack' ] + section_cmd.split() +
            dict_to_args(special, auth_section + section_item.items()))

def create_endpoint(ctx, cclient, service, url):
    endpoint_section = {
        'service': service,
        'publicurl': url,
    }
    return run_section_cmds(ctx, cclient, 'endpoint create', 'service',
                            [ endpoint_section ])

@contextlib.contextmanager
def fill_keystone(ctx, config):
    assert isinstance(config, dict)

    for (cclient, cconfig) in config.items():
        # configure tenants/projects
        run_section_cmds(ctx, cclient, 'project create', 'name',
                         cconfig['tenants'])
        run_section_cmds(ctx, cclient, 'user create', 'name',
                         cconfig['users'])
        run_section_cmds(ctx, cclient, 'role create', 'name',
                         cconfig['roles'])
        run_section_cmds(ctx, cclient, 'role add', 'name',
                         cconfig['role-mappings'])
        run_section_cmds(ctx, cclient, 'service create', 'name',
                         cconfig['services'])

        public_host, public_port = ctx.keystone.public_endpoints[cclient]
        url = 'http://{host}:{port}/v2.0'.format(host=public_host,
                                                 port=public_port)
        create_endpoint(ctx, cclient, 'keystone', url)
        # for the deferred endpoint creation; currently it's used in rgw.py
        ctx.keystone.create_endpoint = create_endpoint

        # sleep driven synchronization -- just in case
        run_in_keystone_venv(ctx, cclient, [ 'sleep', '3' ])
    try:
        yield
    finally:
        pass

def assign_ports(ctx, config, initial_port):
    """
    Assign port numbers starting from @initial_port
    """
    port = initial_port
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for role in roles_for_host:
            if role in config:
                role_endpoints[role] = (remote.name.split('@')[1], port)
                port += 1

    return role_endpoints

@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy and configure Keystone

    Example of configuration:

      - install:
      - ceph:
      - tox: [ client.0 ]
      - keystone:
          client.0:
            force-branch: master
            tenants:
              - name: admin
                description:  Admin Tenant
            users:
              - name: admin
                password: ADMIN
                project: admin
            roles: [ name: admin, name: Member ]
            role-mappings:
              - name: admin
                user: admin
                project: admin
            services:
              - name: keystone
                type: identity
                description: Keystone Identity Service
              - name: swift
                type: object-store
                description: Swift Service
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task keystone only supports a list or dictionary for configuration"

    if not ctx.tox:
        raise ConfigError('keystone must run after the tox task')

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    log.debug('Keystone config is %s', config)

    ctx.keystone = argparse.Namespace()
    ctx.keystone.public_endpoints = assign_ports(ctx, config, 5000)
    ctx.keystone.admin_endpoints = assign_ports(ctx, config, 35357)

    with contextutil.nested(
        lambda: install_packages(ctx=ctx, config=config),
        lambda: download(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: configure_instance(ctx=ctx, config=config),
        lambda: run_keystone(ctx=ctx, config=config),
        lambda: fill_keystone(ctx=ctx, config=config),
        ):
        yield
