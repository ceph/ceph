"""
Deploy and configure Keystone for Teuthology
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user

log = logging.getLogger(__name__)


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
    testdir = teuthology.get_testdir(ctx)

    for (client, cconf) in config.items():
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', cconf.get('force-branch', 'master'),
                'https://github.com/openstack/keystone.git',
                '{tdir}/keystone'.format(tdir=testdir),
                ],
            )

        sha1 = cconf.get('sha1')
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/keystone'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                ],
            )
    try:
        yield
    finally:
        log.info('Removing keystone...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[ 'rm', '-rf', '{tdir}/keystone'.format(tdir=testdir) ],
            )

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

@contextlib.contextmanager
def setup_venv(ctx, config):
    """
    Setup the virtualenv for Keystone using tox.
    """
    assert isinstance(config, dict)
    log.info('Setting up virtualenv for keystone...')
    for (client, _) in config.items():
        run_in_keystone_dir(ctx, client, [ 'tox', '-e', 'venv', '--notest' ])
    yield

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
        safepath.makedirs('/', keyrepo_dir)
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

        run_cmd = get_keystone_venved_cmd(ctx, 'keystone-wsgi-public',
            [ '--host', 'localhost', '--port', '5000' ])
        ctx.daemons.add_daemon(
            remote, 'keystone', client_public_with_id,
            cluster=cluster_name,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=None,
            cwd=get_keystone_dir(ctx),
            wait=False,
            check_status=False,
        )

        # start the admin endpoint
        client_admin_with_id = 'keystone.admin' + '.' + client_id

        run_cmd = get_keystone_venved_cmd(ctx, 'keystone-wsgi-admin',
            [ '--host', 'localhost', '--port', '35357' ])
        ctx.daemons.add_daemon(
            remote, 'keystone', client_admin_with_id,
            cluster=cluster_name,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=None,
            cwd=get_keystone_dir(ctx),
            wait=False,
            check_status=False,
        )

        # sleep driven synchronization
        run_in_keystone_venv(ctx, client, [ 'sleep', '3' ])
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
    auth_section = [
        ( 'os-token', 'ADMIN' ),
        ( 'os-url', 'http://127.0.0.1:35357/v2.0' ),
    ]

    for section_item in section_config_list:
        run_in_keystone_venv(ctx, cclient,
            [ 'openstack' ] + section_cmd.split() +
            dict_to_args(special, auth_section + section_item.items()))

@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)

    (cclient, cconfig) = config.items()[0]

    # configure tenants/projects
    run_section_cmds(ctx, cclient, 'project create', 'name',
                     cconfig['tenants'])
    run_section_cmds(ctx, cclient, 'user create', 'name',
                     cconfig['users'])
    run_section_cmds(ctx, cclient, 'role create', 'name',
                     cconfig['roles'])
    run_section_cmds(ctx, cclient, 'role add', 'name',
                     cconfig['role-mappings'])
    run_section_cmds(ctx, cclient, 'service create', 'type',
                     cconfig['services'])
    run_section_cmds(ctx, cclient, 'endpoint create', 'service',
                     cconfig['endpoints'])

    # sleep driven synchronization -- just in case
    run_in_keystone_venv(ctx, cclient, [ 'sleep', '3' ])
    try:
        yield
    finally:
        pass


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy and configure Keystone

    Example of configuration:

      tasks:
      - local_cluster:
          cluster_path: /home/rzarzynski/ceph-1/build
      - local_rgw:
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
              endpoints:
                - service: keystone
                  publicurl: http://127.0.0.1:$(public_port)s/v2.0
                - service: swift
                  publicurl: http://127.0.0.1:8000/v1/KEY_$(tenant_id)s
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task keystone only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    log.debug('Keystone config is %s', config)

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: configure_instance(ctx=ctx, config=config),
        lambda: run_keystone(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=config),
        ):
        yield
