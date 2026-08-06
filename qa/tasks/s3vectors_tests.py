"""
Run a set of s3vectors tests on rgw.
"""
from io import BytesIO
from configobj import ConfigObj
import base64
import contextlib
import logging
import os
import random
import string

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def download(ctx, config):
    assert isinstance(config, dict)
    log.info('Downloading s3vectors-tests...')
    testdir = teuthology.get_testdir(ctx)
    branch = ctx.config.get('suite_branch')
    repo = ctx.config.get('suite_repo')
    log.info('Using branch %s from %s for s3vectors tests', branch, repo)
    for (client, client_config) in config.items():

        ctx.cluster.only(client).run(
            args=['git', 'clone', '-b', branch, repo, '{tdir}/ceph'.format(tdir=testdir)],
            )

        sha1 = client_config.get('sha1')

        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/ceph'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )

    try:
        yield
    finally:
        log.info('Removing s3vectors-tests...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/ceph'.format(tdir=testdir),
                    ],
                )


def _config_user(s3vtests_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    s3vtests_conf[section].setdefault('user_id', user)
    s3vtests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    s3vtests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    s3vtests_conf[section].setdefault('access_key',
        ''.join(random.choice(string.ascii_uppercase) for i in range(20)))
    s3vtests_conf[section].setdefault('secret_key',
        base64.b64encode(os.urandom(40)).decode())


@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw user...')
    testdir = teuthology.get_testdir(ctx)

    users = {'s3 main': 'foo'}
    # in a multisite configuration, users may be created only on the master zone,
    # and are synced from there to the other zones. so, the user is created once,
    # and its credentials are shared by the clients of all zones
    master_client = config.get('master_client')
    clients = list(config['clients'])
    if master_client in clients:
        clients.remove(master_client)
        clients.insert(0, master_client)

    for client in clients:
        s3vtests_conf = config['s3vtests_conf'][client]
        for section, user in users.items():
            if master_client and client != master_client:
                master_conf = config['s3vtests_conf'][master_client]
                for key in ('user_id', 'email', 'display_name', 'access_key', 'secret_key'):
                    s3vtests_conf[section][key] = master_conf[section][key]
                log.debug('Using the user of {master} on {host}'.format(master=master_client, host=client))
                continue
            _config_user(s3vtests_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=s3vtests_conf[section]['user_id'], host=client))
            cluster_name, daemon_type, client_id = teuthology.split_role(client)
            client_with_id = daemon_type + '.' + client_id
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client_with_id,
                    'user', 'create',
                    '--uid', s3vtests_conf[section]['user_id'],
                    '--display-name', s3vtests_conf[section]['display_name'],
                    '--access-key', s3vtests_conf[section]['access_key'],
                    '--secret', s3vtests_conf[section]['secret_key'],
                    '--cluster', cluster_name,
                    ],
                )

    try:
        yield
    finally:
        for client in config['clients']:
            if master_client and client != master_client:
                # the user was created on the master zone, and is removed there
                continue
            for user in users.values():
                uid = '{user}.{client}'.format(user=user, client=client)
                cluster_name, daemon_type, client_id = teuthology.split_role(client)
                client_with_id = daemon_type + '.' + client_id
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'rm',
                        '--uid', uid,
                        '--purge-data',
                        '--cluster', cluster_name,
                        ],
                    )


def get_backends(client_config):
    """ the backends that the tests should be run against. the backend is set on
    the RGWs by the tests themselves, so a single run may cover all of them """
    return (client_config or {}).get('backends') or [None]


def conf_file_name(testdir, client, backend):
    name = 's3vectors-tests.{client}'.format(client=client)
    if backend:
        name += '.{backend}'.format(backend=backend)
    return '{tdir}/ceph/src/test/rgw/s3vectors/{name}.conf'.format(tdir=testdir, name=name)


@contextlib.contextmanager
def configure(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring s3vectors-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        s3vtests_conf = config['s3vtests_conf'][client]

        # a configuration file per backend, so that all of the tests are run
        # once for every one of them
        for backend in get_backends(properties):
            if backend:
                s3vtests_conf['DEFAULT']['s3vector_backend'] = backend
            conf_fp = BytesIO()
            s3vtests_conf.write(conf_fp)
            remote.write_file(
                path=conf_file_name(testdir, client, backend),
                data=conf_fp.getvalue(),
                )

    try:
        yield
    finally:
        log.info('Removing s3vectors-tests.conf file...')
        testdir = teuthology.get_testdir(ctx)
        for client, properties in config['clients'].items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            for backend in get_backends(properties):
                remote.run(
                     args=['rm', '-f', conf_file_name(testdir, client, backend)],
                     )


def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path


def toxvenv_sh(ctx, remote, args, **kwargs):
    activate = get_toxvenv_dir(ctx) + '/bin/activate'
    return remote.sh(['source', activate, run.Raw('&&')] + args, **kwargs)


@contextlib.contextmanager
def install_sdk_extras(ctx, config):
    """
    Copy the SDK extras file to ~/.aws/models so boto3 picks up
    non-standard API extensions (e.g. filterableMetadataKeys, postFiltering).
    """
    assert isinstance(config, dict)
    log.info('Installing s3vectors SDK extras model...')
    testdir = teuthology.get_testdir(ctx)
    model_dir = '/home/ubuntu/.aws/models/s3vectors/2025-07-15'
    for client in config:
        ctx.cluster.only(client).run(
            args=['mkdir', '-p', model_dir],
        )
        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(args=[
            'cp',
            '{tdir}/ceph/examples/rgw/boto3/s3vectors-service-2.sdk-extras.json'.format(tdir=testdir),
            '{model_dir}/service-2.sdk-extras.json'.format(model_dir=model_dir),
        ])
    try:
        yield
    finally:
        log.info('Removing s3vectors SDK extras model...')
        for client in config:
            ctx.cluster.only(client).run(
                args=['rm', '-rf', '{model_dir}/service-2.sdk-extras.json'.format(model_dir=model_dir)],
            )
            ctx.cluster.only(client).run(
                args=['cd', '/home/ubuntu/', run.Raw('&&'),
                      'rmdir', '-p', '.aws/models/s3vectors/2025-07-15'],
            )


@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the s3vectors tests after everything is set up.
    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    log.info('Running s3vectors-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        # test marks to use by default
        attr = ['vector_bucket_test', 'index_test', 'vector_test']

        if 'extra_attr' in client_config:
            attr = client_config.get('extra_attr')

        # the tests are run once per backend. changing the backend does not
        # require a restart of the RGW, so a single setup covers all of them
        for backend in get_backends(client_config):
            args = ['cd', '{tdir}/ceph/src/test/rgw/s3vectors/'.format(tdir=testdir), run.Raw('&&'),
                'S3VTESTS_CONF={conf}'.format(conf=conf_file_name(testdir, client, backend)),
                'tox', '--', '-v', '-m', ' or '.join(attr)]

            toxvenv_sh(ctx, remote, args,
                       label="s3vectors tests against rgw ({backend} backend)".format(
                           backend=backend or 'default'))

    yield


@contextlib.contextmanager
def task(ctx,config):
    """
    If you want to run the tests against your changes pushed to your remote repo you can provide 'suite_branch' and 'suite_repo'
    parameters in your teuthology-suite command. Example command for this is as follows::

    teuthology-suite --ceph-repo https://github.com/ceph/ceph-ci.git -s rgw:s3vectors --ceph your_ceph_branch_name --suite-repo https://github.com/your_name/ceph.git --suite-branch your_branch_name
    """
    assert hasattr(ctx, 'rgw'), 's3vtests must run after the rgw task'
    assert hasattr(ctx, 'tox'), 's3vtests must run after the tox task'
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task only supports a list or dictionary for configuration"

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients=config.keys()

    log.debug('config is %s', config)

    s3vtests_conf = {}

    s3vector_backend = ctx.config.get('overrides', {}).get('ceph', {}).get('conf', {}).get('client', {}).get('rgw_s3vector_backend', 'rgw')

    # some "radosgw-admin" commands are allowed only on the master zone. note that
    # the client of its RGW is assumed to be the same as the one of the tested zone
    master_cluster = None
    master_client = None
    if getattr(ctx, 'rgw_multisite', None):
        master_cluster = ctx.rgw_multisite.realm.meta_master_zone().cluster.name
        for client in clients:
            if teuthology.split_role(client)[0] == master_cluster:
                master_client = client
                break
        log.info('s3vectors: the master zone is in cluster %s (client %s)',
                 master_cluster, master_client)

    for client in clients:
        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 's3vtests: no rgw endpoint for {}'.format(client)

        # the cluster and the client are needed by the tests when they run
        # "ceph" or "radosgw-admin" commands
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        infile = {
            'DEFAULT':
                {
                'port':endpoint.port,
                'host':endpoint.dns_name,
                'zonegroup':ctx.rgw.zonegroup,
                's3vector_backend':s3vector_backend,
                'cluster':cluster_name,
                'rgw_client':daemon_type + '.' + client_id,
                },
            's3 main':{}
        }

        if master_cluster:
            infile['DEFAULT']['master_cluster'] = master_cluster

        # in a multisite configuration, the RGW of the other zone is used by the
        # tests that verify that nothing is synced between the zones
        secondary = (config.get(client) or {}).get('secondary')
        if secondary:
            secondary_endpoint = ctx.rgw.role_endpoints.get(secondary)
            assert secondary_endpoint, 's3vtests: no rgw endpoint for {}'.format(secondary)
            secondary_cluster, _, _ = teuthology.split_role(secondary)
            infile['secondary'] = {
                'port':secondary_endpoint.port,
                'host':secondary_endpoint.dns_name,
                'cluster':secondary_cluster,
            }

        s3vtests_conf[client] = ConfigObj(
            indent_type='',
            infile=infile
        )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                s3vtests_conf=s3vtests_conf,
                master_client=master_client,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                s3vtests_conf=s3vtests_conf,
                )),
        lambda: install_sdk_extras(ctx=ctx, config=config),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield

