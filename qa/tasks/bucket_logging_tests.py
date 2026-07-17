"""
Run a set of bucket logging tests on rgw.
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
    log.info('Downloading bucket-logging-tests...')
    testdir = teuthology.get_testdir(ctx)
    branch = ctx.config.get('suite_branch')
    repo = ctx.config.get('suite_repo')
    log.info('Using branch %s from %s for bucket logging tests', branch, repo)
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

        # Journal mode requires the boto3 LoggingType extension 
        if client_config.get('logging_type') == 'Journal':
            ctx.cluster.only(client).run(
                args=['mkdir', '-p', '/home/ubuntu/.aws/models/s3/2006-03-01/']
                )
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote_file = '/home/ubuntu/.aws/models/s3/2006-03-01/service-2.sdk-extras.json'
            local_file = '{tdir}/ceph/examples/rgw/boto3/service-2.sdk-extras.json'.format(tdir=testdir)
            remote.run(args=['cp', local_file, remote_file])

    try:
        yield
    finally:
        log.info('Removing bucket-logging-tests...')
        testdir = teuthology.get_testdir(ctx)
        for client, client_config in config.items():
            if client_config.get('logging_type') == 'Journal':
                ctx.cluster.only(client).run(
                    args=['rm', '-rf', '/home/ubuntu/.aws/models/s3/2006-03-01/service-2.sdk-extras.json']
                    )
                ctx.cluster.only(client).run(
                    args=['cd', '/home/ubuntu/', run.Raw('&&'),
                          'rmdir', '-p', '.aws/models/s3/2006-03-01/']
                    )
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/ceph'.format(tdir=testdir),
                    ],
                )


def _config_user(tests_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    tests_conf[section].setdefault('user_id', user)
    tests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    tests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    tests_conf[section].setdefault('access_key',
        ''.join(random.choice(string.ascii_uppercase) for i in range(20)))
    tests_conf[section].setdefault('secret_key',
        base64.b64encode(os.urandom(40)).decode())


@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main s3 user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw user...')
    testdir = teuthology.get_testdir(ctx)

    users = {'s3 main': 'foo'}
    for client in config['clients']:
        tests_conf = config['tests_conf'][client]
        for section, user in users.items():
            _config_user(tests_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=tests_conf[section]['user_id'], host=client))
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
                    '--uid', tests_conf[section]['user_id'],
                    '--display-name', tests_conf[section]['display_name'],
                    '--access-key', tests_conf[section]['access_key'],
                    '--secret', tests_conf[section]['secret_key'],
                    '--cluster', cluster_name,
                    ],
                )

    try:
        yield
    finally:
        for client in config['clients']:
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


@contextlib.contextmanager
def configure(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring bucket-logging-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        tests_conf = config['tests_conf'][client]

        conf_fp = BytesIO()
        tests_conf.write(conf_fp)
        remote.write_file(
            path='{tdir}/ceph/src/test/rgw/bucket_logging/bucket-logging-tests.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    try:
        yield
    finally:
        log.info('Removing bucket-logging-tests.conf file...')
        testdir = teuthology.get_testdir(ctx)
        for client, properties in config['clients'].items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                 args=['rm', '-f',
                       '{tdir}/ceph/src/test/rgw/bucket_logging/bucket-logging-tests.{client}.conf'.format(tdir=testdir,client=client),
                 ],
                 )


def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path


def toxvenv_sh(ctx, remote, args, **kwargs):
    activate = get_toxvenv_dir(ctx) + '/bin/activate'
    return remote.sh(['source', activate, run.Raw('&&')] + args, **kwargs)


@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the bucket logging tests after everything is set up.
    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    log.info('Running bucket-logging-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        attr = ['basic_test']

        if 'extra_attr' in client_config:
            attr = client_config.get('extra_attr')

        args = ['cd', '{tdir}/ceph/src/test/rgw/bucket_logging/'.format(tdir=testdir), run.Raw('&&'),
            'BUCKET_LOGGING_TESTS_CONF=./bucket-logging-tests.{client}.conf'.format(client=client),
            'tox', '--', '-v', '-m', ' or '.join(attr)]

        logging_type = client_config.get('logging_type')
        if logging_type:
            args.extend(['--logging-type', logging_type])

        toxvenv_sh(ctx, remote, args, label="bucket logging tests against rgw")

    yield


@contextlib.contextmanager
def task(ctx,config):
    """

    If you want to run the tests against your changes pushed to your remote repo you can provide 'suite_branch' and 'suite_repo'
    parameters in your teuthology-suite command. Example command for this is as follows::

    teuthology-suite --ceph-repo https://github.com/ceph/ceph-ci.git -s rgw:bucket-logging --ceph your_ceph_branch_name -m smithi --suite-repo https://github.com/your_name/ceph.git --suite-branch your_branch_name

    """
    assert hasattr(ctx, 'rgw'), 'bucket-logging-tests must run after the rgw task'
    assert hasattr(ctx, 'tox'), 'bucket-logging-tests must run after the tox task'
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

    tests_conf = {}

    for client in clients:
        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'bucket-logging-tests: no rgw endpoint for {}'.format(client)

        tests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT':
                    {
                    'port':endpoint.port,
                    'host':endpoint.dns_name,
                    },
                's3 main':{}
            }
        )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                tests_conf=tests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                tests_conf=tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
