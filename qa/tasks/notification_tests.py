"""
Run a set of bucket notification tests on rgw.
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
    log.info('Downloading bucket-notifications-tests...')
    testdir = teuthology.get_testdir(ctx)
    branch = ctx.config.get('suite_branch')
    repo = ctx.config.get('suite_repo')
    log.info('Using branch %s from %s for bucket notifications tests', branch, repo)
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
        log.info('Removing bucket-notifications-tests...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/ceph'.format(tdir=testdir),
                    ],
                )

def _config_user(bntests_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    bntests_conf[section].setdefault('user_id', user)
    bntests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    bntests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    bntests_conf[section].setdefault('access_key',
        ''.join(random.choice(string.ascii_uppercase) for i in range(20)))
    bntests_conf[section].setdefault('secret_key',
        base64.b64encode(os.urandom(40)).decode())


@contextlib.contextmanager
def pre_process(ctx, config):
    """
    This function creates a directory which is required to run some AMQP tests.
    """
    assert isinstance(config, dict)
    log.info('Pre-processing...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        test_dir=teuthology.get_testdir(ctx)

        ctx.cluster.only(client).run(
            args=[
                'mkdir', '-p', '/home/ubuntu/.aws/models/s3/2006-03-01/',
                ],
            )

        ctx.cluster.only(client).run(
            args=[
                'cd', '/home/ubuntu/.aws/models/s3/2006-03-01/', run.Raw('&&'), 'cp', '{tdir}/ceph/examples/rgw/boto3/service-2.sdk-extras.json'.format(tdir=test_dir), 'service-2.sdk-extras.json'
                ],
            )

    try:
        yield
    finally:
        log.info('Pre-processing completed...')
        test_dir = teuthology.get_testdir(ctx)
        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            ctx.cluster.only(client).run(
                args=[
                    'rm', '-rf', '/home/ubuntu/.aws/models/s3/2006-03-01/service-2.sdk-extras.json',
                    ],
                )

            ctx.cluster.only(client).run(
                args=[
                    'cd', '/home/ubuntu/', run.Raw('&&'), 'rmdir', '-p', '.aws/models/s3/2006-03-01/',
                    ],
                )


@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw user...')
    testdir = teuthology.get_testdir(ctx)

    users = {'s3 main': 'foo'}
    for client in config['clients']:
        bntests_conf = config['bntests_conf'][client]
        for section, user in users.items():
            _config_user(bntests_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=bntests_conf[section]['user_id'], host=client))
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
                    '--uid', bntests_conf[section]['user_id'],
                    '--display-name', bntests_conf[section]['display_name'],
                    '--access-key', bntests_conf[section]['access_key'],
                    '--secret', bntests_conf[section]['secret_key'],
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
    log.info('Configuring bucket-notifications-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        bntests_conf = config['bntests_conf'][client]

        conf_fp = BytesIO()
        bntests_conf.write(conf_fp)
        remote.write_file(
            path='{tdir}/ceph/src/test/rgw/bucket_notification/bn-tests.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

        remote.run(
            args=[
                'cd',
                '{tdir}/ceph/src/test/rgw/bucket_notification'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )

    try:
        yield
    finally:
        log.info('Removing bn-tests.conf file...')
        testdir = teuthology.get_testdir(ctx)
        for client, properties in config['clients'].items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                 args=['rm', '-f',
                       '{tdir}/ceph/src/test/rgw/bucket_notification/bn-tests.{client}.conf'.format(tdir=testdir,client=client),
                 ],
                 )

@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the bucket notifications tests after everything is set up.
    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    log.info('Running bucket-notifications-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        attr = ["!kafka_test", "!amqp_test", "!amqp_ssl_test", "!kafka_security_test", "!modification_required", "!manual_test"]

        if 'extra_attr' in client_config:
            attr = client_config.get('extra_attr')

        args = [
            'BNTESTS_CONF={tdir}/ceph/src/test/rgw/bucket_notification/bn-tests.{client}.conf'.format(tdir=testdir, client=client),
            '{tdir}/ceph/src/test/rgw/bucket_notification/virtualenv/bin/python'.format(tdir=testdir),
            '-m', 'nose',
            '-s',
            '{tdir}/ceph/src/test/rgw/bucket_notification/test_bn.py'.format(tdir=testdir),
            '-v',
            '-a', ','.join(attr),
            ]

        remote.run(
            args=args,
            label="bucket notification tests against different endpoints"
            )
    yield

@contextlib.contextmanager
def task(ctx,config):
    """
    To run bucket notification tests under Kafka endpoint the prerequisite is to run the kafka server. Also you need to pass the
    'extra_attr' to the notification tests. Following is the way how to run kafka and finally bucket notification tests::

    tasks:
    - kafka:
        client.0:
          kafka_version: 2.6.0
    - notification_tests:
        client.0:
          extra_attr: ["kafka_test"]

    To run bucket notification tests under AMQP endpoint the prerequisite is to run the rabbitmq server. Also you need to pass the
    'extra_attr' to the notification tests. Following is the way how to run rabbitmq and finally bucket notification tests::

    tasks:
    - rabbitmq:
        client.0:
    - notification_tests:
        client.0:
          extra_attr: ["amqp_test"]

    If you want to run the tests against your changes pushed to your remote repo you can provide 'suite_branch' and 'suite_repo'
    parameters in your teuthology-suite command. Example command for this is as follows::

    teuthology-suite --ceph-repo https://github.com/ceph/ceph-ci.git -s rgw:notifications --ceph your_ceph_branch_name -m smithi --suite-repo https://github.com/your_name/ceph.git --suite-branch your_branch_name
    
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task kafka only supports a list or dictionary for configuration"

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients=config.keys()

    log.debug('Notifications config is %s', config)

    bntests_conf = {}

    for client in clients:
        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'bntests: no rgw endpoint for {}'.format(client)

        bntests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT':
                    {
                    'port':endpoint.port,
                    'host':endpoint.dns_name,
                    'zonegroup':'default',
                    'cluster':'noname',
                    'version':'v2'
                    },
                's3 main':{}
            }
        )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: pre_process(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                bntests_conf=bntests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                bntests_conf=bntests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
