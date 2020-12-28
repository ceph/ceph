"""
Deploy and configure Kafka for Teuthology
"""
from io import BytesIO
from io import StringIO
from configobj import ConfigObj
import base64
import argparse
import contextlib
import logging
import os
import random
import six
import string
import subprocess
import json
import sys
from pathlib import Path

from collections import OrderedDict
from itertools import chain

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.config import config as teuth_config
from teuthology.orchestra import run
from teuthology.packaging import install_package
from teuthology.packaging import remove_package
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)

def get_kafka_dir(ctx):
    return '{tdir}/ceph/kafka-2.6.0-src'.format(tdir=teuthology.get_testdir(ctx))

def run_in_kafka_dir(ctx, client, args, **kwargs):
    return ctx.cluster.only(client).run(
        args=[ 'cd', get_kafka_dir(ctx), run.Raw('&&'), ] + args,
        **kwargs
    )

def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

def toxvenv_sh(ctx, remote, args, **kwargs):
    activate = get_toxvenv_dir(ctx) + '/bin/activate'
    return remote.sh(['source', activate, run.Raw('&&')] + args, **kwargs)

@contextlib.contextmanager
def download(ctx, config):
    """
    Download the bucket notification tests from the git builder.
    Remove downloaded test file upon exit.
    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading bucket-notification-tests...')
    testdir = teuthology.get_testdir(ctx)
    for (client, client_config) in config.items():
        bntests_branch = client_config.get('force-branch', None)
        if not bntests_branch:
            raise ValueError(
                "Could not determine what branch to use for bn-tests. Please add 'force-branch: {bn-tests branch name}' to the .yaml config for this bucket notifications tests task.")

        log.info("Using branch '%s' for bucket notifications tests", bntests_branch)
        sha1 = client_config.get('sha1')
        git_remote = client_config.get('git_remote', teuth_config.ceph_git_base_url)
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', bntests_branch,
                git_remote + 'ceph.git',
                '{tdir}/ceph'.format(tdir=testdir),
                ],
            )
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

@contextlib.contextmanager
def install_kafka(ctx, config):
    """
    Downloading the kafka tar file.
    """
    assert isinstance(config, dict)
    log.info('Installing Kafka...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        test_dir=teuthology.get_testdir(ctx)
        toxvenv_sh(ctx, remote, ['cd', '{tdir}/ceph'.format(tdir=test_dir), run.Raw('&&'), 'wget', 'https://mirrors.estointernet.in/apache/kafka/2.6.0/kafka-2.6.0-src.tgz'])
        toxvenv_sh(ctx, remote, ['cd', '{tdir}/ceph'.format(tdir=test_dir), run.Raw('&&'), 'tar', '-xvzf', 'kafka-2.6.0-src.tgz'])

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Kafka...')
        test_dir=get_kafka_dir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=['rm', '-rf', test_dir],
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
def run_kafka(ctx,config):
    """
    This includes two parts:
    1. Starting Zookeeper service
    2. Starting Kafka service
    """
    assert isinstance(config, dict)
    log.info('Bringing up Zookeeper and Kafka services...')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        toxvenv_sh(ctx, remote,
            ['cd', '{tdir}'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'),
             './gradlew', 'jar', 
             '-PscalaVersion=2.13.2'
            ],
        )

        toxvenv_sh(ctx, remote,
            ['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'),
             './zookeeper-server-start.sh',
             '{tir}/config/zookeeper.properties'.format(tir=get_kafka_dir(ctx)),
             run.Raw('&'), 'exit'
            ],
        )

        toxvenv_sh(ctx, remote,
            ['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'),
             './kafka-server-start.sh',
             '{tir}/config/server.properties'.format(tir=get_kafka_dir(ctx)),
             run.Raw('&'), 'exit'
            ],
        )

    try:
        yield
    finally:
        log.info('Stopping Zookeeper and Kafka Services...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            toxvenv_sh(ctx, remote, 
                ['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'),
                 './kafka-server-stop.sh',  
                 '{tir}/config/kafka.properties'.format(tir=get_kafka_dir(ctx)),
                ]
            )

            toxvenv_sh(ctx, remote, 
                ['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'), 
                 './zookeeper-server-stop.sh',
                 '{tir}/config/zookeeper.properties'.format(tir=get_kafka_dir(ctx)),
                ]
            )

@contextlib.contextmanager
def run_admin_cmds(ctx,config):
    """
    Running Kafka Admin commands in order to check the working of producer anf consumer and creation of topic.
    """
    assert isinstance(config, dict)
    log.info('Checking kafka server through producer/consumer commands...')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        toxvenv_sh(ctx, remote,
            [
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'), 
                './kafka-topics.sh', '--create', '--topic', 'quickstart-events',
                '--bootstrap-server', 'localhost:9092'
            ])

        toxvenv_sh(ctx, remote,
            [
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'),
                'echo', "First", run.Raw('|'),
                './kafka-console-producer.sh', '--topic', 'quickstart-events',
                '--bootstrap-server', 'localhost:9092'
            ])

        toxvenv_sh(ctx, remote,
            [
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx)), run.Raw('&&'),
                './kafka-console-consumer.sh', '--topic', 'quickstart-events',
                '--from-beginning',
                '--bootstrap-server', 'localhost:9092',
                run.Raw('&'), 'exit'
            ])

    try:
        yield
    finally:
        pass

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

        args = [
            'BNTESTS_CONF={tdir}/ceph/src/test/rgw/bucket_notification/bn-tests.{client}.conf'.format(tdir=testdir, client=client),
            '{tdir}/ceph/src/test/rgw/bucket_notification/virtualenv/bin/python'.format(tdir=testdir),
            '-m', 'nose',
            '-s',
            '{tdir}/ceph/src/test/rgw/bucket_notification/test_bn.py'.format(tdir=testdir)
            ]

        remote.run(
            args=args,
            label="bucket notification tests against kafka server"
            )
    yield

@contextlib.contextmanager
def task(ctx,config):
    """
    To run kafka the prerequisite is to run the tox task. Following is the way how to run
    tox and then kafka::
    tasks:
    - tox: [ client.0 ]
    - kafka:
        client.0:
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task kafka only supports a list or dictionary for configuration"

    if not hasattr(ctx, 'tox'):
        raise ConfigError('kafka must run after the tox task')

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients=config.keys()

    log.debug('Kafka config is %s', config)

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
                    },
                's3 main':{}
            }
        )


    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: install_kafka(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                bntests_conf=bntests_conf,
                )),
        lambda: run_kafka(ctx=ctx, config=config),
        lambda: run_admin_cmds(ctx=ctx, config=config),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                bntests_conf=bntests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        yield
