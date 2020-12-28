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
    return '{tdir}/kafka-2.6.0-src'.format(tdir=teuthology.get_testdir(ctx))

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
def install_kafka(ctx, config):
    """
    Downloading the kafka tar file.
    """
    assert isinstance(config, dict)
    log.info('Installing Kafka...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        test_dir=teuthology.get_testdir(ctx)
        toxvenv_sh(ctx, remote, ['cd', '{tdir}'.format(tdir=test_dir), run.Raw('&&'), 'wget', 'https://archive.apache.org/dist/kafka/2.6.0/kafka-2.6.0-src.tgz'])
        toxvenv_sh(ctx, remote, ['cd', '{tdir}'.format(tdir=test_dir), run.Raw('&&'), 'tar', '-xvzf', 'kafka-2.6.0-src.tgz'])

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Kafka...')
        test_dir=get_kafka_dir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=['rm', '-rf', test_dir],
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

    with contextutil.nested(
        lambda: install_kafka(ctx=ctx, config=config),
        lambda: run_kafka(ctx=ctx, config=config),
        lambda: run_admin_cmds(ctx=ctx, config=config),
        ):
        yield
