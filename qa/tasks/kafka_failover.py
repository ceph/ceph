"""
Deploy and configure Kafka for Teuthology
"""
import contextlib
import logging
import time
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)

def get_kafka_version(config):
    for client, client_config in config.items():
        if 'kafka_version' in client_config:
            kafka_version = client_config.get('kafka_version')
    return kafka_version

kafka_prefix = 'kafka_2.13-'

def get_kafka_dir(ctx, config):
    kafka_version = get_kafka_version(config)
    current_version = kafka_prefix + kafka_version
    return '{tdir}/{ver}'.format(tdir=teuthology.get_testdir(ctx),ver=current_version)


@contextlib.contextmanager
def install_kafka(ctx, config):
    """
    Downloading the kafka tar file.
    """
    assert isinstance(config, dict)
    log.info('Installing Kafka...')

    # programmatically find a nearby mirror so as not to hammer archive.apache.org
    apache_mirror_cmd="curl 'https://www.apache.org/dyn/closer.cgi' 2>/dev/null | " \
        "grep -o '<strong>[^<]*</strong>' | sed 's/<[^>]*>//g' | head -n 1"
    log.info("determining apache mirror by running: " + apache_mirror_cmd)
    apache_mirror_url_front = os.popen(apache_mirror_cmd).read().rstrip() # note: includes trailing slash (/)
    log.info("chosen apache mirror is " + apache_mirror_url_front)

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        test_dir=teuthology.get_testdir(ctx)
        current_version = get_kafka_version(config)

        kafka_file =  kafka_prefix + current_version + '.tgz'

        link1 = '{apache_mirror_url_front}/kafka/'.format(apache_mirror_url_front=apache_mirror_url_front) + \
            current_version + '/' + kafka_file
        ctx.cluster.only(client).run(
            args=['cd', '{tdir}'.format(tdir=test_dir), run.Raw('&&'), 'wget', link1],
        )

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}'.format(tdir=test_dir), run.Raw('&&'), 'tar', '-xvzf', kafka_file],
        )

        kafka_dir = get_kafka_dir(ctx, config)
        # create config for second broker
        second_broker_config_name = "server2.properties"
        second_broker_data = "{tdir}/data/broker02".format(tdir=kafka_dir)
        second_broker_data_logs_escaped = "{}/logs".format(second_broker_data).replace("/", "\/")

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}'.format(tdir=kafka_dir), run.Raw('&&'), 
             'cp', '{tdir}/config/server.properties'.format(tdir=kafka_dir), '{tdir}/config/{second_broker_config_name}'.format(tdir=kafka_dir, second_broker_config_name=second_broker_config_name), run.Raw('&&'), 
             'mkdir', '-p', '{tdir}/data'.format(tdir=kafka_dir)
            ],
        )

        # edit config
        ctx.cluster.only(client).run(
            args=['sed', '-i', 's/broker.id=0/broker.id=1/g', '{tdir}/config/{second_broker_config_name}'.format(tdir=kafka_dir, second_broker_config_name=second_broker_config_name), run.Raw('&&'),
                  'sed', '-i', 's/#listeners=PLAINTEXT:\/\/:9092/listeners=PLAINTEXT:\/\/localhost:19092/g', '{tdir}/config/{second_broker_config_name}'.format(tdir=kafka_dir, second_broker_config_name=second_broker_config_name), run.Raw('&&'),
                  'sed', '-i', 's/#advertised.listeners=PLAINTEXT:\/\/your.host.name:9092/advertised.listeners=PLAINTEXT:\/\/localhost:19092/g', '{tdir}/config/{second_broker_config_name}'.format(tdir=kafka_dir, second_broker_config_name=second_broker_config_name), run.Raw('&&'),
                  'sed', '-i', 's/log.dirs=\/tmp\/kafka-logs/log.dirs={}/g'.format(second_broker_data_logs_escaped), '{tdir}/config/{second_broker_config_name}'.format(tdir=kafka_dir, second_broker_config_name=second_broker_config_name), run.Raw('&&'),
                  'cat', '{tdir}/config/{second_broker_config_name}'.format(tdir=kafka_dir, second_broker_config_name=second_broker_config_name)
            ]
        )

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Kafka...')
        test_dir=get_kafka_dir(ctx, config)
        current_version = get_kafka_version(config)
        for (client,_) in config.items():
            ctx.cluster.only(client).run(
                args=['rm', '-rf', '{tdir}/logs'.format(tdir=test_dir)],
            )

            ctx.cluster.only(client).run(
                args=['rm', '-rf', test_dir],
            )

            ctx.cluster.only(client).run(
                args=['rm', '-rf', '{tdir}/{doc}'.format(tdir=teuthology.get_testdir(ctx),doc=kafka_file)],
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
        kafka_dir = get_kafka_dir(ctx, config)

        second_broker_data = "{tdir}/data/broker02".format(tdir=kafka_dir)
        second_broker_java_log_dir = "{}/java_logs".format(second_broker_data)

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
             './zookeeper-server-start.sh',
             '{tir}/config/zookeeper.properties'.format(tir=kafka_dir),
             run.Raw('&'), 'exit'
            ],
        )

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
             './kafka-server-start.sh',
             '{tir}/config/server.properties'.format(tir=get_kafka_dir(ctx, config)),
             run.Raw('&'), 'exit'
            ],
        )
        
        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
             run.Raw('LOG_DIR={second_broker_java_log_dir}'.format(second_broker_java_log_dir=second_broker_java_log_dir)), 
             './kafka-server-start.sh', '{tdir}/config/server2.properties'.format(tdir=kafka_dir),
             run.Raw('&'), 'exit'
            ],
        )

    try:
        yield
    finally:
        log.info('Stopping Zookeeper and Kafka Services...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                 './kafka-server-stop.sh',  
                 '{tir}/config/kafka.properties'.format(tir=get_kafka_dir(ctx, config)),
                ],
            )

            time.sleep(5)

            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'), 
                 './zookeeper-server-stop.sh',
                 '{tir}/config/zookeeper.properties'.format(tir=get_kafka_dir(ctx, config)),
                ],
            )

            time.sleep(5)

            ctx.cluster.only(client).run(args=['killall', '-9', 'java'])


@contextlib.contextmanager
def run_admin_cmds(ctx,config):
    """
    Running Kafka Admin commands in order to check the working of producer anf consumer and creation of topic.
    """
    assert isinstance(config, dict)
    log.info('Checking kafka server through producer/consumer commands...')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        ctx.cluster.only(client).run(
            args=[
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'), 
                './kafka-topics.sh', '--create', '--topic', 'quickstart-events',
                '--bootstrap-server', 'localhost:9092'
            ],
        )

        ctx.cluster.only(client).run(
            args=[
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                'echo', "First", run.Raw('|'),
                './kafka-console-producer.sh', '--topic', 'quickstart-events',
                '--bootstrap-server', 'localhost:9092'
            ],
        )

        ctx.cluster.only(client).run(
            args=[
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                './kafka-console-consumer.sh', '--topic', 'quickstart-events',
                '--from-beginning',
                '--bootstrap-server', 'localhost:9092',
                run.Raw('&'), 'exit'
            ],
        )

    try:
        yield
    finally:
        pass


@contextlib.contextmanager
def task(ctx,config):
    """
    Following is the way how to run kafka::
    tasks:
    - kafka:
        client.0:
          kafka_version: 2.6.0
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

    log.debug('Kafka config is %s', config)

    with contextutil.nested(
        lambda: install_kafka(ctx=ctx, config=config),
        lambda: run_kafka(ctx=ctx, config=config),
        lambda: run_admin_cmds(ctx=ctx, config=config),
        ):
        yield

