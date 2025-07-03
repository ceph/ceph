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

def zookeeper_conf(ctx, client, _id, kafka_dir):
    conf = """
    # zookeeper{_id}.properties
    dataDir={tdir}/data/zookeeper{_id}
    clientPort=218{_id}
    maxClientCnxns=0
    admin.enableServer=false
    tickTime=2000
    initLimit=10
    syncLimit=5
    server.1=localhost:2888:3888
    server.2=localhost:2889:3889
    """.format(tdir=kafka_dir, _id=_id)
    file_name = 'zookeeper{_id}.properties'.format(_id=_id)
    log.info("zookeeper conf file: %s", file_name)
    log.info(conf)
    return ctx.cluster.only(client).run(
            args=[
                'cd', kafka_dir, run.Raw('&&'),
                'mkdir', '-p', 'config', run.Raw('&&'),
                'mkdir', '-p', 'data/zookeeper{_id}'.format(_id=_id), run.Raw('&&'),
                'echo', conf, run.Raw('>'), 'config/{file_name}'.format(file_name=file_name), run.Raw('&&'),
                'echo', str(_id), run.Raw('>'), 'data/zookeeper{_id}/myid'.format(_id=_id)
                ],
            )


def broker_conf(ctx, client, _id, kafka_dir):
    (remote,) = ctx.cluster.only(client).remotes.keys()
    conf = """
    # kafka{_id}.properties
	broker.id={_id}
    listeners=PLAINTEXT://0.0.0.0:909{_id}
    advertised.listeners=PLAINTEXT://{ip}:909{_id}
    log.dirs={tdir}/data/kafka-logs-{_id}
    num.network.threads=3
    num.io.threads=8
    socket.send.buffer.bytes=102400
    socket.receive.buffer.bytes=102400
    socket.request.max.bytes=369295617
    num.partitions=1
    num.recovery.threads.per.data.dir=1
    offsets.topic.replication.factor=2
    transaction.state.log.replication.factor=2
    transaction.state.log.min.isr=2
    log.retention.hours=168
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000
    zookeeper.connect=localhost:2181,localhost:2182
    zookeeper.connection.timeout.ms=18000
    group.initial.rebalance.delay.ms=0
    metadata.max.age.ms=3000
    """.format(tdir=kafka_dir, _id=_id, ip=remote.ip_address)
    file_name = 'kafka{_id}.properties'.format(_id=_id)
    log.info("kafka conf file: %s", file_name)
    log.info(conf)
    return ctx.cluster.only(client).run(
            args=[
                'cd', kafka_dir, run.Raw('&&'),
                'mkdir', '-p', 'config', run.Raw('&&'),
                'mkdir', '-p', 'data', run.Raw('&&'),
                'echo', conf, run.Raw('>'), 'config/{file_name}'.format(file_name=file_name)
                ],
            )


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
        # create config for 2 zookeepers
        zookeeper_conf(ctx, client, 1, kafka_dir)
        zookeeper_conf(ctx, client, 2, kafka_dir)
        # create config for 2 brokers
        broker_conf(ctx, client, 1, kafka_dir)
        broker_conf(ctx, client, 2, kafka_dir)

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Kafka...')
        kafka_dir=get_kafka_dir(ctx, config)
        for (client,_) in config.items():
            ctx.cluster.only(client).run(
                args=['rm', '-rf', '{tdir}'.format(tdir=kafka_dir)],
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

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
             './zookeeper-server-start.sh', '-daemon',
             '{tdir}/config/zookeeper1.properties'.format(tdir=kafka_dir)
            ],
        )
        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
             './zookeeper-server-start.sh', '-daemon',
             '{tdir}/config/zookeeper2.properties'.format(tdir=kafka_dir)
            ],
        )
        # wait for zookeepers to start
        time.sleep(5)
        for zk_id in [1, 2]:
            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
                 './zookeeper-shell.sh', 'localhost:218{_id}'.format(_id=zk_id), 'ls', '/'],
            )
            zk_started = False
            while not zk_started:
                result = ctx.cluster.only(client).run(
                        args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
                        './zookeeper-shell.sh', 'localhost:218{_id}'.format(_id=zk_id), 'ls', '/'],
                        )
                log.info("Checking if Zookeeper %d is started. Result: %s", zk_id, str(result))
                zk_started = True

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
             './kafka-server-start.sh', '-daemon',
             '{tdir}/config/kafka1.properties'.format(tdir=get_kafka_dir(ctx, config))
            ],
        )
        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
             './kafka-server-start.sh', '-daemon',
             '{tdir}/config/kafka2.properties'.format(tdir=get_kafka_dir(ctx, config))
            ],
        )
        # wait for kafka to start
        time.sleep(5)

    try:
        yield
    finally:
        log.info('Stopping Zookeeper and Kafka Services...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                 './kafka-server-stop.sh',
                 '{tdir}/config/kafka1.properties'.format(tdir=get_kafka_dir(ctx, config)),
                ],
            )

            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                 './kafka-server-stop.sh',
                 '{tdir}/config/kafka2.properties'.format(tdir=get_kafka_dir(ctx, config)),
                ],
            )

            # wait for kafka to stop
            time.sleep(5)

            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                 './zookeeper-server-stop.sh',
                 '{tir}/config/zookeeper1.properties'.format(tir=get_kafka_dir(ctx, config)),
                ],
            )
            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                 './zookeeper-server-stop.sh',
                 '{tir}/config/zookeeper2.properties'.format(tir=get_kafka_dir(ctx, config)),
                ],
            )

            # wait for zookeeper to stop
            time.sleep(5)
            ctx.cluster.only(client).run(args=['killall', '-9', 'java'])


@contextlib.contextmanager
def run_admin_cmds(ctx, config):
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
                '--bootstrap-server', 'localhost:9091,localhost:9092',
            ],
        )

        ctx.cluster.only(client).run(
            args=[
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                'echo', "First", run.Raw('|'),
                './kafka-console-producer.sh', '--topic', 'quickstart-events',
                '--bootstrap-server', 'localhost:9091,localhost:9092',
            ],
        )

        ctx.cluster.only(client).run(
            args=[
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                './kafka-console-consumer.sh', '--topic', 'quickstart-events',
                '--from-beginning',
                '--bootstrap-server', 'localhost:9091,localhost:9092', '--max-messages', '1',
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

