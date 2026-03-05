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

        script_src = os.path.join(
            os.path.dirname(__file__),
            '..', '..', 'src', 'test', 'rgw', 'bucket_notification', 'kafka-security.sh',
        )
        with open(script_src, 'r') as f:
            kafka_security_script = f.read()

        script_path = '{tdir}/kafka-security.sh'.format(tdir=kafka_dir)
        remote.write_file(
            path=script_path,
            data=kafka_security_script.encode(),
        )

        # running kafka-security.sh from the kafka dir so certs are generated there
        ctx.cluster.only(client).run(
            args=[
                'cd', kafka_dir, run.Raw('&&'),
                'env',
                'KAFKA_CERT_HOSTNAME={ip}'.format(ip=remote.ip_address),
                'KAFKA_CERT_IP={ip}'.format(ip=remote.ip_address),
                'bash', script_path,
            ],
        )

        ctx.cluster.only(client).run(
            args=['sudo', 'chmod', 'o+rx', '/home/ubuntu'],
        )

        broker_conf(ctx, client, kafka_dir)

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


def broker_conf(ctx, client, kafka_dir):
    """writing a custom server.properties config file"""
    (remote,) = ctx.cluster.only(client).remotes.keys()
    ip = remote.ip_address
    conf = (
        "broker.id=0\n"
        "listeners=PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093,SASL_SSL://0.0.0.0:9094,SASL_PLAINTEXT://0.0.0.0:9095\n"
        "advertised.listeners=PLAINTEXT://{ip}:9092,SSL://{ip}:9093,SASL_SSL://{ip}:9094,SASL_PLAINTEXT://{ip}:9095\n"
        "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL,SASL_PLAINTEXT:SASL_PLAINTEXT\n"
        "log.dirs={tdir}/data/kafka-logs\n"
        "num.network.threads=3\n"
        "num.io.threads=8\n"
        "socket.send.buffer.bytes=102400\n"
        "socket.receive.buffer.bytes=102400\n"
        "socket.request.max.bytes=104857600\n"
        "num.partitions=1\n"
        "num.recovery.threads.per.data.dir=1\n"
        "offsets.topic.replication.factor=1\n"
        "transaction.state.log.replication.factor=1\n"
        "transaction.state.log.min.isr=1\n"
        "log.retention.hours=168\n"
        "log.segment.bytes=1073741824\n"
        "log.retention.check.interval.ms=300000\n"
        "zookeeper.connect=localhost:2181\n"
        "zookeeper.connection.timeout.ms=18000\n"
        "group.initial.rebalance.delay.ms=0\n"
        "ssl.keystore.location={tdir}/server.keystore.jks\n"
        "ssl.keystore.password=mypassword\n"
        "ssl.key.password=mypassword\n"
        "ssl.truststore.location={tdir}/server.truststore.jks\n"
        "ssl.truststore.password=mypassword\n"
        "sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512\n"
        "sasl.mechanism.inter.broker.protocol=PLAIN\n"
        "inter.broker.listener.name=PLAINTEXT\n"
        'listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\\n'
        '  username="admin" \\\n'
        '  password="admin-secret" \\\n'
        '  user_alice="alice-secret";\n'
        'listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n'
        '  username="admin" \\\n'
        '  password="admin-secret";\n'
        'listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n'
        '  username="admin" \\\n'
        '  password="admin-secret";\n'
        'listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\\n'
        '  username="admin" \\\n'
        '  password="admin-secret" \\\n'
        '  user_alice="alice-secret";\n'
        'listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n'
        '  username="admin" \\\n'
        '  password="admin-secret";\n'
        'listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n'
        '  username="admin" \\\n'
        '  password="admin-secret";\n'
    ).format(tdir=kafka_dir, ip=ip)
    file_name = 'server.properties'
    log.info("kafka conf file: %s", file_name)
    log.info(conf)
    ctx.cluster.only(client).run(
        args=[
            'cd', kafka_dir, run.Raw('&&'),
            'mkdir', '-p', 'config', run.Raw('&&'),
            'mkdir', '-p', 'data',
        ],
    )
    remote.write_file(
        path='{tdir}/config/{file_name}'.format(tdir=kafka_dir, file_name=file_name),
        data=conf.encode(),
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

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
             './zookeeper-server-start.sh',
             '{tir}/config/zookeeper.properties'.format(tir=get_kafka_dir(ctx, config)),
             run.Raw('&'), 'exit'
            ],
        )

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
             './kafka-server-start.sh',
             '{tir}/config/server.properties'.format(tir=get_kafka_dir(ctx, config)),
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
                 '{tir}/config/server.properties'.format(tir=get_kafka_dir(ctx, config)),
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
                '--bootstrap-server', '{ip}:9092'.format(ip=remote.ip_address)
            ],
        )

        ctx.cluster.only(client).run(
            args=[
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                'echo', "First", run.Raw('|'),
                './kafka-console-producer.sh', '--topic', 'quickstart-events',
                '--bootstrap-server', '{ip}:9092'.format(ip=remote.ip_address)
            ],
        )

        ctx.cluster.only(client).run(
            args=[
                'cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                './kafka-console-consumer.sh', '--topic', 'quickstart-events',
                '--from-beginning',
                '--bootstrap-server', '{ip}:9092'.format(ip=remote.ip_address),
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

    ctx.kafka_dir = get_kafka_dir(ctx, config)

    log.debug('Kafka config is %s', config)

    with contextutil.nested(
        lambda: install_kafka(ctx=ctx, config=config),
        lambda: run_kafka(ctx=ctx, config=config),
        lambda: run_admin_cmds(ctx=ctx, config=config),
        ):
        yield
