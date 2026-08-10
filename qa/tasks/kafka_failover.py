"""
Deploy and configure Kafka for Teuthology
"""
import contextlib
import logging
import re
import time
import os
import urllib.request

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)

def get_kafka_version(config):
    for client, client_config in config.items():
        if 'kafka_version' in client_config:
            kafka_version = client_config.get('kafka_version')
    return kafka_version

def is_kraft_mode(version_str):
    """Kafka 4.0+ removed Zookeeper and uses KRaft consensus instead."""
    return int(version_str.split('.')[0]) >= 4

def resolve_kafka_version(version):
    """
    Resolve a minor version like "4.3" to the current patch release. Fully
    specified versions like "3.9.2" are used as-is.
    """
    if version.count('.') >= 2:
        return version
    with urllib.request.urlopen(
            'https://dlcdn.apache.org/kafka/', timeout=30) as resp:
        html = resp.read().decode()
    pattern = re.compile(r'href="(' + re.escape(version) + r'\.\d+)/"')
    matches = pattern.findall(html)
    if not matches:
        raise RuntimeError(
            "Kafka {v}.x not found on dlcdn.apache.org "
            "the minor line may have been dropped from Apache's "
            "supported set.".format(v=version)
        )
    return max(matches, key=lambda v: tuple(int(p) for p in v.split('.')))

kafka_prefix = 'kafka_2.13-'

KRAFT_CONTROLLER_PORTS = {
    1: 9097,
    2: 9098,
}

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


def broker_conf(ctx, client, _id, kafka_dir, kraft):
    (remote,) = ctx.cluster.only(client).remotes.keys()
    if kraft:
        bootstrap = ','.join(
            'localhost:{}'.format(port)
            for port in KRAFT_CONTROLLER_PORTS.values()
        )
        consensus_config = """
    node.id={_id}
    process.roles=broker,controller
    listeners=PLAINTEXT://0.0.0.0:909{_id},CONTROLLER://0.0.0.0:{controller_port}
    advertised.listeners=PLAINTEXT://{ip}:909{_id}
    listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
    controller.listener.names=CONTROLLER
    controller.quorum.bootstrap.servers={bootstrap}
    inter.broker.listener.name=PLAINTEXT
    """.format(
            _id=_id,
            ip=remote.ip_address,
            controller_port=KRAFT_CONTROLLER_PORTS[_id],
            bootstrap=bootstrap)
    else:
        consensus_config = """
	broker.id={_id}
    listeners=PLAINTEXT://0.0.0.0:909{_id}
    advertised.listeners=PLAINTEXT://{ip}:909{_id}
    zookeeper.connect=localhost:2181,localhost:2182
    zookeeper.connection.timeout.ms=18000
    """.format(_id=_id, ip=remote.ip_address)

    conf = """
    # kafka{_id}.properties
{consensus_config}
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
    group.initial.rebalance.delay.ms=0
    metadata.max.age.ms=3000
    """.format(tdir=kafka_dir, _id=_id, consensus_config=consensus_config)
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
        kraft = is_kraft_mode(current_version)

        kafka_file =  kafka_prefix + current_version + '.tgz'

        link1 = '{apache_mirror_url_front}/kafka/'.format(apache_mirror_url_front=apache_mirror_url_front) + \
            current_version + '/' + kafka_file
        archive_link = 'https://archive.apache.org/dist/kafka/' + current_version + '/' + kafka_file
        log.info('Trying to download Kafka from mirror: %s', link1)
        log.info('Archive fallback URL: %s', archive_link)
        ctx.cluster.only(client).run(
            args=['cd', '{tdir}'.format(tdir=test_dir), run.Raw('&&'),
                  'wget', link1, run.Raw('||'),
                  run.Raw('('), 'rm', '-f', kafka_file, run.Raw('&&'), 'wget', archive_link, run.Raw(')')],
        )

        ctx.cluster.only(client).run(
            args=['cd', '{tdir}'.format(tdir=test_dir), run.Raw('&&'), 'tar', '-xvzf', kafka_file],
        )

        kafka_dir = get_kafka_dir(ctx, config)
        if not kraft:
            # create config for 2 zookeepers
            zookeeper_conf(ctx, client, 1, kafka_dir)
            zookeeper_conf(ctx, client, 2, kafka_dir)
        # create config for 2 brokers
        broker_conf(ctx, client, 1, kafka_dir, kraft)
        broker_conf(ctx, client, 2, kafka_dir, kraft)

        if kraft:
            cluster_id = remote.sh(
                'cd {tdir}/bin && ./kafka-storage.sh random-uuid'.format(
                    tdir=kafka_dir)
            ).strip()
            controller_dirs = {
                1: remote.sh(
                    'cd {tdir}/bin && ./kafka-storage.sh random-uuid'.format(
                        tdir=kafka_dir)
                ).strip(),
                2: remote.sh(
                    'cd {tdir}/bin && ./kafka-storage.sh random-uuid'.format(
                        tdir=kafka_dir)
                ).strip(),
            }
            initial_controllers = (
                '1@localhost:9097:{dir1},2@localhost:9098:{dir2}'
            ).format(dir1=controller_dirs[1], dir2=controller_dirs[2])
            for broker_id in [1, 2]:
                ctx.cluster.only(client).run(
                    args=[
                        'cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
                        './kafka-storage.sh', 'format',
                        '-t', cluster_id,
                        '--initial-controllers', initial_controllers,
                        '-c', '{tdir}/config/kafka{_id}.properties'.format(
                            tdir=kafka_dir, _id=broker_id),
                    ],
                )

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Kafka...')
        kafka_dir=get_kafka_dir(ctx, config)
        for (client,_) in config.items():
            ctx.cluster.only(client).run(
                args=['rm', '-rf', '{tdir}'.format(tdir=kafka_dir)],
            )
            ctx.cluster.only(client).run(
                args=['rm', '-rf', '{tdir}/{doc}'.format(tdir=teuthology.get_testdir(ctx),doc=kafka_file)],
            )


@contextlib.contextmanager
def run_kafka(ctx,config):
    """
    Starts two Kafka brokers. In Zookeeper mode (3.x), it also starts two
    Zookeeper services. In KRaft mode (4.x), each Kafka process is a combined
    broker/controller.
    """
    assert isinstance(config, dict)
    kraft = is_kraft_mode(get_kafka_version(config))
    log.info('Bringing up Kafka%s services...',
             '' if kraft else ' and Zookeeper')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        kafka_dir = get_kafka_dir(ctx, config)

        if not kraft:
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
        log.info('Stopping Kafka%s Services...',
                 '' if kraft else ' and Zookeeper')

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

            if not kraft:
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
    - kafka-failover:
        client.0:
          kafka_version: "4.3"

    A minor version like "4.3" is resolved to the current patch at test
    time via dlcdn.apache.org. A full version like "3.9.2" is used as-is.
    Kafka 4.x is started in KRaft mode automatically; 3.x is started with
    two colocated Zookeepers.
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

    for client_config in config.values():
        if client_config and 'kafka_version' in client_config:
            raw = client_config['kafka_version']
            resolved = resolve_kafka_version(raw)
            if resolved != raw:
                log.info("Kafka version resolved: %s -> %s", raw, resolved)
            client_config['kafka_version'] = resolved

    ctx.kafka_dir = get_kafka_dir(ctx, config)

    log.debug('Kafka config is %s', config)

    with contextutil.nested(
        lambda: install_kafka(ctx=ctx, config=config),
        lambda: run_kafka(ctx=ctx, config=config),
        lambda: run_admin_cmds(ctx=ctx, config=config),
        ):
        yield
