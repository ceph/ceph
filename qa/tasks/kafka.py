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

KAFKA_PORTS = {
    'PLAINTEXT': 9092,
    'SSL': 9093,
    'SASL_SSL': 9094,
    'SASL_PLAINTEXT': 9095,
    'MTLS': 9096,
}

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
    Resolve a minor version like "4.3" to the current patch (e.g. "4.3.1")
    by querying Apache's live mirror. A fully-qualified version (three
    dotted components, e.g. "3.9.2") is returned unchanged, so it can
    still be used as a literal patch pin.

    Raises RuntimeError if the minor line is not on dlcdn.apache.org.
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

KRAFT_CONTROLLER_PORT = 9097

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

    kerberos = getattr(ctx, 'kerberos', None)

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

        client_kerberos = kerberos.get(client) if kerberos else None
        broker_conf(ctx, client, kafka_dir, client_kerberos, kraft)

        if kraft:
            # Kafka 4.x KRaft mode requires storage to be formatted before
            # the broker starts. --standalone auto-bootstraps a single-node
            # KRaft cluster.
            uuid_result = remote.sh(
                'cd {tdir}/bin && ./kafka-storage.sh random-uuid'.format(
                    tdir=kafka_dir)
            ).strip()
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/bin'.format(tdir=kafka_dir), run.Raw('&&'),
                    './kafka-storage.sh', 'format',
                    '-t', uuid_result,
                    '-c', '{tdir}/config/server.properties'.format(tdir=kafka_dir),
                    '--standalone',
                ],
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


def broker_conf(ctx, client, kafka_dir, kerberos, kraft):
    """writing a custom server.properties config file"""
    (remote,) = ctx.cluster.only(client).remotes.keys()
    ip = remote.ip_address
    if kraft:
        identity = (
            "node.id=1\n"
            "process.roles=broker,controller\n"
        )
        controller_listener = ",CONTROLLER://0.0.0.0:{controller}".format(
            controller=KRAFT_CONTROLLER_PORT)
        controller_protocol = ",CONTROLLER:PLAINTEXT"
        consensus_config = (
            "controller.listener.names=CONTROLLER\n"
            "controller.quorum.bootstrap.servers=localhost:{controller}\n"
        ).format(controller=KRAFT_CONTROLLER_PORT)
    else:
        identity = "broker.id=0\n"
        controller_listener = ""
        controller_protocol = ""
        consensus_config = (
            "zookeeper.connect=localhost:2181\n"
            "zookeeper.connection.timeout.ms=18000\n"
        )
    conf = (
        identity +
        "listeners=PLAINTEXT://0.0.0.0:{plaintext},SSL://0.0.0.0:{ssl},"
        "SASL_SSL://0.0.0.0:{sasl_ssl},SASL_PLAINTEXT://0.0.0.0:{sasl_plaintext},"
        "MTLS://0.0.0.0:{mtls}" + controller_listener + "\n"
        "advertised.listeners=PLAINTEXT://{ip}:{plaintext},SSL://{ip}:{ssl},"
        "SASL_SSL://{ip}:{sasl_ssl},SASL_PLAINTEXT://{ip}:{sasl_plaintext},"
        "MTLS://{ip}:{mtls}\n"
        "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,"
        "SASL_SSL:SASL_SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,MTLS:SSL"
        + controller_protocol + "\n"
        "inter.broker.listener.name=PLAINTEXT\n"
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
        + consensus_config +
        "group.initial.rebalance.delay.ms=0\n"
        # SSL configuration
        "ssl.keystore.location={tdir}/server.keystore.jks\n"
        "ssl.keystore.password=mypassword\n"
        "ssl.key.password=mypassword\n"
        "ssl.truststore.location={tdir}/server.truststore.jks\n"
        "ssl.truststore.password=mypassword\n"
        "ssl.client.auth=requested\n"
        # mTLS listener (port 9096) requires client certificate
        "listener.name.mtls.ssl.client.auth=required\n"
        "listener.name.mtls.ssl.keystore.location={tdir}/server.keystore.jks\n"
        "listener.name.mtls.ssl.keystore.password=mypassword\n"
        "listener.name.mtls.ssl.key.password=mypassword\n"
        "listener.name.mtls.ssl.truststore.location={tdir}/server.truststore.jks\n"
        "listener.name.mtls.ssl.truststore.password=mypassword\n"
        # SASL mechanisms
        "sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512,GSSAPI,OAUTHBEARER\n"
        "sasl.mechanism.inter.broker.protocol=PLAIN\n"
        "sasl.kerberos.service.name={service_name}\n"
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
        'listener.name.sasl_ssl.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \\\n'
        '  useKeyTab=true \\\n'
        '  storeKey=true \\\n'
        '  keyTab="{keytab}" \\\n'
        '  principal="{principal}";\n'
        'listener.name.sasl_ssl.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;\n'
        'listener.name.sasl_ssl.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler\n'
        'listener.name.sasl_ssl.oauthbearer.sasl.oauthbearer.jwks.endpoint.url={jwks}\n'
        'listener.name.sasl_ssl.oauthbearer.sasl.oauthbearer.expected.audience={oauth_client}\n'
        'listener.name.sasl_ssl.oauthbearer.sasl.oauthbearer.expected.issuer={issuer}\n'
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
        'listener.name.sasl_plaintext.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \\\n'
        '  useKeyTab=true \\\n'
        '  storeKey=true \\\n'
        '  keyTab="{keytab}" \\\n'
        '  principal="{principal}";\n'
        'listener.name.sasl_plaintext.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;\n'
        'listener.name.sasl_plaintext.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler\n'
        'listener.name.sasl_plaintext.oauthbearer.sasl.oauthbearer.jwks.endpoint.url={jwks}\n'
        'listener.name.sasl_plaintext.oauthbearer.sasl.oauthbearer.expected.audience={oauth_client}\n'
        'listener.name.sasl_plaintext.oauthbearer.sasl.oauthbearer.expected.issuer={issuer}\n'
    ).format(
        tdir=kafka_dir,
        ip=ip,
        plaintext=KAFKA_PORTS['PLAINTEXT'],
        ssl=KAFKA_PORTS['SSL'],
        sasl_ssl=KAFKA_PORTS['SASL_SSL'],
        sasl_plaintext=KAFKA_PORTS['SASL_PLAINTEXT'],
        mtls=KAFKA_PORTS['MTLS'],
        service_name=kerberos['service_name'] if kerberos else '',
        keytab=kerberos['kafka_keytab'] if kerberos else '',
        principal=kerberos['kafka_principal'] if kerberos else '',
        jwks=getattr(ctx, 'dex_issuer', None),
        oauth_client=getattr(ctx, 'dex_client_id', None),
        issuer=getattr(ctx, 'dex_jwks_url', None)
    )
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
    Starts the Kafka broker. In Zookeeper mode (3.x) also starts Zookeeper
    first; KRaft mode (4.x) embeds the controller in the broker process.
    """
    assert isinstance(config, dict)
    kraft = is_kraft_mode(get_kafka_version(config))
    log.info('Bringing up Kafka%s services...',
             '' if kraft else ' and Zookeeper')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        if not kraft:
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
        log.info('Stopping Kafka%s services...',
                 '' if kraft else ' and Zookeeper')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            ctx.cluster.only(client).run(
                args=['cd', '{tdir}/bin'.format(tdir=get_kafka_dir(ctx, config)), run.Raw('&&'),
                 './kafka-server-stop.sh',
                 '{tir}/config/server.properties'.format(tir=get_kafka_dir(ctx, config)),
                ],
            )

            time.sleep(5)

            if not kraft:
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
          kafka_version: "4.3"

    A minor version like "4.3" is resolved to the current patch at test
    time via dlcdn.apache.org. A full version like "3.9.2" is used as-is.
    Kafka 4.x is started in KRaft mode automatically; 3.x is started with
    a colocated Zookeeper. Pick whichever version your suite needs.
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
