"""
Deploy and configure a Kerberos KDC for Teuthology.

This task stands up a single-node MIT Kerberos KDC and creates the service
principals / keytabs used by the GSSAPI Kafka bucket-notification tests:

  - kafka/<remote-ip>@CEPH.TEST  (used by the Kafka broker)
  - rgw/<remote-ip>@CEPH.TEST    (used by RGW and the test consumer)

"""
import contextlib
import logging

from teuthology import contextutil
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)

REALM = 'CEPH.TEST'
MASTER_PASSWORD = 'ceph-kdc-master'
KEYTAB_DIR = '/etc/krb5-keytabs'
KAFKA_KEYTAB = '{}/kafka.service.keytab'.format(KEYTAB_DIR)
RGW_KEYTAB = '{}/rgw.service.keytab'.format(KEYTAB_DIR)
SERVICE_NAME = 'kafka'


def _distro_paths(remote):
    """Return distro-specific KDC paths and service names."""
    if remote.os.package_type == 'rpm':
        return {
            'kdc_conf_dir': '/var/kerberos/krb5kdc',
            'kdc_service': 'krb5kdc',
            'admin_service': 'kadmin',
        }
    return {
        'kdc_conf_dir': '/etc/krb5kdc',
        'kdc_service': 'krb5-kdc',
        'admin_service': 'krb5-admin-server',
    }


@contextlib.contextmanager
def install_kerberos(ctx, config):
    """Install the Kerberos KDC, admin server and GSSAPI SASL plugin."""
    assert isinstance(config, dict)
    log.info('Installing Kerberos packages...')

    deb_packages = [
        'krb5-kdc', 'krb5-admin-server', 'krb5-user',
        'libkrb5-dev', 'libsasl2-modules-gssapi-mit',
    ]
    rpm_packages = [
        'krb5-devel', 'krb5-server', 'krb5-workstation',
        'cyrus-sasl-gssapi',
    ]

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        if remote.os.package_type == 'rpm':
            remote.run(args=['sudo', 'dnf', 'install', '-y'] + rpm_packages)
        else:
            remote.run(args=['sudo', 'apt-get', 'update'])
            remote.run(args=[
                'sudo', 'DEBIAN_FRONTEND=noninteractive',
                'apt-get', 'install', '-y',
            ] + deb_packages)

    try:
        yield
    finally:
        log.info('Removing Kerberos packages...')
        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            if remote.os.package_type == 'rpm':
                remote.run(
                    args=['sudo', 'dnf', 'remove', '-y'] + rpm_packages,
                    check_status=False,
                )
            else:
                remote.run(
                    args=['sudo', 'DEBIAN_FRONTEND=noninteractive',
                          'apt-get', 'purge', '-y'] + deb_packages,
                    check_status=False,
                )


@contextlib.contextmanager
def setup_kdc(ctx, config):
    """Create the realm, principals and keytabs and start the KDC."""
    assert isinstance(config, dict)
    log.info('Setting up Kerberos KDC...')

    if not hasattr(ctx, 'kerberos'):
        ctx.kerberos = {}

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        try:
            ip = remote.ip_address
            paths = _distro_paths(remote)
            kafka_principal = 'kafka/{ip}@{realm}'.format(ip=ip, realm=REALM)
            rgw_principal = 'rgw/{ip}@{realm}'.format(ip=ip, realm=REALM)

            # /etc/krb5.conf: rdns=false keeps the SPN IP-based (no reverse
            # lookup to a hostname); dns_lookup_kdc/realm=false keeps the KDC 
            # and realm settings local and static, avoiding any DNS-related 
            # issues in the test environment.
            krb5_conf = (
                "[libdefaults]\n"
                "    default_realm = {realm}\n"
                "    dns_lookup_kdc = false\n"
                "    dns_lookup_realm = false\n"
                "    rdns = false\n"
                "    udp_preference_limit = 1\n"
                "\n"
                "[realms]\n"
                "    {realm} = {{\n"
                "        kdc = {ip}\n"
                "        admin_server = {ip}\n"
                "    }}\n"
            ).format(realm=REALM, ip=ip)
            remote.sudo_write_file('/etc/krb5.conf', krb5_conf)

            # kdc.conf
            kdc_conf = (
                "[kdcdefaults]\n"
                "    kdc_ports = 88\n"
                "    kdc_tcp_ports = 88\n"
                "\n"
                "[realms]\n"
                "    {realm} = {{\n"
                "        max_life = 24h 0m 0s\n"
                "        max_renewable_life = 7d 0h 0m 0s\n"
                "    }}\n"
            ).format(realm=REALM)
            remote.run(args=['sudo', 'mkdir', '-p', paths['kdc_conf_dir']])
            remote.sudo_write_file(
                '{}/kdc.conf'.format(paths['kdc_conf_dir']), kdc_conf)
            remote.sudo_write_file(
                '{}/kadm5.acl'.format(paths['kdc_conf_dir']),
                '*/admin@{realm}    *\n'.format(realm=REALM))

            # create the KDC database
            remote.run(args=[
                'sudo', 'kdb5_util', 'create', '-s', '-r', REALM,
                '-P', MASTER_PASSWORD,
            ])

            # create principals and extract keytabs
            remote.run(args=[
                'sudo', 'kadmin.local', '-q',
                'addprinc -randkey {}'.format(kafka_principal),
            ])
            remote.run(args=[
                'sudo', 'kadmin.local', '-q',
                'addprinc -randkey {}'.format(rgw_principal),
            ])
            remote.run(args=['sudo', 'mkdir', '-p', KEYTAB_DIR])
            remote.run(args=[
                'sudo', 'kadmin.local', '-q',
                'ktadd -k {keytab} {principal}'.format(
                    keytab=KAFKA_KEYTAB, principal=kafka_principal),
            ])
            remote.run(args=[
                'sudo', 'kadmin.local', '-q',
                'ktadd -k {keytab} {principal}'.format(
                    keytab=RGW_KEYTAB, principal=rgw_principal),
            ])
            # make the keytabs world-readable to avoid "keytab contains 
            # no suitable keys" permission failures.
            remote.run(args=[
                'sudo', 'chmod', '644', KAFKA_KEYTAB, RGW_KEYTAB,
            ])

            # restart the KDC and admin server
            remote.run(args=[
                'sudo', 'systemctl', 'restart',
                paths['kdc_service'], paths['admin_service'],
            ])

            # kinit with the RGW principal and list the tickets
            remote.run(args=[
                'kinit', '-kt', RGW_KEYTAB, rgw_principal, run.Raw('&&'), 'klist',
            ])

            ctx.kerberos[client] = {
                'realm': REALM,
                'service_name': SERVICE_NAME,
                'principal': rgw_principal,
                'keytab': RGW_KEYTAB,
                'kafka_principal': kafka_principal,
                'kafka_keytab': KAFKA_KEYTAB,
                'ip': ip,
            }
        except Exception as exc:
            log.exception('Kerberos setup failed for %s', client)
            raise RuntimeError('Kerberos setup is failing') from exc

    try:
        yield
    finally:
        log.info('Tearing down Kerberos KDC...')
        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            paths = _distro_paths(remote)
            remote.run(
                args=['sudo', 'systemctl', 'stop',
                      paths['kdc_service'], paths['admin_service']],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'kdb5_util', 'destroy', '-f'],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'rm', '-rf', KEYTAB_DIR, '/etc/krb5.conf',
                      '/tmp/krb5cc_bntests'],
                check_status=False,
            )
            ctx.kerberos.pop(client, None)


@contextlib.contextmanager
def task(ctx, config):
    """
    Set up a Kerberos KDC for the GSSAPI Kafka bucket-notification tests.

    Must run before the kafka and notification-tests tasks::

        tasks:
        - kerberos:
            client.0:
        - kafka:
            client.0:
              kafka_version: 3.9.2
        - notification-tests:
            client.0:
              extra_attr: ["kafka_test", "kafka_security_test"]
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task kerberos only supports a list or dictionary for configuration"

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    log.debug('Kerberos config is %s', config)

    with contextutil.nested(
        lambda: install_kerberos(ctx=ctx, config=config),
        lambda: setup_kdc(ctx=ctx, config=config),
        ):
        yield
