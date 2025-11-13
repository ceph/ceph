"""
Ceph teuthology task for managed smb features.
"""
from io import StringIO
import contextlib
import logging
import json
import time

from teuthology.exceptions import ConfigError, CommandFailedError


log = logging.getLogger(__name__)


def _disable_systemd_resolved(ctx, remote):
    r = remote.run(args=['ss', '-lunH'], stdout=StringIO())
    # this heuristic tries to detect if systemd-resolved is running
    if '%lo:53' not in r.stdout.getvalue():
        return
    log.info('Disabling systemd-resolved on %s', remote.shortname)
    # Samba AD DC container DNS support conflicts with resolved stub
    # resolver when using host networking. And we want host networking
    # because it is the simplest thing to set up.  We therefore will turn
    # off the stub resolver.
    r = remote.run(
        args=['sudo', 'cat', '/etc/systemd/resolved.conf'],
        stdout=StringIO(),
    )
    resolved_conf = r.stdout.getvalue()
    setattr(ctx, 'orig_resolved_conf', resolved_conf)
    new_resolved_conf = (
        resolved_conf + '\n# EDITED BY TEUTHOLOGY: deploy_samba_ad_dc\n'
    )
    if '[Resolve]' not in new_resolved_conf.splitlines():
        new_resolved_conf += '[Resolve]\n'
    new_resolved_conf += 'DNSStubListener=no\n'
    remote.write_file(
        path='/etc/systemd/resolved.conf',
        data=new_resolved_conf,
        sudo=True,
    )
    remote.run(args=['sudo', 'systemctl', 'restart', 'systemd-resolved'])
    r = remote.run(args=['ss', '-lunH'], stdout=StringIO())
    assert '%lo:53' not in r.stdout.getvalue()
    # because docker is a big fat persistent deamon, we need to bounce it
    # after resolved is restarted
    remote.run(args=['sudo', 'systemctl', 'restart', 'docker'])


def _reset_systemd_resolved(ctx, remote):
    orig_resolved_conf = getattr(ctx, 'orig_resolved_conf', None)
    if not orig_resolved_conf:
        return  # no orig_resolved_conf means nothing to reset
    log.info('Resetting systemd-resolved state on %s', remote.shortname)
    remote.write_file(
        path='/etc/systemd/resolved.conf',
        data=orig_resolved_conf,
        sudo=True,
    )
    remote.run(args=['sudo', 'systemctl', 'restart', 'systemd-resolved'])
    setattr(ctx, 'orig_resolved_conf', None)


def _samba_ad_dc_conf(ctx, remote, cengine):
    # this config has not been tested outside of smithi nodes. it's possible
    # that this will break when used elsewhere because we have to list
    # interfaces explicitly. Later I may add a feature to sambacc to exclude
    # known-unwanted interfaces that having to specify known good interfaces.
    cf = {
        "samba-container-config": "v0",
        "configs": {
            "demo": {
                "instance_features": ["addc"],
                "domain_settings": "sink",
                "instance_name": "dc1",
            }
        },
        "domain_settings": {
            "sink": {
                "realm": "DOMAIN1.SINK.TEST",
                "short_domain": "DOMAIN1",
                "admin_password": "Passw0rd",
                "interfaces": {
                    "exclude_pattern": "^docker[0-9]+$",
                },
            }
        },
        "domain_groups": {
            "sink": [
                {"name": "supervisors"},
                {"name": "employees"},
                {"name": "characters"},
                {"name": "bulk"},
            ]
        },
        "domain_users": {
            "sink": [
                {
                    "name": "bwayne",
                    "password": "1115Rose.",
                    "given_name": "Bruce",
                    "surname": "Wayne",
                    "member_of": ["supervisors", "characters", "employees"],
                },
                {
                    "name": "ckent",
                    "password": "1115Rose.",
                    "given_name": "Clark",
                    "surname": "Kent",
                    "member_of": ["characters", "employees"],
                },
                {
                    "name": "user0",
                    "password": "1115Rose.",
                    "given_name": "George0",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
                {
                    "name": "user1",
                    "password": "1115Rose.",
                    "given_name": "George1",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
                {
                    "name": "user2",
                    "password": "1115Rose.",
                    "given_name": "George2",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
                {
                    "name": "user3",
                    "password": "1115Rose.",
                    "given_name": "George3",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
            ]
        },
    }
    cf_json = json.dumps(cf)
    remote.run(args=['sudo', 'mkdir', '-p', '/var/tmp/samba'])
    remote.write_file(
        path='/var/tmp/samba/container.json', data=cf_json, sudo=True
    )
    return [
        '--volume=/var/tmp/samba:/etc/samba-container:ro',
        '-eSAMBACC_CONFIG=/etc/samba-container/container.json',
    ]


@contextlib.contextmanager
def configure_samba_client_container(ctx, config):
    # TODO: deduplicate logic between this task and deploy_samba_ad_dc
    role = config.get('role')
    samba_client_image = config.get(
        'samba_client_image', 'quay.io/samba.org/samba-client:latest'
    )
    if not role:
        raise ConfigError(
            "you must specify a role to discover container engine / pull image"
        )
    (remote,) = ctx.cluster.only(role).remotes.keys()
    cengine = 'podman'
    try:
        log.info("Testing if podman is available")
        remote.run(args=['sudo', cengine, '--help'])
    except CommandFailedError:
        log.info("Failed to find podman. Using docker")
        cengine = 'docker'

    remote.run(args=['sudo', cengine, 'pull', samba_client_image])
    samba_client_container_cmd = [
        'sudo',
        cengine,
        'run',
        '--rm',
        '--net=host',
        '-eKRB5_CONFIG=/dev/null',
        samba_client_image,
    ]

    setattr(ctx, 'samba_client_container_cmd', samba_client_container_cmd)
    try:
        yield
    finally:
        setattr(ctx, 'samba_client_container_cmd', None)


@contextlib.contextmanager
def deploy_samba_ad_dc(ctx, config):
    role = config.get('role')
    ad_dc_image = config.get(
        'ad_dc_image', 'quay.io/samba.org/samba-ad-server:latest'
    )
    samba_client_image = config.get(
        'samba_client_image', 'quay.io/samba.org/samba-client:latest'
    )
    test_user_pass = config.get('test_user_pass', 'DOMAIN1\\ckent%1115Rose.')
    if not role:
        raise ConfigError(
            "you must specify a role to allocate a host for the AD DC"
        )
    (remote,) = ctx.cluster.only(role).remotes.keys()
    ip = remote.ssh.get_transport().getpeername()[0]
    cengine = 'podman'
    try:
        log.info("Testing if podman is available")
        remote.run(args=['sudo', cengine, '--help'])
    except CommandFailedError:
        log.info("Failed to find podman. Using docker")
        cengine = 'docker'
    remote.run(args=['sudo', cengine, 'pull', ad_dc_image])
    remote.run(args=['sudo', cengine, 'pull', samba_client_image])
    _disable_systemd_resolved(ctx, remote)
    remote.run(
        args=[
            'sudo',
            'mkdir',
            '-p',
            '/var/lib/samba/container/logs',
            '/var/lib/samba/container/data',
        ]
    )
    remote.run(
        args=[
            'sudo',
            cengine,
            'run',
            '-d',
            '--name=samba-ad',
            '--network=host',
            '--privileged',
        ]
        + _samba_ad_dc_conf(ctx, remote, cengine)
        + [ad_dc_image]
    )

    # test that the ad dc is running and basically works
    connected = False
    samba_client_container_cmd = [
        'sudo',
        cengine,
        'run',
        '--rm',
        '--net=host',
        f'--dns={ip}',
        '-eKRB5_CONFIG=/dev/null',
        samba_client_image,
    ]
    for idx in range(10):
        time.sleep((2 ** (1 + idx)) / 8)
        log.info("Probing SMB status of DC %s, idx=%s", ip, idx)
        cmd = samba_client_container_cmd + [
            'smbclient',
            '-U',
            test_user_pass,
            '//domain1.sink.test/sysvol',
            '-c',
            'ls',
        ]
        try:
            remote.run(args=cmd)
            connected = True
            log.info("SMB status probe succeeded")
            break
        except CommandFailedError:
            pass
    if not connected:
        raise RuntimeError('failed to connect to AD DC SMB share')

    setattr(ctx, 'samba_ad_dc_ip', ip)
    setattr(ctx, 'samba_client_container_cmd', samba_client_container_cmd)
    try:
        yield
    finally:
        try:
            remote.run(args=['sudo', cengine, 'stop', 'samba-ad'])
        except CommandFailedError:
            log.error("Failed to stop samba-ad container")
        try:
            remote.run(args=['sudo', cengine, 'rm', 'samba-ad'])
        except CommandFailedError:
            log.error("Failed to remove samba-ad container")
        remote.run(
            args=[
                'sudo',
                'rm',
                '-rf',
                '/var/lib/samba/container/logs',
                '/var/lib/samba/container/data',
            ]
        )
        _reset_systemd_resolved(ctx, remote)
        setattr(ctx, 'samba_ad_dc_ip', None)
        setattr(ctx, 'samba_client_container_cmd', None)
