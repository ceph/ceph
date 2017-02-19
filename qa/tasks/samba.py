"""
Samba
"""
import contextlib
import logging
import sys
import time

from teuthology import misc as teuthology
from teuthology.orchestra import run
from teuthology.orchestra.daemon import DaemonGroup

log = logging.getLogger(__name__)


def get_sambas(ctx, roles):
    """
    Scan for roles that are samba.  Yield the id of the the samba role
    (samba.0, samba.1...)  and the associated remote site

    :param ctx: Context
    :param roles: roles for this test (extracted from yaml files)
    """
    for role in roles:
        assert isinstance(role, basestring)
        PREFIX = 'samba.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        yield (id_, remote)


@contextlib.contextmanager
def task(ctx, config):
    """
    Setup samba smbd with ceph vfs module.  This task assumes the samba
    package has already been installed via the install task.

    The config is optional and defaults to starting samba on all nodes.
    If a config is given, it is expected to be a list of
    samba nodes to start smbd servers on.

    Example that starts smbd on all samba nodes::

        tasks:
        - install:
        - install:
            project: samba
            extra_packages: ['samba']
        - ceph:
        - samba:
        - interactive:

    Example that starts smbd on just one of the samba nodes and cifs on the other::

        tasks:
        - samba: [samba.0]
        - cifs: [samba.1]

    An optional backend can be specified, and requires a path which smbd will
    use as the backend storage location:

        roles:
            - [osd.0, osd.1, osd.2, mon.0, mon.1, mon.2, mds.a]
            - [client.0, samba.0]

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - samba:
            samba.0:
              cephfuse: "{testdir}/mnt.0"

    This mounts ceph to {testdir}/mnt.0 using fuse, and starts smbd with
    a UNC of //localhost/cephfuse.  Access through that UNC will be on
    the ceph fuse mount point.

    If no arguments are specified in the samba
    role, the default behavior is to enable the ceph UNC //localhost/ceph
    and use the ceph vfs module as the smbd backend.

    :param ctx: Context
    :param config: Configuration
    """
    log.info("Setting up smbd with ceph vfs...")
    assert config is None or isinstance(config, list) or isinstance(config, dict), \
        "task samba got invalid config"

    if config is None:
        config = dict(('samba.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'samba'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    samba_servers = list(get_sambas(ctx=ctx, roles=config.keys()))

    testdir = teuthology.get_testdir(ctx)

    if not hasattr(ctx, 'daemons'):
        ctx.daemons = DaemonGroup()

    for id_, remote in samba_servers:

        rolestr = "samba.{id_}".format(id_=id_)

        confextras = """vfs objects = ceph
  ceph:config_file = /etc/ceph/ceph.conf"""

        unc = "ceph"
        backend = "/"

        if config[rolestr] is not None:
            # verify that there's just one parameter in role
            if len(config[rolestr]) != 1:
                log.error("samba config for role samba.{id_} must have only one parameter".format(id_=id_))
                raise Exception('invalid config')
            confextras = ""
            (unc, backendstr) = config[rolestr].items()[0]
            backend = backendstr.format(testdir=testdir)

        # on first samba role, set ownership and permissions of ceph root
        # so that samba tests succeed
        if config[rolestr] is None and id_ == samba_servers[0][0]:
            remote.run(
                    args=[
                        'mkdir', '-p', '/tmp/cmnt', run.Raw('&&'),
                        'sudo', 'ceph-fuse', '/tmp/cmnt', run.Raw('&&'),
                        'sudo', 'chown', 'ubuntu:ubuntu', '/tmp/cmnt/', run.Raw('&&'),
                        'sudo', 'chmod', '1777', '/tmp/cmnt/', run.Raw('&&'),
                        'sudo', 'umount', '/tmp/cmnt/', run.Raw('&&'),
                        'rm', '-rf', '/tmp/cmnt',
                        ],
                    )
        else:
            remote.run(
                    args=[
                        'sudo', 'chown', 'ubuntu:ubuntu', backend, run.Raw('&&'),
                        'sudo', 'chmod', '1777', backend,
                        ],
                    )

        teuthology.sudo_write_file(remote, "/usr/local/samba/etc/smb.conf", """
[global]
  workgroup = WORKGROUP
  netbios name = DOMAIN

[{unc}]
  path = {backend}
  {extras}
  writeable = yes
  valid users = ubuntu
""".format(extras=confextras, unc=unc, backend=backend))

        # create ubuntu user
        remote.run(
            args=[
                'sudo', '/usr/local/samba/bin/smbpasswd', '-e', 'ubuntu',
                run.Raw('||'),
                'printf', run.Raw('"ubuntu\nubuntu\n"'),
                run.Raw('|'),
                'sudo', '/usr/local/samba/bin/smbpasswd', '-s', '-a', 'ubuntu'
            ])

        smbd_cmd = [
                'sudo',
                'daemon-helper',
                'term',
                'nostdin',
                '/usr/local/samba/sbin/smbd',
                '-F',
                ]
        ctx.daemons.add_daemon(remote, 'smbd', id_,
                               args=smbd_cmd,
                               logger=log.getChild("smbd.{id_}".format(id_=id_)),
                               stdin=run.PIPE,
                               wait=False,
                               )

        # let smbd initialize, probably a better way...
        seconds_to_sleep = 100
        log.info('Sleeping for %s  seconds...' % seconds_to_sleep)
        time.sleep(seconds_to_sleep)
        log.info('Sleeping stopped...')

    try:
        yield
    finally:
        log.info('Stopping smbd processes...')
        exc_info = (None, None, None)
        for d in ctx.daemons.iter_daemons_of_role('smbd'):
            try:
                d.stop()
            except (run.CommandFailedError,
                    run.CommandCrashedError,
                    run.ConnectionLostError):
                exc_info = sys.exc_info()
                log.exception('Saw exception from %s.%s', d.role, d.id_)
        if exc_info != (None, None, None):
            raise exc_info[0], exc_info[1], exc_info[2]

        for id_, remote in samba_servers:
            remote.run(
                args=[
                    'sudo',
                    'rm', '-rf',
                    '/usr/local/samba/etc/smb.conf',
                    '/usr/local/samba/private/*',
                    '/usr/local/samba/var/run/',
                    '/usr/local/samba/var/locks',
                    '/usr/local/samba/var/lock',
                    ],
                )
            # make sure daemons are gone
            try:
                remote.run(
                    args=[
                        'while',
                        'sudo', 'killall', '-9', 'smbd',
                        run.Raw(';'),
                        'do', 'sleep', '1',
                        run.Raw(';'),
                        'done',
                        ],
                    )

                remote.run(
                    args=[
                        'sudo',
                        'lsof',
                        backend,
                        ],
                    check_status=False
                    )
                remote.run(
                    args=[
                        'sudo',
                        'fuser',
                        '-M',
                        backend,
                        ],
                    check_status=False
                    )
            except Exception:
                log.exception("Saw exception")
                pass
