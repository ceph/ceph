from cStringIO import StringIO

import logging
import errno
import socket
import time

from teuthology import misc as teuthology
from orchestra import connection, run

log = logging.getLogger(__name__)

def normalize_config(ctx, config):
    """
    Returns a config whose keys are all real roles.
    Generic roles (client, mon, osd, etc.) are replaced with
    the actual roles (client.0, client.1, etc.). If the config
    specifies a different version for a specific role, this is
    unchanged.

    For example, with 3 OSDs this::

         osd:
           tag: v3.0
         osd.1:
           branch: new_btrfs

    is transformed into::

         osd.0:
           tag: v3.0
         osd.1:
           branch: new_btrfs
         osd.2:
           tag: v3.0

    If config is None or just specifies a version to use,
    it is applied to all nodes.
    """
    if config is None or \
            len(config) == 1 and config.keys() in [['tag'], ['branch'], ['sha1']]:
        new_config = {}
        if config is None:
            config = {'branch': 'master'}
        for _, roles_for_host in ctx.cluster.remotes.iteritems():
            new_config[roles_for_host[0]] = config
        return new_config

    new_config = {}
    for role, role_config in config.iteritems():
        if role_config is None:
            role_config = {'branch': 'master'}
        if '.' in role:
            new_config[role] = role_config
        else:
            for id_ in teuthology.all_roles_of_type(ctx.cluster, role):
                name = '{type}.{id}'.format(type=role, id=id_)
                # specific overrides generic
                if name not in config:
                    new_config[name] = role_config
    return new_config

def validate_config(ctx, config):
    for _, roles_for_host in ctx.cluster.remotes.iteritems():
        kernel = None
        for role in roles_for_host:
            role_kernel = config.get(role, kernel)
            if kernel is None:
                kernel = role_kernel
            elif role_kernel is not None:
                assert kernel == role_kernel, \
                    "everything on the same host must use the same kernel"
                if role in config:
                    del config[role]

def need_to_install(ctx, role, sha1):
    ret = True
    log.info('Checking kernel version of {role}...'.format(role=role))
    version_fp = StringIO()
    ctx.cluster.only(role).run(
        args=[
            'uname',
            '-r',
            ],
        stdout=version_fp,
        )
    version = version_fp.getvalue().rstrip('\n')
    log.debug('current kernel version is: {version}'.format(version=version))
    if '-g' in version:
        _, current_sha1 = version.rsplit('-g', 1)
        if sha1.startswith(current_sha1):
            log.debug('current sha1 is the same, do not need to install')
            ret = False
    version_fp.close()
    return ret

def install_and_reboot(ctx, config):
    for role, sha1 in config.iteritems():
        log.info('Installing kernel version {sha1} on {role}...'.format(sha1=sha1,
                                                                        role=role))
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        _, deb_url = teuthology.get_ceph_binary_url(sha1=sha1, flavor='kernel')
        log.info('fetching kernel from {url}'.format(url=deb_url))
        role_remote.run(
            args=[
                'echo',
                'linux-image.deb',
                run.Raw('|'),
                'wget',
                '-nv',
                '-O',
                '/tmp/linux-image.deb',
                '--base={url}'.format(url=deb_url),
                '--input-file=-',
                run.Raw('&&'),
                'sudo',
                'dpkg',
                '-i',
                '/tmp/linux-image.deb',
                run.Raw('&&'),
                'rm',
                '/tmp/linux-image.deb',
                run.Raw('&&'),
                'sudo',
                'shutdown',
                '-r',
                'now',
                ],
            )

def reconnect(ctx, timeout):
    log.info('Re-opening connections...')
    starttime = time.time()
    need_reconnect = ctx.cluster.remotes.keys()
    while True:
        for remote in list(need_reconnect):
            try:
                remote.ssh = connection.connect(
                    user_at_host=remote.name,
                    host_key=ctx.config['targets'][remote.name],
                    )
            except socket.error as e:
                if hasattr(e, '__getitem__'):
                    if e[0] not in [errno.ECONNREFUSED, errno.ETIMEDOUT,
                                errno.EHOSTUNREACH, errno.EHOSTDOWN] or \
                                time.time() - starttime > timeout:
                        log.exception('unknown socket error: %s', repr(e))
                        raise
                else:
                    log.exception('weird socket error without error code')
                    raise
            else:
                need_reconnect.remove(remote)

        if not need_reconnect:
            break
        log.debug('waited {elapsed}'.format(elapsed=str(time.time() - starttime)))
        time.sleep(1)


def task(ctx, config):
    """
    Make sure the specified kernel is installed.
    This can be a branch, tag, or sha1 of ceph-client.git.

    To install the kernel from the master branch on all hosts::

        kernel:
        tasks:
        - ceph:

    To wait 5 minutes for hosts to reboot::

        kernel:
          timeout: 300
        tasks:
        - ceph:

    To specify different kernels for each client::

        kernel:
          client.0:
            branch: foo
          client.1:
            tag: v3.0rc1
          client.2:
            sha1: db3540522e955c1ebb391f4f5324dff4f20ecd09
        tasks:
        - ceph:

    You can specify a branch, tag, or sha1 for all roles
    of a certain type (more specific roles override this)::

        kernel:
          client:
            tag: v3.0
          osd:
            branch: btrfs_fixes
          client.1:
            branch: more_specific_branch
          osd.3:
            branch: master
    """
    assert config is None or isinstance(config, dict), \
        "task kernel only supports a dictionary for configuration"

    timeout = 180
    if config is not None and 'timeout' in config:
        timeout = config.pop('timeout')

    config = normalize_config(ctx, config)
    validate_config(ctx, config)

    need_install = {}
    for role, role_config in config.iteritems():
        sha1, _ = teuthology.get_ceph_binary_url(
            branch=role_config.get('branch'),
            tag=role_config.get('tag'),
            sha1=role_config.get('sha1'),
            flavor='kernel',
            )
        log.debug('sha1 for {role} is {sha1}'.format(role=role, sha1=sha1))
        ctx.summary['{role}-kernel-sha1'.format(role=role)] = sha1
        if need_to_install(ctx, role, sha1):
            need_install[role] = sha1

    if len(need_install) > 0:
        install_and_reboot(ctx, need_install)
        reconnect(ctx, timeout)

    for client, sha1 in need_install.iteritems():
        log.info('Checking client {client} for new kernel version...'.format(client=client))
        assert not need_to_install(ctx, client, sha1), \
            "Client did not boot to the new kernel!"
