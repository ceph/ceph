from cStringIO import StringIO

import logging

from teuthology import misc as teuthology
from ..orchestra import run

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

def install_firmware(ctx, config):
    # uri = 'git://git.kernel.org/pub/scm/linux/kernel/git/firmware/linux-firmware.git'
    uri = 'git://ceph.com/git/linux-firmware.git'
    fw_dir = '/lib/firmware/updates'

    for role in config.iterkeys():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Installing linux-firmware on {role}...'.format(role=role))
        role_remote.run(
            args=[
                # kludge around mysterious 0-byte .git/HEAD files
                'cd', fw_dir,
                run.Raw('&&'),
                'test', '-d', '.git',
                run.Raw('&&'),
                'test', '!', '-s', '.git/HEAD',
                run.Raw('&&'),
                'sudo', 'rm', '-rf', '.git',
                run.Raw(';'),
                # init
                'sudo', 'install', '-d', '-m0755', fw_dir,
                run.Raw('&&'),
                'cd', fw_dir,
                run.Raw('&&'),
                'sudo', 'git', 'init',
                ],
            )
        role_remote.run(
            args=[
                'sudo', 'git', '--git-dir=%s/.git' % fw_dir, 'config',           
                '--get', 'remote.origin.url', run.Raw('>/dev/null'),
                run.Raw('||'),
                'sudo', 'git', '--git-dir=%s/.git' % fw_dir,           
                'remote', 'add', 'origin', uri,
                ],
            )
        role_remote.run(
            args=[
                'cd', fw_dir,
                run.Raw('&&'),
                'sudo', 'git', 'fetch', 'origin',
                run.Raw('&&'),
                'sudo', 'git', 'reset', '--hard', 'origin/master'
                ],
            )


def install_and_reboot(ctx, config):
    procs = {}
    for role, sha1 in config.iteritems():
        log.info('Installing kernel version {sha1} on {role}...'.format(sha1=sha1,
                                                                        role=role))
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        _, deb_url = teuthology.get_ceph_binary_url(
            package='kernel',
            sha1=sha1, 
            format='deb',
            flavor='basic',
            arch='x86_64',
            dist='precise',
            )
        log.info('fetching kernel from {url}'.format(url=deb_url))
        proc = role_remote.run(
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
                # and now extract the actual boot image name from the deb
                'dpkg-deb',
                '--fsys-tarfile',
                '/tmp/linux-image.deb',
                run.Raw('|'),
                'tar',
                '-t',
                '-v',
                '-f', '-',
                '--wildcards',
                '--',
                './boot/vmlinuz-*',
                run.Raw('|'),
                # we can only rely on mawk being installed, so just call it explicitly
                'mawk',
                # and use the image name to construct the content of
                # the grub menu entry, so we can default to it;
                # hardcoded to assume Ubuntu, English, etc.
                r'{sub("^\\./boot/vmlinuz-", "", $6); print "cat <<EOF\n" "set default=\"Ubuntu, with Linux " $6 "\"\n" "EOF"}',
                # make it look like an emacs backup file so
                # unfortunately timed update-grub runs don't pick it
                # up yet; use sudo tee so we are able to write to /etc
                run.Raw('|'),
                'sudo',
                'tee',
                '--',
                '/etc/grub.d/01_ceph_kernel.tmp~',
                run.Raw('>/dev/null'),
                run.Raw('&&'),
                'sudo',
                'chmod',
                'a+x',
                '--',
                '/etc/grub.d/01_ceph_kernel.tmp~',
                run.Raw('&&'),
                'sudo',
                'mv',
                '--',
                '/etc/grub.d/01_ceph_kernel.tmp~',
                '/etc/grub.d/01_ceph_kernel',
                run.Raw('&&'),
                'sudo',
                'update-grub',
                run.Raw('&&'),
                'rm',
                '/tmp/linux-image.deb',
                run.Raw('&&'),
                'sudo',
                'shutdown',
                '-r',
                'now',
                ],
            wait=False,
            )
        procs[role_remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug('Waiting for install on %s to complete...', name)
        proc.exitstatus.get()

def wait_for_reboot(ctx, need_install, timeout):
    """
    Loop reconnecting and checking kernel versions until
    they're all correct or the timeout is exceeded.
    """
    import time
    starttime = time.time()
    while need_install:
        teuthology.reconnect(ctx, timeout)
        for client in need_install.keys():
            log.info('Checking client {client} for new kernel version...'.format(client=client))
            try:
                assert not need_to_install(ctx, client, need_install[client]), \
                        'failed to install new kernel version within timeout'
                del need_install[client]
            except:
                # ignore connection resets and asserts while time is left
                if time.time() - starttime > timeout:
                    raise
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

    timeout = 300
    if config is not None and 'timeout' in config:
        timeout = config.pop('timeout')

    config = normalize_config(ctx, config)
    validate_config(ctx, config)

    need_install = {}
    for role, role_config in config.iteritems():
        sha1, _ = teuthology.get_ceph_binary_url(
            package='kernel',
            branch=role_config.get('branch'),
            tag=role_config.get('tag'),
            sha1=role_config.get('sha1'),
            flavor='basic',
            format='deb',
            dist='precise',
            arch='x86_64',
            )
        log.debug('sha1 for {role} is {sha1}'.format(role=role, sha1=sha1))
        ctx.summary['{role}-kernel-sha1'.format(role=role)] = sha1
        if need_to_install(ctx, role, sha1):
            need_install[role] = sha1

    if need_install:
        install_firmware(ctx, need_install)
        install_and_reboot(ctx, need_install)
        wait_for_reboot(ctx, need_install, timeout)
