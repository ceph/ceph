from cStringIO import StringIO

import logging
import re
import shlex

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
           kdb: true
         osd.1:
           branch: new_btrfs
           kdb: false
         osd.3:
           deb: /path/to/linux-whatever.deb

    is transformed into::

         osd.0:
           tag: v3.0
           kdb: true
         osd.1:
           branch: new_btrfs
           kdb: false
         osd.2:
           tag: v3.0
           kdb: true
         osd.3:
           deb: /path/to/linux-whatever.deb

    If config is None or just specifies a version to use,
    it is applied to all nodes.
    """
    if config is None or \
            len(filter(lambda x: x in ['tag', 'branch', 'sha1', 'kdb',
                                       'deb'],
                       config.keys())) == len(config.keys()):
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

def _find_arch_and_dist(ctx):
    """
    Return the arch and distro value as a tuple.
    """
    info = ctx.config.get('machine_type', 'plana')
    if teuthology.is_arm(info):
        return ('armv7l', 'quantal')
    return ('x86_64', 'precise')

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

def _vsplitter(version):
    """kernels from Calxeda are named ...ceph-<sha1>...highbank
    kernels that we generate named ...-g<sha1>
    This routine finds the text in front of the sha1 that is used by
    need_to_install() to extract information from the kernel name.
    """

    if version.endswith('highbank'):
        return 'ceph-'
    return '-g'

def need_to_install(ctx, role, sha1):
    ret = True
    log.info('Checking kernel version of {role}, want {sha1}...'.format(
            role=role,
            sha1=sha1))
    version_fp = StringIO()
    ctx.cluster.only(role).run(
        args=[
            'uname',
            '-r',
            ],
        stdout=version_fp,
        )
    version = version_fp.getvalue().rstrip('\n')
    splt = _vsplitter(version)
    if splt in version:
        _, current_sha1 = version.rsplit(splt, 1)
        dloc = current_sha1.find('-')
        if dloc > 0:
            current_sha1 = current_sha1[0:dloc]
        log.debug('current kernel version is: {version} sha1 {sha1}'.format(
                version=version,
                sha1=current_sha1))
        if sha1.startswith(current_sha1):
            log.debug('current sha1 is the same, do not need to install')
            ret = False
    else:
        log.debug('current kernel version is: {version}, unknown sha1'.format(
                version=version))
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

def download_deb(ctx, config):
    procs = {}
    for role, src in config.iteritems():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        if src.find('/') >= 0:
            # local deb
            log.info('Copying kernel deb {path} to {role}...'.format(path=src,
                                                                     role=role))
            f = open(src, 'r')
            proc = role_remote.run(
                args=[
                    'python', '-c',
                    'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
                    '/tmp/linux-image.deb',
                    ],
                wait=False,
                stdin=f
                )
            procs[role_remote.name] = proc

        else:
            log.info('Downloading kernel {sha1} on {role}...'.format(sha1=src,
                                                                     role=role))
            larch, ldist = _find_arch_and_dist(ctx)
            _, deb_url = teuthology.get_ceph_binary_url(
                package='kernel',
                sha1=src,
                format='deb',
                flavor='basic',
                arch=larch,
                dist=ldist,
                )

            log.info('fetching kernel from {url}'.format(url=deb_url))
            proc = role_remote.run(
                args=[
                    'sudo', 'rm', '-f', '/tmp/linux-image.deb',
                    run.Raw('&&'),
                    'echo',
                    'linux-image.deb',
                    run.Raw('|'),
                    'wget',
                    '-nv',
                    '-O',
                    '/tmp/linux-image.deb',
                    '--base={url}'.format(url=deb_url),
                    '--input-file=-',
                    ],
                wait=False)
            procs[role_remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug('Waiting for download/copy to %s to complete...', name)
        proc.exitstatus.get()


def _no_grub_link(in_file, remote, kernel_ver):
    boot1 = '/boot/%s' % in_file
    boot2 = '%s.old' % boot1 
    remote.run(
        args=[
            'if', 'test', '-e', boot1, run.Raw(';'), 'then',
            'sudo', 'mv', boot1, boot2, run.Raw(';'), 'fi',],
    ) 
    remote.run(
        args=['sudo', 'ln', '-s', '%s-%s' % (in_file, kernel_ver) , boot1, ],
    )

def install_and_reboot(ctx, config):
    procs = {}
    kernel_title = ''
    for role, src in config.iteritems():
        log.info('Installing kernel {src} on {role}...'.format(src=src,
                                                               role=role))
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        proc = role_remote.run(
            args=[
                # install the kernel deb
                'sudo',
                'dpkg',
                '-i',
                '/tmp/linux-image.deb',
                ],
            )

        # collect kernel image name from the .deb
        cmdout = StringIO()
        proc = role_remote.run(
            args=[
                # extract the actual boot image name from the deb
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
                'sed',
                r'-e s;.*\./boot/vmlinuz-;;',
            ],
            stdout = cmdout,
            )
        kernel_title = cmdout.getvalue().rstrip()
        cmdout.close()
        log.info('searching for kernel {}'.format(kernel_title))

        if kernel_title.endswith("-highbank"):
            _no_grub_link('vmlinuz', role_remote, kernel_title)
            _no_grub_link('initrd.img', role_remote, kernel_title)
            proc = role_remote.run(
                args=[
                    'sudo',
                    'shutdown',
                    '-r',
                    'now',
                    ],
                wait=False,
            )
            procs[role_remote.name] = proc
            continue

        # look for menuentry for our kernel, and collect any
        # submenu entries for their titles.  Assume that if our
        # kernel entry appears later in the file than a submenu entry,
        # it's actually nested under that submenu.  If it gets more
        # complex this will totally break.
        
        cmdout = StringIO()
        proc = role_remote.run(
            args=[
                'egrep',
                '(submenu|menuentry.*' + kernel_title + ').*{',
                '/boot/grub/grub.cfg'
               ],
            stdout = cmdout,
            )
        submenu_title = ''
        default_title = ''
        for l in cmdout.getvalue().split('\n'):
            fields = shlex.split(l)
            if len(fields) >= 2:
                command, title = fields[:2]
                if command == 'submenu':
                    submenu_title = title + '>'
                if command == 'menuentry':
                    if title.endswith(kernel_title):
                        default_title = title
                        break
        cmdout.close()
        log.info('submenu_title:{}'.format(submenu_title))
        log.info('default_title:{}'.format(default_title))

        proc = role_remote.run(
            args=[
                # use the title(s) to construct the content of
                # the grub menu entry, so we can default to it.
                '/bin/echo',
                '-e',
                r'cat <<EOF\nset default="' + submenu_title + \
                    default_title + r'"\nEOF\n',
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
                # update grub again so it accepts our default
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

def enable_disable_kdb(ctx, config):
    for role, enable in config.iteritems():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        if "mira" in role_remote.name:
            serialdev = "ttyS2"
        else:
            serialdev = "ttyS1"
        if enable:
            log.info('Enabling kdb on {role}...'.format(role=role))
            role_remote.run(
                args=[
                    'echo', serialdev,
                    run.Raw('|'),
                    'sudo', 'tee', '/sys/module/kgdboc/parameters/kgdboc'
                    ])
        else:
            log.info('Disabling kdb on {role}...'.format(role=role))
            role_remote.run(
                args=[
                    'echo', '',
                    run.Raw('|'),
                    'sudo', 'tee', '/sys/module/kgdboc/parameters/kgdboc'
                    ])

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

    To enable kdb::

        kernel:
          kdb: true

    """
    assert config is None or isinstance(config, dict), \
        "task kernel only supports a dictionary for configuration"

    timeout = 300
    if config is not None and 'timeout' in config:
        timeout = config.pop('timeout')

    config = normalize_config(ctx, config)
    validate_config(ctx, config)
    log.info('config %s' % config)

    need_install = {}  # sha1 to dl, or path to deb
    need_sha1 = {}     # sha1
    kdb = {}
    for role, role_config in config.iteritems():
        if role_config.get('deb'):
            path = role_config.get('deb')
            match = re.search('\d+-g(\w{7})', path)
            if match:
                sha1 = match.group(1)
                log.info('kernel deb sha1 appears to be %s', sha1)
                if need_to_install(ctx, role, sha1):
                    need_install[role] = path
                    need_sha1[role] = sha1
            else:
                log.info('unable to extract sha1 from deb path, forcing install')
                assert False
        else:
            nsha1 = ctx.config.get('overrides',{}).get('ceph',{}).get('sha1',role_config.get('sha1'))
            larch, ldist = _find_arch_and_dist(ctx)
            sha1, _ = teuthology.get_ceph_binary_url(
                package='kernel',
                branch=role_config.get('branch'),
                tag=role_config.get('tag'),
                sha1=nsha1,
                flavor='basic',
                format='deb',
                dist=ldist,
                arch=larch,
                )
            log.debug('sha1 for {role} is {sha1}'.format(role=role, sha1=sha1))
            ctx.summary['{role}-kernel-sha1'.format(role=role)] = sha1
            if need_to_install(ctx, role, sha1):
                need_install[role] = sha1
                need_sha1[role] = sha1

        # enable or disable kdb if specified, otherwise do not touch
        if role_config.get('kdb') is not None:
            kdb[role] = role_config.get('kdb')

    if need_install:
        install_firmware(ctx, need_install)
        download_deb(ctx, need_install)
        install_and_reboot(ctx, need_install)
        wait_for_reboot(ctx, need_sha1, timeout)

    enable_disable_kdb(ctx, kdb)
