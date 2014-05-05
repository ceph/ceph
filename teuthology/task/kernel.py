"""
Kernel installation task
"""
from cStringIO import StringIO

import logging
import re
import shlex
import urllib2
import urlparse

from teuthology import misc as teuthology
from ..orchestra import run
from ..config import config as teuth_config

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

    :param ctx: Context
    :param config: Configuration
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

    Currently this only returns armv7l on the quantal distro or x86_64
    on the precise distro
  
    :param ctx: Context
    :returns: arch,distro
    """
    info = ctx.config.get('machine_type', 'plana')
    if teuthology.is_arm(info):
        return ('armv7l', 'quantal')
    return ('x86_64', 'precise')

def validate_config(ctx, config):
    """
    Make sure that all kernels in the list of remove kernels
    refer to the same kernel.

    :param ctx: Context
    :param config: Configuration
    """
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
    """Kernels from Calxeda are named ...ceph-<sha1>...highbank.
    Kernels that we generate are named ...-g<sha1>.
    This routine finds the text in front of the sha1 that is used by
    need_to_install() to extract information from the kernel name.

    :param version: Name of the kernel
    """
    if version.endswith('highbank'):
        return 'ceph-'
    return '-g'

def need_to_install(ctx, role, version):
    """
    Check to see if we need to install a kernel.  Get the version of the
    currently running kernel, and compare it against the value passed in.

    :param ctx: Context
    :param role: Role
    :param version: value to compare against (used in checking), can be either
                    a utsrelease string (e.g. '3.13.0-rc3-ceph-00049-ge2817b3')
                    or a sha1.
    """
    ret = True
    log.info('Checking kernel version of {role}, want {ver}...'.format(
             role=role, ver=version))
    uname_fp = StringIO()
    ctx.cluster.only(role).run(
        args=[
            'uname',
            '-r',
            ],
        stdout=uname_fp,
        )
    cur_version = uname_fp.getvalue().rstrip('\n')
    log.debug('current kernel version is {ver}'.format(ver=cur_version))

    if '.' in version:
        # version is utsrelease, yay
        if cur_version == version:
            log.debug('utsrelease strings match, do not need to install')
            ret = False
    else:
        # version is sha1, need to try to extract sha1 from cur_version
        splt = _vsplitter(cur_version)
        if splt in cur_version:
            _, cur_sha1 = cur_version.rsplit(splt, 1)
            dloc = cur_sha1.find('-')
            if dloc > 0:
                cur_sha1 = cur_sha1[0:dloc]
            log.debug('extracting sha1, {ver} -> {sha1}'.format(
                      ver=cur_version, sha1=cur_sha1))
            if version.startswith(cur_sha1):
                log.debug('extracted sha1 matches, do not need to install')
                ret = False
        else:
            log.debug('failed to parse current kernel version')
    uname_fp.close()
    return ret

def install_firmware(ctx, config):
    """
    Go to the github to get the latest firmware.

    :param ctx: Context
    :param config: Configuration
    """
    linux_firmware_git_upstream = 'git://git.kernel.org/pub/scm/linux/kernel/git/firmware/linux-firmware.git'
    uri = teuth_config.linux_firmware_git_url or linux_firmware_git_upstream
    fw_dir = '/lib/firmware/updates'

    for role in config.iterkeys():
        if config[role].find('distro') >= 0:
            log.info('Skipping firmware on distro kernel');
            return
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
    """
    Download a Debian kernel and copy the assocated linux image.

    :param ctx: Context
    :param config: Configuration
    """
    procs = {}
    #Don't need to download distro kernels
    for role, src in config.iteritems():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
	if src.find('distro') >= 0:
            log.info('Installing newest kernel distro');
            return

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
    """
    Copy and link kernel related files if grub cannot be used
    (as is the case in Arm kernels)

    :param infile: kernel file or image file to be copied.
    :param remote: remote machine 
    :param kernel_ver: kernel version
    """
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
    """
    Install and reboot the kernel.  This mostly performs remote
    installation operations.   The code does check for Arm images
    and skips grub operations if the kernel is Arm.  Otherwise, it
    extracts kernel titles from submenu entries and makes the appropriate
    grub calls.   The assumptions here are somewhat simplified in that
    it expects kernel entries to be present under submenu entries.

    :param ctx: Context
    :param config: Configuration
    """
    procs = {}
    kernel_title = ''
    for role, src in config.iteritems():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        if src.find('distro') >= 0:
            log.info('Installing distro kernel on {role}...'.format(role=role))
            install_distro_kernel(role_remote)
            continue

        log.info('Installing kernel {src} on {role}...'.format(src=src,
                                                               role=role))
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
    """
    Enable kdb on remote machines in use.  Disable on those that are
    not in use.

    :param ctx: Context
    :param config: Configuration
    """
    for role, enable in config.iteritems():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        if "mira" in role_remote.name:
            serialdev = "ttyS2"
        else:
            serialdev = "ttyS1"
        if enable:
            log.info('Enabling kdb on {role}...'.format(role=role))
            try:
                role_remote.run(
                    args=[
                        'echo', serialdev,
                        run.Raw('|'),
                        'sudo', 'tee', '/sys/module/kgdboc/parameters/kgdboc'
                        ])
            except run.CommandFailedError:
                log.warn('Kernel does not support kdb')
        else:
            log.info('Disabling kdb on {role}...'.format(role=role))
            # Add true pipe so command doesn't fail on kernel without kdb support.
            try:
                role_remote.run(
                    args=[
                        'echo', '',
                        run.Raw('|'),
                        'sudo', 'tee', '/sys/module/kgdboc/parameters/kgdboc',
                        run.Raw('|'),
                        'true',
                        ])
            except run.CommandFailedError:
                log.warn('Kernel does not support kdb')

def wait_for_reboot(ctx, need_install, timeout, distro=False):
    """
    Loop reconnecting and checking kernel versions until
    they're all correct or the timeout is exceeded.

    :param ctx: Context
    :param need_install: list of packages that we need to reinstall.
    :param timeout: number of second before we timeout.
    """
    import time
    starttime = time.time()
    while need_install:
        teuthology.reconnect(ctx, timeout)
        for client in need_install.keys():
            if 'distro' in need_install[client]:
                 distro = True
            log.info('Checking client {client} for new kernel version...'.format(client=client))
            try:
                if distro:
                    assert not need_to_install_distro(ctx, client), \
                            'failed to install new distro kernel version within timeout'

                else:
                    assert not need_to_install(ctx, client, need_install[client]), \
                            'failed to install new kernel version within timeout'
                del need_install[client]
            except Exception:
                log.exception("Saw exception")
                # ignore connection resets and asserts while time is left
                if time.time() - starttime > timeout:
                    raise
        time.sleep(1)


def need_to_install_distro(ctx, role):
    """
    Installing kernels on rpm won't setup grub/boot into them.
    This installs the newest kernel package and checks its version
    and compares against current (uname -r) and returns true if newest != current.
    Similar check for deb.
    """
    (role_remote,) = ctx.cluster.only(role).remotes.keys()
    system_type = teuthology.get_system_type(role_remote)
    output, err_mess = StringIO(), StringIO()
    role_remote.run(args=['uname', '-r' ], stdout=output, stderr=err_mess )
    current = output.getvalue().strip()
    if system_type == 'rpm':
        role_remote.run(args=['sudo', 'yum', 'install', '-y', 'kernel' ], stdout=output, stderr=err_mess )
        #reset stringIO output.
        output, err_mess = StringIO(), StringIO()
        role_remote.run(args=['rpm', '-q', 'kernel', '--last' ], stdout=output, stderr=err_mess )
        newest=output.getvalue().split()[0]

    if system_type == 'deb':
        distribution = teuthology.get_system_type(role_remote, distro=True)
        newest = get_version_from_pkg(role_remote, distribution)

    output.close()
    err_mess.close()
    if current in newest:
        return False
    log.info('Not newest distro kernel. Curent: {cur} Expected: {new}'.format(cur=current, new=newest))
    return True

def install_distro_kernel(remote):
    """
    RPM: Find newest kernel on the machine and update grub to use kernel + reboot.
    DEB: Find newest kernel. Parse grub.cfg to figure out the entryname/subentry.
    then modify 01_ceph_kernel to have correct entry + updategrub + reboot.
    """
    system_type = teuthology.get_system_type(remote)
    distribution = ''
    if system_type == 'rpm':
        output, err_mess = StringIO(), StringIO()
        remote.run(args=['rpm', '-q', 'kernel', '--last' ], stdout=output, stderr=err_mess )
        newest=output.getvalue().split()[0].split('kernel-')[1]
        log.info('Distro Kernel Version: {version}'.format(version=newest))
        update_grub_rpm(remote, newest)
        remote.run( args=['sudo', 'shutdown', '-r', 'now'], wait=False )
        output.close()
        err_mess.close()
        return

    if system_type == 'deb':
        distribution = teuthology.get_system_type(remote, distro=True)
        newversion = get_version_from_pkg(remote, distribution)
        if 'ubuntu' in distribution:
            grub2conf = teuthology.get_file(remote, '/boot/grub/grub.cfg', True)
            submenu = ''
            menuentry = ''
            for line in grub2conf.split('\n'):
                if 'submenu' in line:
                    submenu = line.split('submenu ')[1]
                    # Ubuntu likes to be sneaky and change formatting of
                    # grub.cfg between quotes/doublequotes between versions
                    if submenu.startswith("'"):
                        submenu = submenu.split("'")[1]
                    if submenu.startswith('"'):
                        submenu = submenu.split('"')[1]
                if 'menuentry' in line:
                    if newversion in line and 'recovery' not in line:
                        menuentry = line.split('\'')[1]
                        break
            if submenu:
                grubvalue = submenu + '>' + menuentry
            else:
                grubvalue = menuentry
            grubfile = 'cat <<EOF\nset default="' + grubvalue + '"\nEOF'
            teuthology.delete_file(remote, '/etc/grub.d/01_ceph_kernel', sudo=True, force=True)
            teuthology.sudo_write_file(remote, '/etc/grub.d/01_ceph_kernel', StringIO(grubfile), '755')
            log.info('Distro Kernel Version: {version}'.format(version=newversion))
            remote.run(args=['sudo', 'update-grub'])
            remote.run(args=['sudo', 'shutdown', '-r', 'now'], wait=False )
            return

        if 'debian' in distribution:
            grub2_kernel_select_generic(remote, newversion, 'deb')
            log.info('Distro Kernel Version: {version}'.format(version=newversion))
            remote.run( args=['sudo', 'shutdown', '-r', 'now'], wait=False )
            return

def update_grub_rpm(remote, newversion):
    """
    Updates grub file to boot new kernel version on both legacy grub/grub2.
    """
    grub='grub2'
    # Check if grub2 is isntalled
    try:
        remote.run(args=['sudo', 'rpm', '-qi', 'grub2'])
    except Exception:
        grub = 'legacy'
    log.info('Updating Grub Version: {grub}'.format(grub=grub))
    if grub == 'legacy':
        data = ''
        #Write new legacy grub entry.
        newgrub = generate_legacy_grub_entry(remote, newversion)
        for line in newgrub:
            data += line + '\n'
        temp_file_path = teuthology.remote_mktemp(remote)
        teuthology.sudo_write_file(remote, temp_file_path, StringIO(data), '755')
        teuthology.move_file(remote, temp_file_path, '/boot/grub/grub.conf', True)
    else:
        #Update grub menu entry to new version.
        grub2_kernel_select_generic(remote, newversion, 'rpm')

def grub2_kernel_select_generic(remote, newversion, ostype):
    """
    Can be used on DEB and RPM. Sets which entry should be boted by entrynum.
    """
    if ostype == 'rpm':
        grubset = 'grub2-set-default'
        mkconfig = 'grub2-mkconfig'
        grubconfig = '/boot/grub2/grub.cfg'
    if ostype == 'deb':
        grubset = 'grub-set-default'
        grubconfig = '/boot/grub/grub.cfg'
        mkconfig = 'grub-mkconfig'
    remote.run(args=['sudo', mkconfig, '-o', grubconfig, ])
    grub2conf = teuthology.get_file(remote, grubconfig, True)
    entry_num = 0
    for line in grub2conf.split('\n'):
        if line.startswith('menuentry'):
            if newversion in line:
                break
            entry_num =+ 1
    remote.run(args=['sudo', grubset, str(entry_num), ])

def generate_legacy_grub_entry(remote, newversion):
    """
    This will likely need to be used for ceph kernels as well
    as legacy grub rpm distros don't have an easy way of selecting
    a kernel just via a command. This generates an entry in legacy
    grub for a new kernel version using the existing entry as a base.
    """
    grubconf = teuthology.get_file(remote, '/boot/grub/grub.conf', True)
    titleline = ''
    rootline = ''
    kernelline = ''
    initline = ''
    kernelversion = ''
    linenum = 0
    titlelinenum = 0

    #Grab first kernel entry (title/root/kernel/init lines)
    for line in grubconf.split('\n'):
        if re.match('^title', line):
            titleline = line
            titlelinenum = linenum
        if re.match('(^\s+)root', line):
            rootline = line
        if re.match('(^\s+)kernel', line):
            kernelline = line
            for word in line.split(' '):
                if 'vmlinuz' in word:
                    kernelversion = word.split('vmlinuz-')[-1]
        if re.match('(^\s+)initrd', line):
            initline = line
        if (kernelline != '') and (initline != ''):
            break
        else:
            linenum += 1

    #insert new entry into grubconfnew list:
    linenum = 0
    newgrubconf = []
    for line in grubconf.split('\n'):
        line = line.rstrip('\n')
        if linenum == titlelinenum:
            newtitle = re.sub(kernelversion, newversion, titleline)
            newroot = re.sub(kernelversion, newversion, rootline)
            newkernel = re.sub(kernelversion, newversion, kernelline)
            newinit = re.sub(kernelversion, newversion, initline)
            newgrubconf.append(newtitle)
            newgrubconf.append(newroot)
            newgrubconf.append(newkernel)
            newgrubconf.append(newinit)
            newgrubconf.append('')
            newgrubconf.append(line)
        else:
            newgrubconf.append(line)
        linenum += 1
    return newgrubconf

def get_version_from_pkg(remote, ostype):
    """
    Round-about way to get the newest kernel uname -r compliant version string
    from the virtual package which is the newest kenel for debian/ubuntu.
    """
    output, err_mess = StringIO(), StringIO()
    newest=''
    #Depend of virtual package has uname -r output in package name. Grab that.
    if 'debian' in ostype:
        remote.run(args=['sudo', 'apt-get', '-y', 'install', 'linux-image-amd64' ], stdout=output, stderr=err_mess )
        remote.run(args=['dpkg', '-s', 'linux-image-amd64' ], stdout=output, stderr=err_mess )
        for line in output.getvalue().split('\n'):
            if 'Depends:' in line:
                newest = line.split('linux-image-')[1]
                output.close()
                err_mess.close()
                return newest
     #Ubuntu is a depend in a depend.
    if 'ubuntu' in ostype:
        try:
            remote.run(args=['sudo', 'apt-get', '-y', 'install', 'linux-image-current-generic' ], stdout=output, stderr=err_mess )
            remote.run(args=['dpkg', '-s', 'linux-image-current-generic' ], stdout=output, stderr=err_mess )
            for line in output.getvalue().split('\n'):
                if 'Depends:' in line:
                    depends = line.split('Depends: ')[1]
            remote.run(args=['dpkg', '-s', depends ], stdout=output, stderr=err_mess )
        except run.CommandFailedError:
            # Non precise ubuntu machines (like trusty) don't have
            # linux-image-current-generic so use linux-image-generic instead.
            remote.run(args=['sudo', 'apt-get', '-y', 'install', 'linux-image-generic' ], stdout=output, stderr=err_mess )
            remote.run(args=['dpkg', '-s', 'linux-image-generic' ], stdout=output, stderr=err_mess )
        for line in output.getvalue().split('\n'):
            if 'Depends:' in line:
                newest = line.split('linux-image-')[1]
                if ',' in newest:
                    newest = newest.split(',')[0]
    output.close()
    err_mess.close()
    return newest

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

    :param ctx: Context
    :param config: Configuration
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
    need_version = {}  # utsrelease or sha1
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
                    need_version[role] = sha1
            else:
                log.info('unable to extract sha1 from deb path, forcing install')
                assert False
        elif role_config.get('sha1') == 'distro':
            if need_to_install_distro(ctx, role):
                need_install[role] = 'distro'
                need_version[role] = 'distro'
        else:
            larch, ldist = _find_arch_and_dist(ctx)
            sha1, base_url = teuthology.get_ceph_binary_url(
                package='kernel',
                branch=role_config.get('branch'),
                tag=role_config.get('tag'),
                sha1=role_config.get('sha1'),
                flavor='basic',
                format='deb',
                dist=ldist,
                arch=larch,
                )
            log.debug('sha1 for {role} is {sha1}'.format(role=role, sha1=sha1))
            ctx.summary['{role}-kernel-sha1'.format(role=role)] = sha1

            if need_to_install(ctx, role, sha1):
                version = sha1
                version_url = urlparse.urljoin(base_url, 'version')
                try:
                    version_fp = urllib2.urlopen(version_url)
                    version = version_fp.read().rstrip('\n')
                    version_fp.close()
                except urllib2.HTTPError:
                    log.debug('failed to get utsrelease string using url {url}'.format(
                              url=version_url))

                need_install[role] = sha1
                need_version[role] = version

        # enable or disable kdb if specified, otherwise do not touch
        if role_config.get('kdb') is not None:
            kdb[role] = role_config.get('kdb')

    if need_install:
        install_firmware(ctx, need_install)
        download_deb(ctx, need_install)
        install_and_reboot(ctx, need_install)
        wait_for_reboot(ctx, need_version, timeout)

    enable_disable_kdb(ctx, kdb)
