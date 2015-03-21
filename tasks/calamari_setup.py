"""
Calamari setup task
"""
import contextlib
import logging
import os
import requests
import shutil
import subprocess
import webbrowser

from cStringIO import StringIO
from teuthology.orchestra import run
from teuthology import contextutil
from teuthology import misc

log = logging.getLogger(__name__)

ICE_VERSION_DEFAULT = '1.2.2'


@contextlib.contextmanager
def task(ctx, config):
    """
    Do the setup of a calamari server.

    - calamari_setup:
        version: 'v80.1'
        ice_tool_dir: <directory>
        iceball_location: <directory>

    Options are:

    version -- ceph version we are testing against (defaults to 80.1)
    ice_tool_dir -- optional local directory where ice-tool exists or will
                    be loaded (defaults to src in home directory)
    ice_version  -- version of ICE we're testing (with default)
    iceball_location -- Can be an HTTP URL, in which case fetch from this
                        location, using 'ice_version' and distro information
                        to select the right tarball.  Can also be a local
                        path.  If local path is '.', and iceball is
                        not already present, then we try to build
                        an iceball using the ice_tool_dir commands.
    ice_git_location -- location of ice tool on git
    start_browser -- If True, start a browser.  To be used by runs that will
                     bring up a browser quickly for human use.  Set to False
                     for overnight suites that are testing for problems in
                     the installation itself (defaults to False).
    email -- email address for the user (defaults to x@y.com)
    no_epel -- indicates if we should remove epel files prior to yum
               installations.  Defaults to True.
    calamari_user -- user name to log into gui (defaults to admin)
    calamari_password -- calamari user password (defaults to admin)
    """
    cal_svr = None
    start_browser = config.get('start_browser', False)
    no_epel = config.get('no_epel', True)
    for remote_, roles in ctx.cluster.remotes.items():
        if 'client.0' in roles:
            cal_svr = remote_
            break
    if not cal_svr:
        raise RuntimeError('client.0 not found in roles')
    with contextutil.nested(
        lambda: adjust_yum_repos(ctx, cal_svr, no_epel),
        lambda: calamari_install(config, cal_svr),
        lambda: ceph_install(ctx, cal_svr),
        lambda: calamari_connect(ctx, cal_svr),
        lambda: browser(start_browser, cal_svr.hostname),
    ):
        yield


@contextlib.contextmanager
def adjust_yum_repos(ctx, cal_svr, no_epel):
    """
    For each remote machine, fix the repos if yum is used.
    """
    ice_distro = str(cal_svr.os)
    if ice_distro.startswith('rhel') or ice_distro.startswith('centos'):
        if no_epel:
            for remote in ctx.cluster.remotes:
                fix_yum_repos(remote, ice_distro)
    try:
        yield
    finally:
        if ice_distro.startswith('rhel') or ice_distro.startswith('centos'):
            if no_epel:
                for remote in ctx.cluster.remotes:
                    restore_yum_repos(remote)


def restore_yum_repos(remote):
    """
    Copy the old saved repo back in.
    """
    if remote.run(args=['sudo', 'rm', '-rf', '/etc/yum.repos.d']).exitstatus:
        return False
    if remote.run(args=['sudo', 'mv', '/etc/yum.repos.d.old',
                        '/etc/yum.repos.d']).exitstatus:
        return False


def fix_yum_repos(remote, distro):
    """
    For yum calamari installations, the repos.d directory should only
    contain a repo file named rhel<version-number>.repo
    """
    if distro.startswith('centos'):
        cmds = [
            'sudo mkdir /etc/yum.repos.d.old'.split(),
            ['sudo', 'cp', run.Raw('/etc/yum.repos.d/*'),
             '/etc/yum.repos.d.old'],
            ['sudo', 'rm', run.Raw('/etc/yum.repos.d/epel*')],
        ]
        for cmd in cmds:
            if remote.run(args=cmd).exitstatus:
                return False
    else:
        cmds = [
            'sudo mv /etc/yum.repos.d /etc/yum.repos.d.old'.split(),
            'sudo mkdir /etc/yum.repos.d'.split(),
        ]
        for cmd in cmds:
            if remote.run(args=cmd).exitstatus:
                return False

        # map "distroversion" from Remote.os to a tuple of
        # (repo title, repo name descriptor, apt-mirror repo path chunk)
        yum_repo_params = {
            'rhel 6.4': ('rhel6-server', 'RHEL', 'rhel6repo-server'),
            'rhel 6.5': ('rhel6-server', 'RHEL', 'rhel6repo-server'),
            'rhel 7.0': ('rhel7-server', 'RHEL', 'rhel7repo/server'),
        }
        repotitle, reponame, path = yum_repo_params[distro]
        repopath = '/etc/yum.repos.d/%s.repo' % repotitle
        # TO DO:  Make this data configurable too
        repo_contents = '\n'.join(
            ('[%s]' % repotitle,
             'name=%s $releasever - $basearch' % reponame,
             'baseurl=http://apt-mirror.front.sepia.ceph.com/' + path,
             'gpgcheck=0',
             'enabled=1')
        )
        misc.sudo_write_file(remote, repopath, repo_contents)
    cmds = [
        'sudo yum clean all'.split(),
        'sudo yum makecache'.split(),
    ]
    for cmd in cmds:
        if remote.run(args=cmd).exitstatus:
            return False
    return True


def get_iceball_with_http(urlbase, ice_version, ice_distro, destdir):
    '''
    Copy iceball with http to destdir.  Try both .tar.gz and .iso.
    '''
    urlprefix = os.path.join(urlbase, '{ver}/ICE-{ver}-{distro}'.format(
                             ver=ice_version, distro=ice_distro))
    urls = [urlprefix + ext for ext in ('.tar.gz', '.iso')]

    # stream=True means we don't download until copyfileobj below,
    # and don't need a temp file
    for url in urls:
        r = requests.get(url, stream=True)
        if not r.ok:
            continue
        filename = os.path.join(destdir, url.split('/')[-1])
        with open(filename, 'w') as f:
            shutil.copyfileobj(r.raw, f)
        log.info('saved %s as %s' % (url, filename))
        return filename
    raise RuntimeError("Failed to download %s", str(urls))


def create_iceball(ice_tool_dir, git_icetool_loc, ice_version, version, ice_distro):
    ice_tool_loc = os.path.join(ice_tool_dir, 'ice-tools')
    if not os.path.isdir(ice_tool_loc):
        try:
            subprocess.check_call(['git', 'clone',
                                   git_icetool_loc + os.sep +
                                   'ice-tools.git',
                                   ice_tool_loc])
        except subprocess.CalledProcessError:
            raise RuntimeError('git clone of ice-tools failed')
    exec_ice = os.path.join(ice_tool_loc,
                            'teuth-virtenv/bin/make_iceball')
    try:
        subprocess.check_call('virtualenv teuth-virtenv'.split(),
                              cwd=ice_tool_loc)
        subprocess.check_call(
            'teuth-virtenv/bin/python setup.py develop'.split(),
            cwd=ice_tool_loc
        )
        subprocess.check_call(
            'teuth-virtenv/bin/pip install -r requirements.txt'.split(),
            cwd=ice_tool_loc
        )
        subprocess.check_call([exec_ice, '-I', ice_version,
                               '-b', version, '-o', ice_distro])
    except subprocess.CalledProcessError:
        raise RuntimeError('%s failed for %s distro' %
                           (exec_ice, ice_distro))
    subprocess.check_call('rm -rf teuth-virtenv'.split(),
                          cwd=ice_tool_loc)
    return os.path.join(
        ice_tool_loc, 'ICE-{0}-{1}.tar.gz'.format(ice_version, ice_distro)
    )


@contextlib.contextmanager
def calamari_install(config, cal_svr):
    """
    Install calamari

    The steps here are:
        -- Get the iceball, building it if necessary.
        -- Copy the iceball to the calamari server, and untar/mount it.
        -- Run ice-setup on the calamari server.
        -- Run calamari-ctl initialize.
    """
    def translate_os_to_ice_distro(osname):
        convert = {'ubuntu12.04': 'precise', 'ubuntu14.04': 'trusty',
                   'rhel7.0': 'rhel7', 'debian7': 'wheezy'}

        ice_distro = osname.replace(' ', '')

        if ice_distro in convert:
            ice_distro = convert[ice_distro]

        return ice_distro

    ice_distro = translate_os_to_ice_distro(str(cal_svr.os))

    client_id = str(cal_svr)
    at_loc = client_id.find('@')
    if at_loc > 0:
        client_id = client_id[at_loc + 1:]

    version = config.get('version', 'v0.80.1')
    email = config.get('email', 'x@x.com')
    ice_tool_dir = config.get('ice_tool_dir', '%s%s%s' %
                              (os.environ['HOME'], os.sep, 'src'))
    calamari_user = config.get('calamari_user', 'admin')
    calamari_password = config.get('calamari_password', 'admin')
    git_icetool_loc = config.get('ice_git_location',
                                 'git@github.com:inktankstorage')
    iceball_loc = config.get('iceball_location', '.')
    ice_version = config.get('ice_version', ICE_VERSION_DEFAULT)

    log.info('calamari server distro: %s' % ice_distro)
    delete_iceball = False

    if iceball_loc.startswith('http'):
        iceball_file = get_iceball_with_http(
            iceball_loc, ice_version, ice_distro, '/tmp'
        )
        delete_iceball = True
    elif iceball_loc == '.':  # TODO this is a bad sentinel
        iceball_file = create_iceball(
            ice_tool_dir, git_icetool_loc, ice_version, version, ice_distro
        )
        shutil.move(iceball_file, '/tmp')
        iceball_file = os.path.join('/tmp', iceball_file)
        delete_iceball = True
    else:
        prefix = os.path.join(iceball_loc,
                              'ICE-{0}-{1}'.format(ice_version, ice_distro))
        for name in [prefix + ext for ext in ('.iso', '.tar.gz')]:
            if os.path.exists(name):
                iceball_file = name
                break
        else:
            raise RuntimeError(
                'Can\'t find {0} at {1} in iso or tar.gz format'.format(prefix, iceball_loc)
            )

    remote_iceball_file = os.path.join('/tmp', os.path.split(iceball_file)[1])
    cal_svr.put_file(iceball_file, remote_iceball_file)
    if iceball_file.endswith('.tar.gz'):   # XXX specify tar/iso in config?
        icetype = 'tarball'
    elif iceball_file.endswith('.iso'):
        icetype = 'iso'
    else:
        raise RuntimeError('Can''t handle iceball {0}'.format(iceball_file))

    if icetype == 'tarball':
        ret = cal_svr.run(args=['gunzip', run.Raw('<'), remote_iceball_file,
                          run.Raw('|'), 'tar', 'xvf', run.Raw('-')])
        if ret.exitstatus:
            raise RuntimeError('remote iceball untar failed')
    elif icetype == 'iso':
        mountpoint = '/mnt/'   # XXX create?
        ret = cal_svr.run(
            args=['sudo', 'mount', '-r', remote_iceball_file, mountpoint]
        )

    # install ice_setup package
    args = {
        'deb': 'sudo dpkg -i /mnt/ice-setup*deb',
        'rpm': 'sudo yum -y localinstall /mnt/ice_setup*rpm'
    }.get(cal_svr.system_type, None)
    if not args:
        raise RuntimeError('{0}: unknown system type'.format(cal_svr))
    ret = cal_svr.run(args=args)
    if ret.exitstatus:
        raise RuntimeError('ice_setup package install failed')

    # Run ice_setup
    icesetdata = 'yes\n\n%s\nhttp\n' % client_id
    ice_in = StringIO(icesetdata)
    ice_out = StringIO()
    args = 'sudo ice_setup'
    if icetype == 'iso':
        args += ' -d /mnt'
    ret = cal_svr.run(args=args, stdin=ice_in, stdout=ice_out)
    log.debug(ice_out.getvalue())
    if ret.exitstatus:
        raise RuntimeError('ice_setup.py failed')

    # Run calamari-ctl initialize.
    icesetdata = '%s\n%s\n%s\n%s\n' % (calamari_user, email, calamari_password,
                                       calamari_password)
    ice_in = StringIO(icesetdata)
    ret = cal_svr.run(args=['sudo', 'calamari-ctl', 'initialize'],
                      stdin=ice_in, stdout=ice_out)
    log.debug(ice_out.getvalue())
    if ret.exitstatus:
        raise RuntimeError('calamari-ctl initialize failed')
    try:
        yield
    finally:
        log.info('Cleaning up after Calamari installation')
        if delete_iceball:
            os.unlink(iceball_file)


@contextlib.contextmanager
def ceph_install(ctx, cal_svr):
    """
    Install ceph if ceph was not previously installed by teuthology.  This
    code tests the case where calamari is installed on a brand new system.
    """
    loc_inst = False
    if 'install' not in [x.keys()[0] for x in ctx.config['tasks']]:
        loc_inst = True
        ret = deploy_ceph(ctx, cal_svr)
        if ret:
            raise RuntimeError('ceph installs failed')
    try:
        yield
    finally:
        if loc_inst:
            if not undeploy_ceph(ctx, cal_svr):
                log.error('Cleanup of Ceph installed by Calamari-setup failed')


def deploy_ceph(ctx, cal_svr):
    """
    Perform the ceph-deploy actions needed to bring up a Ceph cluster.  This
    test is needed to check the ceph-deploy that comes with the calamari
    package.
    """
    osd_to_name = {}
    all_machines = set()
    all_mons = set()
    for remote in ctx.cluster.remotes:
        all_machines.add(remote.shortname)
        roles = ctx.cluster.remotes[remote]
        for role in roles:
            daemon_type, number = role.split('.')
            if daemon_type == 'osd':
                osd_to_name[number] = remote.shortname
            if daemon_type == 'mon':
                all_mons.add(remote.shortname)
    first_cmds = [['new'] + list(all_mons), ['install'] + list(all_machines),
                  ['mon', 'create-initial']]
    ret = True
    for entry in first_cmds:
        arg_list = ['ceph-deploy'] + entry
        log.info('Running: %s' % ' '.join(arg_list))
        ret &= cal_svr.run(args=arg_list).exitstatus
    disk_labels = '_dcba'
    # NEEDS WORK assumes disks start with vd (need to check this somewhere)
    for cmd_pts in [['disk', 'zap'], ['osd', 'prepare'], ['osd', 'activate']]:
        mach_osd_cnt = {}
        for osdn in osd_to_name:
            osd_mac = osd_to_name[osdn]
            mach_osd_cnt[osd_mac] = mach_osd_cnt.get(osd_mac, 0) + 1
            arg_list = ['ceph-deploy']
            arg_list.extend(cmd_pts)
            disk_id = '%s:vd%s' % (osd_to_name[osdn],
                                   disk_labels[mach_osd_cnt[osd_mac]])
            if 'activate' in cmd_pts:
                disk_id += '1'
            arg_list.append(disk_id)
            log.info('Running: %s' % ' '.join(arg_list))
            ret &= cal_svr.run(args=arg_list).exitstatus
    return ret


def undeploy_ceph(ctx, cal_svr):
    """
    Cleanup deployment of ceph.
    """
    all_machines = []
    ret = True
    for remote in ctx.cluster.remotes:
        ret &= remote.run(args=['sudo', 'stop', 'ceph-all', run.Raw('||'),
                                'sudo', 'service', 'ceph', 'stop']
                          ).exitstatus
        all_machines.append(remote.shortname)
    all_machines = set(all_machines)
    cmd1 = ['ceph-deploy', 'uninstall']
    cmd1.extend(all_machines)
    ret &= cal_svr.run(args=cmd1).exitstatus
    cmd2 = ['ceph-deploy', 'purge']
    cmd2.extend(all_machines)
    ret &= cal_svr.run(args=cmd2).exitstatus
    for remote in ctx.cluster.remotes:
        ret &= remote.run(args=['sudo', 'rm', '-rf',
                                '.ssh/known_hosts']).exitstatus
    return ret


@contextlib.contextmanager
def calamari_connect(ctx, cal_svr):
    """
    Connect calamari to the ceph nodes.
    """
    connects = ['ceph-deploy', 'calamari', 'connect']
    for machine_info in ctx.cluster.remotes:
        if 'client.0' not in ctx.cluster.remotes[machine_info]:
            connects.append(machine_info.shortname)
    ret = cal_svr.run(args=connects)
    if ret.exitstatus:
        raise RuntimeError('calamari connect failed')
    try:
        yield
    finally:
        log.info('Calamari test terminating')


@contextlib.contextmanager
def browser(start_browser, web_page):
    """
    Bring up a browser, if wanted.
    """
    if start_browser:
        webbrowser.open('http://%s' % web_page)
    try:
        yield
    finally:
        if start_browser:
            log.info('Web browser support terminating')
