"""
Calamari setup task
"""
import contextlib
import re
import os
import subprocess
import logging
import webbrowser
from cStringIO import StringIO
from teuthology.orchestra import run
from teuthology import contextutil
from teuthology import misc

log = logging.getLogger(__name__)


def fix_yum_repos(remote, distro):
    """
    For yum calamari installations, the repos.d directory should only
    contain a repo file named rhel<version-number>.repo
    """
    distroname, distroversion = distro.split()
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

        '''
        map "distroversion" from Remote.os to a tuple of
        (repo title, repo name descriptor, apt-mirror repo path chunk)
        '''
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


def restore_yum_repos(remote):
    """
    Copy the old saved repo back in.
    """
    if remote.run(args=['sudo', 'rm', '-rf', '/etc/yum.repos.d']).exitstatus:
        return False
    if remote.run(args=['sudo', 'mv', '/etc/yum.repos.d.old',
                        '/etc/yum.repos.d']).exitstatus:
        return False


def deploy_ceph(ctx, cal_svr):
    """
    Perform the ceph-deploy actions needed to bring up a Ceph cluster.  This
    test is needed to check the ceph-deploy that comes with the calamari
    package.
    """
    osd_to_name = {}
    all_machines = set()
    mon0 = ''
    for remote in ctx.cluster.remotes:
        all_machines.add(remote.shortname)
        roles = ctx.cluster.remotes[remote]
        if 'mon.0' in roles:
            mon0 = remote.shortname
        for role in roles:
            daemon_type, number = role.split('.')
            if daemon_type == 'osd':
                osd_to_name[number] = remote.shortname
    first_cmds = [['new', mon0], ['install'] + list(all_machines),
                  ['mon', 'create-initial', mon0],
                  ['gatherkeys', mon0]]
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
def task(ctx, config):
    """
    Do the setup of a calamari server.

    - calamari_setup:
        version: 'v80.1'
        ice-tool-dir: <directory>
        iceball-location: <directory>

    Options are:

    version -- ceph version we are testing against (defaults to 80.1)
    ice-tool-dir -- local directory where ice-tool either exists or will
                    be loaded (defaults to src in home directory)
    iceball-location -- location of preconfigured iceball (defaults to .)
    start-browser -- If True, start a browser.  To be used by runs that will
                     bring up a browser quickly for human use.  Set to False
                     for overnight suites that are testing for problems in
                     the installation itself (defaults to True).
    email -- email address for the user (defaults to x@y.com)
    calamari_user -- user name to log into gui (defaults to admin)
    calamari_password -- calamari user password (defaults to admin)

    If iceball-location is '.', then we try to build a gz file using the
    ice-tool-dir commands.
    """
    cal_svr = None
    start_browser = config.get('start-browser', True)
    for remote_, roles in ctx.cluster.remotes.items():
        if 'client.0' in roles:
            cal_svr = remote_
            break
    if not cal_svr:
        raise RuntimeError('client.0 not found in roles')
    with contextutil.nested(
        lambda: adjust_yum_repos(ctx, cal_svr),
        lambda: calamari_install(config, cal_svr),
        lambda: ceph_install(ctx, cal_svr),
        lambda: calamari_connect(ctx, cal_svr),
        lambda: browser(start_browser, cal_svr.hostname),
    ):
        yield


@contextlib.contextmanager
def adjust_yum_repos(ctx, cal_svr):
    """
    For each remote machine, fix the repos if yum is used.
    """
    ice_distro = str(cal_svr.os)
    if ice_distro.startswith('rhel') or ice_distro.startswith('centos'):
        for remote in ctx.cluster.remotes:
            fix_yum_repos(remote, ice_distro)
    try:
        yield
    finally:
        if ice_distro.startswith('rhel') or ice_distro.startswith('centos'):
            for remote in ctx.cluster.remotes:
                restore_yum_repos(remote)


@contextlib.contextmanager
def calamari_install(config, cal_svr):
    """
    Install calamari

    The steps here are:
        -- Get the iceball, building it if necessary.
        -- Copy the iceball to the calamari server, and untarring it.
        -- Running ice-setup.py on the calamari server.
        -- Running calamari-ctl initialize.
    """
    ice_distro = str(cal_svr.os)
    ice_distro = ice_distro.replace(" ", "")
    client_id = str(cal_svr)
    at_loc = client_id.find('@')
    if at_loc > 0:
        client_id = client_id[at_loc + 1:]
    convert = {'ubuntu12.04': 'precise', 'ubuntu14.04': 'trusty',
               'rhel7.0': 'rhel7', 'debian7': 'wheezy'}
    version = config.get('version', 'v0.80.1')
    ice_tool_dir = config.get('ice-tool-dir', '%s%s%s' %
                              (os.environ['HOME'], os.sep, 'src'))
    email = config.get('email', 'x@x.com')
    ice_tool_dir = config.get('ice-tool-dir', '%s%s%s' %
                              (os.environ['HOME'], os.sep, 'src'))
    calamari_user = config.get('calamari-user', 'admin')
    calamari_password = config.get('calamari-passwd', 'admin')
    if ice_distro in convert:
        ice_distro = convert[ice_distro]
    log.info('calamari server on %s' % ice_distro)
    iceball_loc = config.get('iceball-location', '.')
    if iceball_loc == '.':
        ice_tool_loc = os.path.join(ice_tool_dir, 'ice-tools')
        if not os.path.isdir(ice_tool_loc):
            try:
                subprocess.check_call(['git', 'clone',
                                       'git@github.com:inktankstorage/' +
                                       'ice-tools.git',
                                       ice_tool_loc])
            except subprocess.CalledProcessError:
                raise RuntimeError('client.0 not found in roles')
        exec_ice = os.path.join(ice_tool_loc, 'iceball', 'ice_repo_tgz.py')
        try:
            subprocess.check_call([exec_ice, '-b', version, '-o', ice_distro])
        except subprocess.CalledProcessError:
            raise RuntimeError('Unable to create %s distro' % ice_distro)
    gz_file = ''
    for file_loc in os.listdir(iceball_loc):
        sfield = '^ICE-.*{0}\.tar\.gz$'.format(ice_distro)
        if re.search(sfield, file_loc):
            if file_loc > gz_file:
                gz_file = file_loc
    lgz_file = os.path.join(iceball_loc, gz_file)
    try:
        subprocess.check_call(['scp', lgz_file, "%s:/tmp" % client_id])
    except subprocess.CalledProcessError:
        raise RuntimeError('Copy of ICE zip file failed')
    ret = cal_svr.run(args=['gunzip', run.Raw('<'), "/tmp/%s" % gz_file,
                      run.Raw('|'), 'tar', 'xvf', run.Raw('-')])
    if ret.exitstatus:
        raise RuntimeError('remote tar failed')
    icesetdata = 'yes\n%s\nhttp\n' % client_id
    ice_in = StringIO(icesetdata)
    ice_setup_io = StringIO()
    ret = cal_svr.run(args=['sudo', 'python', 'ice_setup.py'], stdin=ice_in,
                      stdout=ice_setup_io)
    # Run Calamari-ceph connect.
    if ret.exitstatus:
        raise RuntimeError('ice_setup.py failed')
    icesetdata = '%s\n%s\n%s\n%s\n' % (calamari_user, email, calamari_password,
                                       calamari_password)
    ice_in = StringIO(icesetdata)
    ret = cal_svr.run(args=['sudo', 'calamari-ctl', 'initialize'],
                      stdin=ice_in, stdout=ice_setup_io)
    if ret.exitstatus:
        raise RuntimeError('calamari-ctl initialize failed')
    try:
        yield
    finally:
        log.info('Cleaning up after Calamari installation')


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
