"""
Ceph cluster task.

Handle the setup, starting, and clean-up of a Ceph cluster.
"""
from copy import deepcopy
from io import BytesIO
from io import StringIO

import argparse
import configobj
import contextlib
import errno
import logging
import os
import json
import time
import gevent
import re
import socket
import yaml

from paramiko import SSHException
from tasks.ceph_manager import CephManager, write_conf, get_valgrind_args
from tarfile import ReadError
from tasks.cephfs.filesystem import MDSCluster, Filesystem
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology import exceptions
from teuthology.orchestra import run
from teuthology.util.scanner import ValgrindScanner
from tasks import ceph_client as cclient
from teuthology.orchestra.daemon import DaemonGroup
from tasks.daemonwatchdog import DaemonWatchdog

CEPH_ROLE_TYPES = ['mon', 'mgr', 'osd', 'mds', 'rgw']
DATA_PATH = '/var/lib/ceph/{type_}/{cluster}-{id_}'

log = logging.getLogger(__name__)


def generate_caps(type_):
    """
    Each call will return the next capability for each system type
    (essentially a subset of possible role values).  Valid types are osd,
    mds and client.
    """
    defaults = dict(
        osd=dict(
            mon='allow profile osd',
            mgr='allow profile osd',
            osd='allow *',
        ),
        mgr=dict(
            mon='allow profile mgr',
            osd='allow *',
            mds='allow *',
        ),
        mds=dict(
            mon='allow *',
            mgr='allow *',
            osd='allow *',
            mds='allow',
        ),
        client=dict(
            mon='allow rw',
            mgr='allow r',
            osd='allow rwx',
            mds='allow',
        ),
    )
    for subsystem, capability in defaults[type_].items():
        yield '--cap'
        yield subsystem
        yield capability


def update_archive_setting(ctx, key, value):
    """
    Add logs directory to job's info log file
    """
    if ctx.archive is None:
        return
    with open(os.path.join(ctx.archive, 'info.yaml'), 'r+') as info_file:
        info_yaml = yaml.safe_load(info_file)
        info_file.seek(0)
        if 'archive' in info_yaml:
            info_yaml['archive'][key] = value
        else:
            info_yaml['archive'] = {key: value}
        yaml.safe_dump(info_yaml, info_file, default_flow_style=False)


@contextlib.contextmanager
def ceph_crash(ctx, config):
    """
    Gather crash dumps from /var/lib/ceph/crash
    """

    # Add crash directory to job's archive
    update_archive_setting(ctx, 'crash', '/var/lib/ceph/crash')

    try:
        yield

    finally:
        if ctx.archive is not None:
            log.info('Archiving crash dumps...')
            path = os.path.join(ctx.archive, 'remote')
            try:
                os.makedirs(path)
            except OSError:
                pass
            for remote in ctx.cluster.remotes.keys():
                sub = os.path.join(path, remote.shortname)
                try:
                    os.makedirs(sub)
                except OSError:
                    pass
                try:
                    teuthology.pull_directory(remote, '/var/lib/ceph/crash',
                                              os.path.join(sub, 'crash'))
                except ReadError:
                    pass


@contextlib.contextmanager
def ceph_log(ctx, config):
    """
    Create /var/log/ceph log directory that is open to everyone.
    Add valgrind and profiling-logger directories.

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Making ceph log dir writeable by non-root...')
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'chmod',
                '777',
                '/var/log/ceph',
            ],
            wait=False,
        )
    )
    log.info('Disabling ceph logrotate...')
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'rm', '-f', '--',
                '/etc/logrotate.d/ceph',
            ],
            wait=False,
        )
    )
    log.info('Creating extra log directories...')
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'install', '-d', '-m0777', '--',
                '/var/log/ceph/valgrind',
                '/var/log/ceph/profiling-logger',
            ],
            wait=False,
        )
    )

    # Add logs directory to job's info log file
    update_archive_setting(ctx, 'log', '/var/log/ceph')

    class Rotater(object):
        stop_event = gevent.event.Event()

        def invoke_logrotate(self):
            # 1) install ceph-test.conf in /etc/logrotate.d
            # 2) continuously loop over logrotate invocation with ceph-test.conf
            while not self.stop_event.is_set():
                self.stop_event.wait(timeout=30)
                try:
                    procs = ctx.cluster.run(
                          args=['sudo', 'logrotate', '/etc/logrotate.d/ceph-test.conf'],
                          wait=False,
                          stderr=StringIO()
                    )
                    run.wait(procs)
                except exceptions.ConnectionLostError as e:
                    # Some tests may power off nodes during test, in which
                    # case we will see connection errors that we should ignore.
                    log.debug("Missed logrotate, node '{0}' is offline".format(
                        e.node))
                except EOFError:
                    # Paramiko sometimes raises this when it fails to
                    # connect to a node during open_session.  As with
                    # ConnectionLostError, we ignore this because nodes
                    # are allowed to get power cycled during tests.
                    log.debug("Missed logrotate, EOFError")
                except SSHException:
                    log.debug("Missed logrotate, SSHException")
                except run.CommandFailedError as e:
                    for p in procs:
                        if p.finished and p.exitstatus != 0:
                            err = p.stderr.getvalue()
                            if 'error: error renaming temp state file' in err:
                                log.info('ignoring transient state error: %s', e)
                            else:
                                raise
                except socket.error as e:
                    if e.errno in (errno.EHOSTUNREACH, errno.ECONNRESET):
                        log.debug("Missed logrotate, host unreachable")
                    else:
                        raise

        def begin(self):
            self.thread = gevent.spawn(self.invoke_logrotate)

        def end(self):
            self.stop_event.set()
            self.thread.get()

    def write_rotate_conf(ctx, daemons):
        testdir = teuthology.get_testdir(ctx)
        remote_logrotate_conf = '%s/logrotate.ceph-test.conf' % testdir
        rotate_conf_path = os.path.join(os.path.dirname(__file__), 'logrotate.conf')
        with open(rotate_conf_path) as f:
            conf = ""
            for daemon, size in daemons.items():
                log.info('writing logrotate stanza for {}'.format(daemon))
                conf += f.read().format(daemon_type=daemon,
                                        max_size=size)
                f.seek(0, 0)

            for remote in ctx.cluster.remotes.keys():
                remote.write_file(remote_logrotate_conf, BytesIO(conf.encode()))
                remote.sh(
                    f'sudo mv {remote_logrotate_conf} /etc/logrotate.d/ceph-test.conf && '
                    'sudo chmod 0644 /etc/logrotate.d/ceph-test.conf && '
                    'sudo chown root.root /etc/logrotate.d/ceph-test.conf')
                remote.chcon('/etc/logrotate.d/ceph-test.conf',
                             'system_u:object_r:etc_t:s0')

    if ctx.config.get('log-rotate'):
        daemons = ctx.config.get('log-rotate')
        log.info('Setting up log rotation with ' + str(daemons))
        write_rotate_conf(ctx, daemons)
        logrotater = Rotater()
        logrotater.begin()
    try:
        yield

    finally:
        if ctx.config.get('log-rotate'):
            log.info('Shutting down logrotate')
            logrotater.end()
            ctx.cluster.sh('sudo rm /etc/logrotate.d/ceph-test.conf')
        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and ctx.summary['success']):
            # and logs
            log.info('Compressing logs...')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'time',
                        'sudo',
                        'find',
                        '/var/log/ceph',
                        '-name',
                        '*.log',
                        '-print0',
                        run.Raw('|'),
                        'sudo',
                        'xargs',
                        '--max-args=1',
                        '--max-procs=0',
                        '--verbose',
                        '-0',
                        '--no-run-if-empty',
                        '--',
                        'gzip',
                        '-5',
                        '--verbose',
                        '--',
                    ],
                    wait=False,
                ),
            )

            log.info('Archiving logs...')
            path = os.path.join(ctx.archive, 'remote')
            try:
                os.makedirs(path)
            except OSError:
                pass
            for remote in ctx.cluster.remotes.keys():
                sub = os.path.join(path, remote.shortname)
                try:
                    os.makedirs(sub)
                except OSError:
                    pass
                teuthology.pull_directory(remote, '/var/log/ceph',
                                          os.path.join(sub, 'log'))


def assign_devs(roles, devs):
    """
    Create a dictionary of devs indexed by roles

    :param roles: List of roles
    :param devs: Corresponding list of devices.
    :returns: Dictionary of devs indexed by roles.
    """
    return dict(zip(roles, devs))


@contextlib.contextmanager
def valgrind_post(ctx, config):
    """
    After the tests run, look through all the valgrind logs.  Exceptions are raised
    if textual errors occurred in the logs, or if valgrind exceptions were detected in
    the logs.

    :param ctx: Context
    :param config: Configuration
    """
    try:
        yield
    finally:
        valgrind_exception = None
        valgrind_yaml = os.path.join(ctx.archive, 'valgrind.yaml')
        for remote in ctx.cluster.remotes.keys():
            scanner = ValgrindScanner(remote)
            errors = scanner.scan_all_files('/var/log/ceph/valgrind/*')
            scanner.write_summary(valgrind_yaml)
            if errors and not valgrind_exception:
                log.debug('valgrind exception message: %s', errors[0])
                valgrind_exception = Exception(errors[0])

        if config.get('expect_valgrind_errors'):
            if not valgrind_exception:
                raise Exception('expected valgrind issues and found none')
        else:
            if valgrind_exception:
                raise valgrind_exception


@contextlib.contextmanager
def crush_setup(ctx, config):
    cluster_name = config['cluster']
    first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.keys()

    profile = config.get('crush_tunables', 'default')
    log.info('Setting crush tunables to %s', profile)
    mon_remote.run(
        args=['sudo', 'ceph', '--cluster', cluster_name,
              'osd', 'crush', 'tunables', profile])
    yield


@contextlib.contextmanager
def module_setup(ctx, config):
    cluster_name = config['cluster']
    first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.keys()

    modules = config.get('mgr-modules', [])
    for m in modules:
        m = str(m)
        cmd = [
           'sudo',
           'ceph',
           '--cluster',
           cluster_name,
           'mgr',
           'module',
           'emable',
           m,
        ]
        log.info("enabling module %s", m)
        mon_remote.run(args=cmd)
    yield


@contextlib.contextmanager
def conf_setup(ctx, config):
    cluster_name = config['cluster']
    first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.keys()

    configs = config.get('cluster-conf', {})
    procs = []
    for section, confs in configs.items():
        section = str(section)
        for k, v in confs.items():
            k = str(k).replace(' ', '_') # pre-pacific compatibility
            v = str(v)
            cmd = [
                'sudo',
                'ceph',
                '--cluster',
                cluster_name,
                'config',
                'set',
                section,
                k,
                v,
            ]
            log.info("setting config [%s] %s = %s", section, k, v)
            procs.append(mon_remote.run(args=cmd, wait=False))
    log.debug("set %d configs", len(procs))
    for p in procs:
        log.debug("waiting for %s", p)
        p.wait()
    yield

@contextlib.contextmanager
def conf_epoch(ctx, config):
    cm = ctx.managers[config['cluster']]
    cm.save_conf_epoch()
    yield

@contextlib.contextmanager
def check_enable_crimson(ctx, config):
    # enable crimson-osds if crimson
    log.info("check_enable_crimson: {}".format(is_crimson(config)))
    if is_crimson(config):
        cluster_name = config['cluster']
        first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
        (mon_remote,) = ctx.cluster.only(first_mon).remotes.keys()
        log.info('check_enable_crimson: setting set-allow-crimson')
        mon_remote.run(
            args=[
                'sudo', 'ceph', '--cluster', cluster_name,
                'osd', 'set-allow-crimson', '--yes-i-really-mean-it'
            ]
        )
    yield


@contextlib.contextmanager
def setup_manager(ctx, config):
    first_mon = teuthology.get_first_mon(ctx, config, config['cluster'])
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    if not hasattr(ctx, 'managers'):
        ctx.managers = {}
    ctx.managers[config['cluster']] = CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager.' + config['cluster']),
        cluster=config['cluster'],
    )
    yield

@contextlib.contextmanager
def create_rbd_pool(ctx, config):
    cluster_name = config['cluster']
    first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.keys()
    log.info('Waiting for OSDs to come up')
    teuthology.wait_until_osds_up(
        ctx,
        cluster=ctx.cluster,
        remote=mon_remote,
        ceph_cluster=cluster_name,
    )
    if config.get('create_rbd_pool', True):
        log.info('Creating RBD pool')
        mon_remote.run(
            args=['sudo', 'ceph', '--cluster', cluster_name,
                  'osd', 'pool', 'create', 'rbd', '8'])
        mon_remote.run(
            args=[
                'sudo', 'ceph', '--cluster', cluster_name,
                'osd', 'pool', 'application', 'enable',
                'rbd', 'rbd', '--yes-i-really-mean-it'
            ],
            check_status=False)
    yield

@contextlib.contextmanager
def cephfs_setup(ctx, config):
    cluster_name = config['cluster']

    first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.keys()
    mdss = ctx.cluster.only(teuthology.is_type('mds', cluster_name))
    # If there are any MDSs, then create a filesystem for them to use
    # Do this last because requires mon cluster to be up and running
    if mdss.remotes:
        log.info('Setting up CephFS filesystem(s)...')
        cephfs_config = config.get('cephfs', {})
        fs_configs =  cephfs_config.pop('fs', [{'name': 'cephfs'}])

        # wait for standbys to become available (slow due to valgrind, perhaps)
        mdsc = MDSCluster(ctx)
        mds_count = len(list(teuthology.all_roles_of_type(ctx.cluster, 'mds')))
        with contextutil.safe_while(sleep=2,tries=150) as proceed:
            while proceed():
                if len(mdsc.get_standby_daemons()) >= mds_count:
                    break

        fss = []
        for fs_config in fs_configs:
            assert isinstance(fs_config, dict)
            name = fs_config.pop('name')
            temp = deepcopy(cephfs_config)
            teuthology.deep_merge(temp, fs_config)
            subvols = config.get('subvols', None)
            if subvols:
                teuthology.deep_merge(temp, {'subvols': subvols})
            fs = Filesystem(ctx, fs_config=temp, name=name, create=True)
            fss.append(fs)

        yield

        for fs in fss:
            fs.destroy()
    else:
        yield

@contextlib.contextmanager
def watchdog_setup(ctx, config):
    ctx.ceph[config['cluster']].thrashers = []
    ctx.ceph[config['cluster']].watchdog = DaemonWatchdog(ctx, config, ctx.ceph[config['cluster']].thrashers)
    ctx.ceph[config['cluster']].watchdog.start()
    yield

def get_mons(roles, ips, cluster_name,
             mon_bind_msgr2=False,
             mon_bind_addrvec=False):
    """
    Get monitors and their associated addresses
    """
    mons = {}
    v1_ports = {}
    v2_ports = {}
    is_mon = teuthology.is_type('mon', cluster_name)
    for idx, roles in enumerate(roles):
        for role in roles:
            if not is_mon(role):
                continue
            if ips[idx] not in v1_ports:
                v1_ports[ips[idx]] = 6789
            else:
                v1_ports[ips[idx]] += 1
            if mon_bind_msgr2:
                if ips[idx] not in v2_ports:
                    v2_ports[ips[idx]] = 3300
                    addr = '{ip}'.format(ip=ips[idx])
                else:
                    assert mon_bind_addrvec
                    v2_ports[ips[idx]] += 1
                    addr = '[v2:{ip}:{port2},v1:{ip}:{port1}]'.format(
                        ip=ips[idx],
                        port2=v2_ports[ips[idx]],
                        port1=v1_ports[ips[idx]],
                    )
            elif mon_bind_addrvec:
                addr = '[v1:{ip}:{port}]'.format(
                    ip=ips[idx],
                    port=v1_ports[ips[idx]],
                )
            else:
                addr = '{ip}:{port}'.format(
                    ip=ips[idx],
                    port=v1_ports[ips[idx]],
                )
            mons[role] = addr
    assert mons
    return mons

def skeleton_config(ctx, roles, ips, mons, cluster='ceph'):
    """
    Returns a ConfigObj that is prefilled with a skeleton config.

    Use conf[section][key]=value or conf.merge to change it.

    Use conf.write to write it out, override .filename first if you want.
    """
    path = os.path.join(os.path.dirname(__file__), 'ceph.conf.template')
    conf = configobj.ConfigObj(path, file_error=True)
    mon_hosts = []
    for role, addr in mons.items():
        mon_cluster, _, _ = teuthology.split_role(role)
        if mon_cluster != cluster:
            continue
        name = teuthology.ceph_role(role)
        conf.setdefault(name, {})
        mon_hosts.append(addr)
    conf.setdefault('global', {})
    conf['global']['mon host'] = ','.join(mon_hosts)
    # set up standby mds's
    is_mds = teuthology.is_type('mds', cluster)
    for roles_subset in roles:
        for role in roles_subset:
            if is_mds(role):
                name = teuthology.ceph_role(role)
                conf.setdefault(name, {})
    return conf

def create_simple_monmap(ctx, remote, conf, mons,
                         path=None,
                         mon_bind_addrvec=False):
    """
    Writes a simple monmap based on current ceph.conf into path, or
    <testdir>/monmap by default.

    Assumes ceph_conf is up to date.

    Assumes mon sections are named "mon.*", with the dot.

    :return the FSID (as a string) of the newly created monmap
    """

    addresses = list(mons.items())
    assert addresses, "There are no monitors in config!"
    log.debug('Ceph mon addresses: %s', addresses)

    try:
        log.debug('writing out conf {c}'.format(c=conf))
    except:
        log.debug('my conf logging attempt failed')
    testdir = teuthology.get_testdir(ctx)
    tmp_conf_path = '{tdir}/ceph.tmp.conf'.format(tdir=testdir)
    conf_fp = BytesIO()
    conf.write(conf_fp)
    conf_fp.seek(0)
    teuthology.write_file(remote, tmp_conf_path, conf_fp)
    args = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'monmaptool',
        '-c',
        '{conf}'.format(conf=tmp_conf_path),
        '--create',
        '--clobber',
    ]
    if mon_bind_addrvec:
        args.extend(['--enable-all-features'])
    for (role, addr) in addresses:
        _, _, n = teuthology.split_role(role)
        if mon_bind_addrvec and (',' in addr or 'v' in addr or ':' in addr):
            args.extend(('--addv', n, addr))
        else:
            args.extend(('--add', n, addr))
    if not path:
        path = '{tdir}/monmap'.format(tdir=testdir)
    args.extend([
        '--print',
        path
    ])

    monmap_output = remote.sh(args)
    fsid = re.search("generated fsid (.+)$",
                     monmap_output, re.MULTILINE).group(1)
    teuthology.delete_file(remote, tmp_conf_path)
    return fsid


def is_crimson(config):
    return config.get('flavor', 'default') == 'crimson'


def maybe_redirect_stderr(config, type_, args, log_path):
    if type_ == 'osd' and is_crimson(config):
        # teuthworker uses ubuntu:ubuntu to access the test nodes
        create_log_cmd = \
            f'sudo install -b -o ubuntu -g ubuntu /dev/null {log_path}'
        return create_log_cmd, args + [run.Raw('2>>'), log_path]
    else:
        return None, args


@contextlib.contextmanager
def cluster(ctx, config):
    """
    Handle the creation and removal of a ceph cluster.

    On startup:
        Create directories needed for the cluster.
        Create remote journals for all osds.
        Create and set keyring.
        Copy the monmap to the test systems.
        Setup mon nodes.
        Setup mds nodes.
        Mkfs osd nodes.
        Add keyring information to monmaps
        Mkfs mon nodes.

    On exit:
        If errors occurred, extract a failure message and store in ctx.summary.
        Unmount all test files and temporary journaling files.
        Save the monitor information and archive all ceph logs.
        Cleanup the keyring setup, and remove all monitor map and data files left over.

    :param ctx: Context
    :param config: Configuration
    """
    if ctx.config.get('use_existing_cluster', False) is True:
        log.info("'use_existing_cluster' is true; skipping cluster creation")
        yield

    testdir = teuthology.get_testdir(ctx)
    cluster_name = config['cluster']
    data_dir = '{tdir}/{cluster}.data'.format(tdir=testdir, cluster=cluster_name)
    log.info('Creating ceph cluster %s...', cluster_name)
    log.info('config %s', config)
    log.info('ctx.config %s', ctx.config)
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                data_dir,
            ],
            wait=False,
        )
    )

    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'install', '-d', '-m0777', '--', '/var/run/ceph',
            ],
            wait=False,
        )
    )

    devs_to_clean = {}
    remote_to_roles_to_devs = {}
    osds = ctx.cluster.only(teuthology.is_type('osd', cluster_name))
    for remote, roles_for_host in osds.remotes.items():
        devs = teuthology.get_scratch_devices(remote)
        roles_to_devs = assign_devs(
            teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name), devs
        )
        devs_to_clean[remote] = []
        log.info('osd dev map: {}'.format(roles_to_devs))
        assert roles_to_devs, \
            "remote {} has osd roles, but no osd devices were specified!".format(remote.hostname)
        remote_to_roles_to_devs[remote] = roles_to_devs
    log.info("remote_to_roles_to_devs: {}".format(remote_to_roles_to_devs))
    for osd_role, dev_name in remote_to_roles_to_devs.items():
        assert dev_name, "{} has no associated device!".format(osd_role)

    log.info('Generating config...')
    remotes_and_roles = ctx.cluster.remotes.items()
    roles = [role_list for (remote, role_list) in remotes_and_roles]
    ips = [host for (host, port) in
           (remote.ssh.get_transport().getpeername() for (remote, role_list) in remotes_and_roles)]
    mons = get_mons(
        roles, ips, cluster_name,
        mon_bind_msgr2=config.get('mon_bind_msgr2'),
        mon_bind_addrvec=config.get('mon_bind_addrvec'),
        )
    conf = skeleton_config(
        ctx, roles=roles, ips=ips, mons=mons, cluster=cluster_name,
    )
    for section, keys in config['conf'].items():
        for key, value in keys.items():
            log.info("[%s] %s = %s" % (section, key, value))
            if section not in conf:
                conf[section] = {}
            conf[section][key] = value

    if not hasattr(ctx, 'ceph'):
        ctx.ceph = {}
    ctx.ceph[cluster_name] = argparse.Namespace()
    ctx.ceph[cluster_name].conf = conf
    ctx.ceph[cluster_name].mons = mons

    default_keyring = '/etc/ceph/{cluster}.keyring'.format(cluster=cluster_name)
    keyring_path = config.get('keyring_path', default_keyring)

    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)

    firstmon = teuthology.get_first_mon(ctx, config, cluster_name)

    log.info('Setting up %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            coverage_dir,
            'ceph-authtool',
            '--create-keyring',
            keyring_path,
        ],
    )
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            coverage_dir,
            'ceph-authtool',
            '--gen-key',
            '--name=mon.',
            keyring_path,
        ],
    )
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'chmod',
            '0644',
            keyring_path,
        ],
    )
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    monmap_path = '{tdir}/{cluster}.monmap'.format(tdir=testdir,
                                                   cluster=cluster_name)
    fsid = create_simple_monmap(
        ctx,
        remote=mon0_remote,
        conf=conf,
        mons=mons,
        path=monmap_path,
        mon_bind_addrvec=config.get('mon_bind_addrvec'),
    )
    ctx.ceph[cluster_name].fsid = fsid
    if not 'global' in conf:
        conf['global'] = {}
    conf['global']['fsid'] = fsid

    default_conf_path = '/etc/ceph/{cluster}.conf'.format(cluster=cluster_name)
    conf_path = config.get('conf_path', default_conf_path)
    log.info('Writing %s for FSID %s...' % (conf_path, fsid))
    write_conf(ctx, conf_path, cluster_name)

    log.info('Creating admin key on %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            coverage_dir,
            'ceph-authtool',
            '--gen-key',
            '--name=client.admin',
            '--cap', 'mon', 'allow *',
            '--cap', 'osd', 'allow *',
            '--cap', 'mds', 'allow *',
            '--cap', 'mgr', 'allow *',
            keyring_path,
        ],
    )

    log.info('Copying monmap to all nodes...')
    keyring = mon0_remote.read_file(keyring_path)
    monmap = mon0_remote.read_file(monmap_path)

    for rem in ctx.cluster.remotes.keys():
        # copy mon key and initial monmap
        log.info('Sending monmap to node {remote}'.format(remote=rem))
        rem.write_file(keyring_path, keyring, mode='0644', sudo=True)
        rem.write_file(monmap_path, monmap)

    log.info('Setting up mon nodes...')
    mons = ctx.cluster.only(teuthology.is_type('mon', cluster_name))

    if not config.get('skip_mgr_daemons', False):
        log.info('Setting up mgr nodes...')
        mgrs = ctx.cluster.only(teuthology.is_type('mgr', cluster_name))
        for remote, roles_for_host in mgrs.remotes.items():
            for role in teuthology.cluster_roles_of_type(roles_for_host, 'mgr',
                                                         cluster_name):
                _, _, id_ = teuthology.split_role(role)
                mgr_dir = DATA_PATH.format(
                    type_='mgr', cluster=cluster_name, id_=id_)
                remote.run(
                    args=[
                        'sudo',
                        'mkdir',
                        '-p',
                        mgr_dir,
                        run.Raw('&&'),
                        'sudo',
                        'adjust-ulimits',
                        'ceph-coverage',
                        coverage_dir,
                        'ceph-authtool',
                        '--create-keyring',
                        '--gen-key',
                        '--name=mgr.{id}'.format(id=id_),
                        mgr_dir + '/keyring',
                    ],
                )

    log.info('Setting up mds nodes...')
    mdss = ctx.cluster.only(teuthology.is_type('mds', cluster_name))
    for remote, roles_for_host in mdss.remotes.items():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'mds',
                                                     cluster_name):
            _, _, id_ = teuthology.split_role(role)
            mds_dir = DATA_PATH.format(
                type_='mds', cluster=cluster_name, id_=id_)
            remote.run(
                args=[
                    'sudo',
                    'mkdir',
                    '-p',
                    mds_dir,
                    run.Raw('&&'),
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-authtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=mds.{id}'.format(id=id_),
                    mds_dir + '/keyring',
                ],
            )
            remote.run(args=[
                'sudo', 'chown', '-R', 'ceph:ceph', mds_dir
            ])

    cclient.create_keyring(ctx, cluster_name)
    log.info('Running mkfs on osd nodes...')

    if not hasattr(ctx, 'disk_config'):
        ctx.disk_config = argparse.Namespace()
    if not hasattr(ctx.disk_config, 'remote_to_roles_to_dev'):
        ctx.disk_config.remote_to_roles_to_dev = {}
    if not hasattr(ctx.disk_config, 'remote_to_roles_to_dev_mount_options'):
        ctx.disk_config.remote_to_roles_to_dev_mount_options = {}
    if not hasattr(ctx.disk_config, 'remote_to_roles_to_dev_fstype'):
        ctx.disk_config.remote_to_roles_to_dev_fstype = {}

    teuthology.deep_merge(ctx.disk_config.remote_to_roles_to_dev, remote_to_roles_to_devs)

    log.info("ctx.disk_config.remote_to_roles_to_dev: {r}".format(r=str(ctx.disk_config.remote_to_roles_to_dev)))

    for remote, roles_for_host in osds.remotes.items():
        roles_to_devs = remote_to_roles_to_devs[remote]

        for role in teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            mnt_point = DATA_PATH.format(
                type_='osd', cluster=cluster_name, id_=id_)
            remote.run(
                args=[
                    'sudo',
                    'mkdir',
                    '-p',
                    mnt_point,
                ])
            log.info('roles_to_devs: {}'.format(roles_to_devs))
            log.info('role: {}'.format(role))
            if roles_to_devs.get(role):
                dev = roles_to_devs[role]
                fs = config.get('fs')
                package = None
                mkfs_options = config.get('mkfs_options')
                mount_options = config.get('mount_options')
                if fs == 'btrfs':
                    # package = 'btrfs-tools'
                    if mount_options is None:
                        mount_options = ['noatime', 'user_subvol_rm_allowed']
                    if mkfs_options is None:
                        mkfs_options = ['-m', 'single',
                                        '-l', '32768',
                                        '-n', '32768']
                if fs == 'xfs':
                    # package = 'xfsprogs'
                    if mount_options is None:
                        mount_options = ['noatime']
                    if mkfs_options is None:
                        mkfs_options = ['-f', '-i', 'size=2048']
                if fs == 'ext4' or fs == 'ext3':
                    if mount_options is None:
                        mount_options = ['noatime', 'user_xattr']

                if mount_options is None:
                    mount_options = []
                if mkfs_options is None:
                    mkfs_options = []
                mkfs = ['mkfs.%s' % fs] + mkfs_options
                log.info('%s on %s on %s' % (mkfs, dev, remote))
                if package is not None:
                    remote.sh('sudo apt-get install -y %s' % package)

                try:
                    remote.run(args=['yes', run.Raw('|')] + ['sudo'] + mkfs + [dev])
                except run.CommandFailedError:
                    # Newer btfs-tools doesn't prompt for overwrite, use -f
                    if '-f' not in mount_options:
                        mkfs_options.append('-f')
                        mkfs = ['mkfs.%s' % fs] + mkfs_options
                        log.info('%s on %s on %s' % (mkfs, dev, remote))
                    remote.run(args=['yes', run.Raw('|')] + ['sudo'] + mkfs + [dev])

                log.info('mount %s on %s -o %s' % (dev, remote,
                                                   ','.join(mount_options)))
                remote.run(
                    args=[
                        'sudo',
                        'mount',
                        '-t', fs,
                        '-o', ','.join(mount_options),
                        dev,
                        mnt_point,
                    ]
                )
                remote.run(
                    args=[
                        'sudo', '/sbin/restorecon', mnt_point,
                    ],
                    check_status=False,
                )
                if not remote in ctx.disk_config.remote_to_roles_to_dev_mount_options:
                    ctx.disk_config.remote_to_roles_to_dev_mount_options[remote] = {}
                ctx.disk_config.remote_to_roles_to_dev_mount_options[remote][role] = mount_options
                if not remote in ctx.disk_config.remote_to_roles_to_dev_fstype:
                    ctx.disk_config.remote_to_roles_to_dev_fstype[remote] = {}
                ctx.disk_config.remote_to_roles_to_dev_fstype[remote][role] = fs
                devs_to_clean[remote].append(mnt_point)

        for role in teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            try:
                args = ['sudo',
                        'MALLOC_CHECK_=3',
                        'adjust-ulimits',
                        'ceph-coverage', coverage_dir,
                        'ceph-osd',
                        '--no-mon-config',
                        '--cluster', cluster_name,
                        '--mkfs',
                        '--mkkey',
                        '-i', id_,
                        '--monmap', monmap_path]
                log_path = f'/var/log/ceph/{cluster_name}-osd.{id_}.log'
                create_log_cmd, args = \
                    maybe_redirect_stderr(config, 'osd', args, log_path)
                if create_log_cmd:
                    remote.sh(create_log_cmd)
                remote.run(args=args)
            except run.CommandFailedError:
                # try without --no-mon-config.. this may be an upgrade test
                remote.run(
                    args=[
                        'sudo',
                        'MALLOC_CHECK_=3',
                        'adjust-ulimits',
                        'ceph-coverage',
                        coverage_dir,
                        'ceph-osd',
                        '--cluster',
                        cluster_name,
                        '--mkfs',
                        '--mkkey',
                        '-i', id_,
                    '--monmap', monmap_path,
                    ],
                )
            mnt_point = DATA_PATH.format(
                type_='osd', cluster=cluster_name, id_=id_)
            remote.run(args=[
                'sudo', 'chown', '-R', 'ceph:ceph', mnt_point
            ])

    log.info('Reading keys from all nodes...')
    keys_fp = BytesIO()
    keys = []
    for remote, roles_for_host in ctx.cluster.remotes.items():
        for type_ in ['mgr',  'mds', 'osd']:
            if type_ == 'mgr' and config.get('skip_mgr_daemons', False):
                continue
            for role in teuthology.cluster_roles_of_type(roles_for_host, type_, cluster_name):
                _, _, id_ = teuthology.split_role(role)
                data = remote.read_file(
                    os.path.join(
                        DATA_PATH.format(
                            type_=type_, id_=id_, cluster=cluster_name),
                        'keyring',
                    ),
                    sudo=True,
                )
                keys.append((type_, id_, data))
                keys_fp.write(data)
    for remote, roles_for_host in ctx.cluster.remotes.items():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'client', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            data = remote.read_file(
                '/etc/ceph/{cluster}.client.{id}.keyring'.format(id=id_, cluster=cluster_name)
            )
            keys.append(('client', id_, data))
            keys_fp.write(data)

    log.info('Adding keys to all mons...')
    writes = mons.run(
        args=[
            'sudo', 'tee', '-a',
            keyring_path,
        ],
        stdin=run.PIPE,
        wait=False,
        stdout=BytesIO(),
    )
    keys_fp.seek(0)
    teuthology.feed_many_stdins_and_close(keys_fp, writes)
    run.wait(writes)
    for type_, id_, data in keys:
        run.wait(
            mons.run(
                args=[
                         'sudo',
                         'adjust-ulimits',
                         'ceph-coverage',
                         coverage_dir,
                         'ceph-authtool',
                         keyring_path,
                         '--name={type}.{id}'.format(
                             type=type_,
                             id=id_,
                         ),
                     ] + list(generate_caps(type_)),
                wait=False,
            ),
        )

    log.info('Running mkfs on mon nodes...')
    for remote, roles_for_host in mons.remotes.items():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'mon', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            mnt_point = DATA_PATH.format(
                type_='mon', id_=id_, cluster=cluster_name)
            remote.run(
                args=[
                    'sudo',
                    'mkdir',
                    '-p',
                    mnt_point,
                ],
            )
            remote.run(
                args=[
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-mon',
                    '--cluster', cluster_name,
                    '--mkfs',
                    '-i', id_,
                    '--monmap', monmap_path,
                    '--keyring', keyring_path,
                ],
            )
            remote.run(args=[
                'sudo', 'chown', '-R', 'ceph:ceph', mnt_point
            ])

    run.wait(
        mons.run(
            args=[
                'rm',
                '--',
                monmap_path,
            ],
            wait=False,
        ),
    )

    try:
        yield
    except Exception:
        # we need to know this below
        ctx.summary['success'] = False
        raise
    finally:
        (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()

        log.info('Checking cluster log for badness...')

        def first_in_ceph_log(pattern, excludes):
            """
            Find the first occurrence of the pattern specified in the Ceph log,
            Returns None if none found.

            :param pattern: Pattern scanned for.
            :param excludes: Patterns to ignore.
            :return: First line of text (or None if not found)
            """
            args = [
                'sudo',
                'egrep', pattern,
                '/var/log/ceph/{cluster}.log'.format(cluster=cluster_name),
            ]
            for exclude in excludes:
                args.extend([run.Raw('|'), 'egrep', '-v', exclude])
            args.extend([
                run.Raw('|'), 'head', '-n', '1',
            ])
            stdout = mon0_remote.sh(args)
            return stdout or None

        if first_in_ceph_log('\[ERR\]|\[WRN\]|\[SEC\]',
                             config['log_ignorelist']) is not None:
            log.warning('Found errors (ERR|WRN|SEC) in cluster log')
            ctx.summary['success'] = False
            # use the most severe problem as the failure reason
            if 'failure_reason' not in ctx.summary:
                for pattern in ['\[SEC\]', '\[ERR\]', '\[WRN\]']:
                    match = first_in_ceph_log(pattern, config['log_ignorelist'])
                    if match is not None:
                        ctx.summary['failure_reason'] = \
                            '"{match}" in cluster log'.format(
                                match=match.rstrip('\n'),
                            )
                        break

        for remote, dirs in devs_to_clean.items():
            for dir_ in dirs:
                log.info('Unmounting %s on %s' % (dir_, remote))
                try:
                    remote.run(
                        args=[
                            'sync',
                            run.Raw('&&'),
                            'sudo',
                            'umount',
                            '-f',
                            dir_
                        ]
                    )
                except Exception as e:
                    remote.run(args=[
                        'sudo',
                        run.Raw('PATH=/usr/sbin:$PATH'),
                        'lsof',
                        run.Raw(';'),
                        'ps', 'auxf',
                    ])
                    raise e

        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and ctx.summary['success']):

            # archive mon data, too
            log.info('Archiving mon data...')
            path = os.path.join(ctx.archive, 'data')
            try:
                os.makedirs(path)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            for remote, roles in mons.remotes.items():
                for role in roles:
                    is_mon = teuthology.is_type('mon', cluster_name)
                    if is_mon(role):
                        _, _, id_ = teuthology.split_role(role)
                        mon_dir = DATA_PATH.format(
                            type_='mon', id_=id_, cluster=cluster_name)
                        teuthology.pull_directory_tarball(
                            remote,
                            mon_dir,
                            path + '/' + role + '.tgz')

        log.info('Cleaning ceph cluster...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo',
                    'rm',
                    '-rf',
                    '--',
                    conf_path,
                    keyring_path,
                    data_dir,
                    monmap_path,
                    run.Raw('{tdir}/../*.pid'.format(tdir=testdir)),
                ],
                wait=False,
            ),
        )


def osd_scrub_pgs(ctx, config):
    """
    Scrub pgs when we exit.

    First make sure all pgs are active and clean.
    Next scrub all osds.
    Then periodically check until all pgs have scrub time stamps that
    indicate the last scrub completed.  Time out if no progress is made
    here after two minutes.
    """
    retries = 40
    delays = 20
    cluster_name = config['cluster']
    manager = ctx.managers[cluster_name]
    for _ in range(retries):
        stats = manager.get_pg_stats()
        unclean = [stat['pgid'] for stat in stats if 'active+clean' not in stat['state']]
        split_merge = []
        osd_dump = manager.get_osd_dump_json()
        try:
            split_merge = [i['pool_name'] for i in osd_dump['pools'] if i['pg_num'] != i['pg_num_target']]
        except KeyError:
            # we don't support pg_num_target before nautilus
            pass
        if not unclean and not split_merge:
            break
        waiting_on = []
        if unclean:
            waiting_on.append(f'{unclean} to go clean')
        if split_merge:
            waiting_on.append(f'{split_merge} to split/merge')
        waiting_on = ' and '.join(waiting_on)
        log.info('Waiting for all PGs to be active+clean and split+merged, waiting on %s', waiting_on)
        time.sleep(delays)
    else:
        raise RuntimeError("Scrubbing terminated -- not all pgs were active and clean.")
    check_time_now = time.localtime()
    time.sleep(1)
    all_roles = teuthology.all_roles(ctx.cluster)
    for role in teuthology.cluster_roles_of_type(all_roles, 'osd', cluster_name):
        log.info("Scrubbing {osd}".format(osd=role))
        _, _, id_ = teuthology.split_role(role)
        # allow this to fail; in certain cases the OSD might not be up
        # at this point.  we will catch all pgs below.
        try:
            manager.raw_cluster_cmd('tell', 'osd.' + id_, 'config', 'set',
                                    'osd_debug_deep_scrub_sleep', '0');
            manager.raw_cluster_cmd('osd', 'deep-scrub', id_)
        except run.CommandFailedError:
            pass
    prev_good = 0
    gap_cnt = 0
    loop = True
    while loop:
        stats = manager.get_pg_stats()
        timez = [(stat['pgid'],stat['last_scrub_stamp']) for stat in stats]
        loop = False
        thiscnt = 0
        re_scrub = []
        for (pgid, tmval) in timez:
            t = tmval[0:tmval.find('.')].replace(' ', 'T')
            pgtm = time.strptime(t, '%Y-%m-%dT%H:%M:%S')
            if pgtm > check_time_now:
                thiscnt += 1
            else:
                log.info('pgid %s last_scrub_stamp %s %s <= %s', pgid, tmval, pgtm, check_time_now)
                loop = True
                re_scrub.append(pgid)
        if thiscnt > prev_good:
            prev_good = thiscnt
            gap_cnt = 0
        else:
            gap_cnt += 1
            if gap_cnt % 6 == 0:
                for pgid in re_scrub:
                    # re-request scrub every so often in case the earlier
                    # request was missed.  do not do it every time because
                    # the scrub may be in progress or not reported yet and
                    # we will starve progress.
                    manager.raw_cluster_cmd('pg', 'deep-scrub', pgid)
            if gap_cnt > retries:
                raise RuntimeError('Exiting scrub checking -- not all pgs scrubbed.')
        if loop:
            log.info('Still waiting for all pgs to be scrubbed.')
            time.sleep(delays)


@contextlib.contextmanager
def run_daemon(ctx, config, type_):
    """
    Run daemons for a role type.  Handle the startup and termination of a a daemon.
    On startup -- set coverages, cpu_profile, valgrind values for all remotes,
    and a max_mds value for one mds.
    On cleanup -- Stop all existing daemons of this type.

    :param ctx: Context
    :param config: Configuration
    :param type_: Role type
    """
    cluster_name = config['cluster']
    log.info('Starting %s daemons in cluster %s...', type_, cluster_name)
    testdir = teuthology.get_testdir(ctx)
    daemons = ctx.cluster.only(teuthology.is_type(type_, cluster_name))

    # check whether any daemons if this type are configured
    if daemons is None:
        return
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)

    daemon_signal = 'kill'
    if config.get('coverage') or config.get('valgrind') is not None:
        daemon_signal = 'term'

    # create osds in order.  (this only matters for pre-luminous, which might
    # be jewel/hammer, which doesn't take an id_ argument to legacy 'osd create').
    osd_uuids  = {}
    for remote, roles_for_host in daemons.remotes.items():
        is_type_ = teuthology.is_type(type_, cluster_name)
        for role in roles_for_host:
            if not is_type_(role):
                continue
            _, _, id_ = teuthology.split_role(role)


            if type_ == 'osd':
                datadir='/var/lib/ceph/osd/{cluster}-{id}'.format(
                    cluster=cluster_name, id=id_)
                osd_uuid = remote.read_file(
                    datadir + '/fsid', sudo=True).decode().strip()
                osd_uuids[id_] = osd_uuid
    for osd_id in range(len(osd_uuids)):
        id_ = str(osd_id)
        osd_uuid = osd_uuids.get(id_)
        try:
            remote.run(
                args=[
                'sudo', 'ceph', '--cluster', cluster_name,
                    'osd', 'new', osd_uuid, id_,
                ]
            )
        except:
            # fallback to pre-luminous (jewel)
            remote.run(
                args=[
                'sudo', 'ceph', '--cluster', cluster_name,
                    'osd', 'create', osd_uuid,
                ]
            )
            if config.get('add_osds_to_crush'):
                remote.run(
                args=[
                    'sudo', 'ceph', '--cluster', cluster_name,
                    'osd', 'crush', 'create-or-move', 'osd.' + id_,
                    '1.0', 'host=localhost', 'root=default',
                ]
            )

    for remote, roles_for_host in daemons.remotes.items():
        is_type_ = teuthology.is_type(type_, cluster_name)
        for role in roles_for_host:
            if not is_type_(role):
                continue
            _, _, id_ = teuthology.split_role(role)

            run_cmd = [
                'sudo',
                'adjust-ulimits',
                'ceph-coverage',
                coverage_dir,
                'daemon-helper',
                daemon_signal,
            ]
            run_cmd_tail = [
                'ceph-%s' % (type_),
                '-f',
                '--cluster', cluster_name,
                '-i', id_]

            if type_ in config.get('cpu_profile', []):
                profile_path = '/var/log/ceph/profiling-logger/%s.prof' % (role)
                run_cmd.extend(['env', 'CPUPROFILE=%s' % profile_path])

            vc = config.get('valgrind')
            if vc is not None:
                valgrind_args = None
                if type_ in vc:
                    valgrind_args = vc[type_]
                if role in vc:
                    valgrind_args = vc[role]
                exit_on_first_error = vc.get('exit_on_first_error', True)
                run_cmd = get_valgrind_args(testdir, role, run_cmd, valgrind_args,
                    exit_on_first_error=exit_on_first_error)

            run_cmd.extend(run_cmd_tail)
            log_path = f'/var/log/ceph/{cluster_name}-{type_}.{id_}.log'
            create_log_cmd, run_cmd = \
                maybe_redirect_stderr(config, type_, run_cmd, log_path)
            if create_log_cmd:
                remote.sh(create_log_cmd)
            # always register mgr; don't necessarily start
            ctx.daemons.register_daemon(
                remote, type_, id_,
                cluster=cluster_name,
                args=run_cmd,
                logger=log.getChild(role),
                stdin=run.PIPE,
                wait=False
            )
            if type_ != 'mgr' or not config.get('skip_mgr_daemons', False):
                role = cluster_name + '.' + type_
                ctx.daemons.get_daemon(type_, id_, cluster_name).restart()

    # kludge: run any pre-manager commands
    if type_ == 'mon':
        for cmd in config.get('pre-mgr-commands', []):
            firstmon = teuthology.get_first_mon(ctx, config, cluster_name)
            (remote,) = ctx.cluster.only(firstmon).remotes.keys()
            remote.run(args=cmd.split(' '))

    try:
        yield
    finally:
        teuthology.stop_daemons_of_type(ctx, type_, cluster_name)


def healthy(ctx, config):
    """
    Wait for all osd's to be up, and for the ceph health monitor to return HEALTH_OK.

    :param ctx: Context
    :param config: Configuration
    """
    config = config if isinstance(config, dict) else dict()
    cluster_name = config.get('cluster', 'ceph')
    log.info('Waiting until %s daemons up and pgs clean...', cluster_name)
    manager = ctx.managers[cluster_name]
    try:
        manager.wait_for_mgr_available(timeout=30)
    except (run.CommandFailedError, AssertionError) as e:
        log.info('ignoring mgr wait error, probably testing upgrade: %s', e)

    manager.wait_for_all_osds_up(timeout=300)

    try:
        manager.flush_all_pg_stats()
    except (run.CommandFailedError, Exception) as e:
        log.info('ignoring flush pg stats error, probably testing upgrade: %s', e)
    manager.wait_for_clean()

    if config.get('wait-for-healthy', True):
        log.info('Waiting until ceph cluster %s is healthy...', cluster_name)
        manager.wait_until_healthy(timeout=300)

    if ctx.cluster.only(teuthology.is_type('mds', cluster_name)).remotes:
        # Some MDSs exist, wait for them to be healthy
        for fs in Filesystem.get_all_fs(ctx):
            fs.wait_for_daemons(timeout=300)

def wait_for_mon_quorum(ctx, config):
    """
    Check renote ceph status until all monitors are up.

    :param ctx: Context
    :param config: Configuration
    """
    if isinstance(config, dict):
        mons = config['daemons']
        cluster_name = config.get('cluster', 'ceph')
    else:
        assert isinstance(config, list)
        mons = config
        cluster_name = 'ceph'
    firstmon = teuthology.get_first_mon(ctx, config, cluster_name)
    (remote,) = ctx.cluster.only(firstmon).remotes.keys()
    with contextutil.safe_while(sleep=10, tries=60,
                                action='wait for monitor quorum') as proceed:
        while proceed():
            quorum_status = remote.sh('sudo ceph quorum_status',
                                      logger=log.getChild('quorum_status'))
            j = json.loads(quorum_status)
            q = j.get('quorum_names', [])
            log.debug('Quorum: %s', q)
            if sorted(q) == sorted(mons):
                break


def created_pool(ctx, config):
    """
    Add new pools to the dictionary of pools that the ceph-manager
    knows about.
    """
    for new_pool in config:
        if new_pool not in ctx.managers['ceph'].pools:
            ctx.managers['ceph'].pools[new_pool] = ctx.managers['ceph'].get_pool_int_property(
                new_pool, 'pg_num')


@contextlib.contextmanager
def suppress_mon_health_to_clog(ctx, config):
    """
    set the option, and then restore it with its original value

    Note, due to the way how tasks are executed/nested, it's not suggested to
    use this method as a standalone task. otherwise, it's likely that it will
    restore the tweaked option at the /end/ of 'tasks' block.
    """
    if config.get('mon-health-to-clog', 'true') == 'false':
        cluster = config.get('cluster', 'ceph')
        manager = ctx.managers[cluster]
        manager.raw_cluster_command(
            'config', 'set', 'mon', 'mon_health_to_clog', 'false'
        )
        yield
        manager.raw_cluster_command(
            'config', 'rm', 'mon', 'mon_health_to_clog'
        )
    else:
        yield

@contextlib.contextmanager
def restart(ctx, config):
    """
   restart ceph daemons

   For example::
      tasks:
      - ceph.restart: [all]

   For example::
      tasks:
      - ceph.restart: [osd.0, mon.1, mds.*]

   or::

      tasks:
      - ceph.restart:
          daemons: [osd.0, mon.1]
          wait-for-healthy: false
          wait-for-osds-up: true

    :param ctx: Context
    :param config: Configuration
    """
    if config is None:
        config = {}
    elif isinstance(config, list):
        config = {'daemons': config}

    daemons = ctx.daemons.resolve_role_list(config.get('daemons', None), CEPH_ROLE_TYPES, True)
    clusters = set()

    with suppress_mon_health_to_clog(ctx, config):
        for role in daemons:
            cluster, type_, id_ = teuthology.split_role(role)
            ctx.daemons.get_daemon(type_, id_, cluster).stop()
            if type_ == 'osd':
                ctx.managers[cluster].mark_down_osd(id_)
            ctx.daemons.get_daemon(type_, id_, cluster).restart()
            clusters.add(cluster)

    if config.get('wait-for-healthy', True):
        for cluster in clusters:
            healthy(ctx=ctx, config=dict(cluster=cluster))
    if config.get('wait-for-osds-up', False):
        for cluster in clusters:
            ctx.managers[cluster].wait_for_all_osds_up()
    if config.get('expected-failure') is not None:
        log.info('Checking for expected-failure in osds logs after restart...')
        expected_fail = config.get('expected-failure')
        is_osd = teuthology.is_type('osd')
        for role in daemons:
            if not is_osd(role):
                continue
            (remote,) = ctx.cluster.only(role).remotes.keys()
            cluster, type_, id_ = teuthology.split_role(role)
            remote.run(
               args = ['sudo',
                       'egrep', expected_fail,
                       '/var/log/ceph/{cluster}-{type_}.{id_}.log'.format(cluster=cluster, type_=type_, id_=id_),
                ])
    yield


@contextlib.contextmanager
def stop(ctx, config):
    """
    Stop ceph daemons

    For example::
      tasks:
      - ceph.stop: [mds.*]

      tasks:
      - ceph.stop: [osd.0, osd.2]

      tasks:
      - ceph.stop:
          daemons: [osd.0, osd.2]

    """
    if config is None:
        config = {}
    elif isinstance(config, list):
        config = {'daemons': config}

    daemons = ctx.daemons.resolve_role_list(config.get('daemons', None), CEPH_ROLE_TYPES, True)
    clusters = set()

    for role in daemons:
        cluster, type_, id_ = teuthology.split_role(role)
        ctx.daemons.get_daemon(type_, id_, cluster).stop()
        clusters.add(cluster)


    for cluster in clusters:
        ctx.ceph[cluster].watchdog.stop()
        ctx.ceph[cluster].watchdog.join()

    yield


@contextlib.contextmanager
def wait_for_failure(ctx, config):
    """
    Wait for a failure of a ceph daemon

    For example::
      tasks:
      - ceph.wait_for_failure: [mds.*]

      tasks:
      - ceph.wait_for_failure: [osd.0, osd.2]

      tasks:
      - ceph.wait_for_failure:
          daemons: [osd.0, osd.2]

    """
    if config is None:
        config = {}
    elif isinstance(config, list):
        config = {'daemons': config}

    daemons = ctx.daemons.resolve_role_list(config.get('daemons', None), CEPH_ROLE_TYPES, True)
    for role in daemons:
        cluster, type_, id_ = teuthology.split_role(role)
        try:
            ctx.daemons.get_daemon(type_, id_, cluster).wait()
        except:
            log.info('Saw expected daemon failure.  Continuing.')
            pass
        else:
            raise RuntimeError('daemon %s did not fail' % role)

    yield


def validate_config(ctx, config):
    """
    Perform some simple validation on task configuration.
    Raises exceptions.ConfigError if an error is found.
    """
    # check for osds from multiple clusters on the same host
    for remote, roles_for_host in ctx.cluster.remotes.items():
        last_cluster = None
        last_role = None
        for role in roles_for_host:
            role_cluster, role_type, _ = teuthology.split_role(role)
            if role_type != 'osd':
                continue
            if last_cluster and last_cluster != role_cluster:
                msg = "Host should not have osds (%s and %s) from multiple clusters" % (
                    last_role, role)
                raise exceptions.ConfigError(msg)
            last_cluster = role_cluster
            last_role = role


@contextlib.contextmanager
def task(ctx, config):
    """
    Set up and tear down a Ceph cluster.

    For example::

        tasks:
        - ceph:
        - interactive:

    You can also specify what branch to run::

        tasks:
        - ceph:
            branch: foo

    Or a tag::

        tasks:
        - ceph:
            tag: v0.42.13

    Or a sha1::

        tasks:
        - ceph:
            sha1: 1376a5ab0c89780eab39ffbbe436f6a6092314ed

    Or a local source dir::

        tasks:
        - ceph:
            path: /home/sage/ceph

    To capture code coverage data, use::

        tasks:
        - ceph:
            coverage: true

    To use btrfs, ext4, or xfs on the target's scratch disks, use::

        tasks:
        - ceph:
            fs: xfs
            mkfs_options: [-b,size=65536,-l,logdev=/dev/sdc1]
            mount_options: [nobarrier, inode64]

    To change the cephfs's default max_mds (1), use::

        tasks:
        - ceph:
            cephfs:
              max_mds: 2

    To change the max_mds of a specific filesystem, use::

        tasks:
        - ceph:
            cephfs:
              max_mds: 2
              fs:
                - name: a
                  max_mds: 3
                - name: b

    In the above example, filesystem 'a' will have 'max_mds' 3,
    and filesystme 'b' will have 'max_mds' 2.

    To change the mdsmap's default session_timeout (60 seconds), use::

        tasks:
        - ceph:
            cephfs:
              session_timeout: 300

    Note, this will cause the task to check the /scratch_devs file on each node
    for available devices.  If no such file is found, /dev/sdb will be used.

    To run some daemons under valgrind, include their names
    and the tool/args to use in a valgrind section::

        tasks:
        - ceph:
          valgrind:
            mds.1: --tool=memcheck
            osd.1: [--tool=memcheck, --leak-check=no]

    Those nodes which are using memcheck or valgrind will get
    checked for bad results.

    To adjust or modify config options, use::

        tasks:
        - ceph:
            conf:
              section:
                key: value

    For example::

        tasks:
        - ceph:
            conf:
              mds.0:
                some option: value
                other key: other value
              client.0:
                debug client: 10
                debug ms: 1

    By default, the cluster log is checked for errors and warnings,
    and the run marked failed if any appear. You can ignore log
    entries by giving a list of egrep compatible regexes, i.e.:

        tasks:
        - ceph:
            log-ignorelist: ['foo.*bar', 'bad message']

    To run multiple ceph clusters, use multiple ceph tasks, and roles
    with a cluster name prefix, e.g. cluster1.client.0. Roles with no
    cluster use the default cluster name, 'ceph'. OSDs from separate
    clusters must be on separate hosts. Clients and non-osd daemons
    from multiple clusters may be colocated. For each cluster, add an
    instance of the ceph task with the cluster name specified, e.g.::

        roles:
        - [mon.a, osd.0, osd.1]
        - [backup.mon.a, backup.osd.0, backup.osd.1]
        - [client.0, backup.client.0]
        tasks:
        - ceph:
            cluster: ceph
        - ceph:
            cluster: backup

    :param ctx: Context
    :param config: Configuration

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))

    first_ceph_cluster = False
    if not hasattr(ctx, 'daemons'):
        first_ceph_cluster = True
        ctx.daemons = DaemonGroup()

    testdir = teuthology.get_testdir(ctx)
    if config.get('coverage'):
        coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
        log.info('Creating coverage directory...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'install', '-d', '-m0755', '--',
                    coverage_dir,
                ],
                wait=False,
            )
        )

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    validate_config(ctx, config)

    subtasks = []
    if first_ceph_cluster:
        # these tasks handle general log setup and parsing on all hosts,
        # so they should only be run once
        subtasks = [
            lambda: ceph_log(ctx=ctx, config=None),
            lambda: ceph_crash(ctx=ctx, config=None),
            lambda: valgrind_post(ctx=ctx, config=config),
        ]

    subtasks += [
        lambda: cluster(ctx=ctx, config=dict(
            conf=config.get('conf', {}),
            fs=config.get('fs', 'xfs'),
            mkfs_options=config.get('mkfs_options', None),
            mount_options=config.get('mount_options', None),
            skip_mgr_daemons=config.get('skip_mgr_daemons', False),
            log_ignorelist=config.get('log-ignorelist', []),
            cpu_profile=set(config.get('cpu_profile', []),),
            cluster=config['cluster'],
            mon_bind_msgr2=config.get('mon_bind_msgr2', True),
            mon_bind_addrvec=config.get('mon_bind_addrvec', True),
        )),
        lambda: run_daemon(ctx=ctx, config=config, type_='mon'),
        lambda: module_setup(ctx=ctx, config=config),
        lambda: run_daemon(ctx=ctx, config=config, type_='mgr'),
        lambda: conf_setup(ctx=ctx, config=config),
        lambda: crush_setup(ctx=ctx, config=config),
        lambda: check_enable_crimson(ctx=ctx, config=config),
        lambda: run_daemon(ctx=ctx, config=config, type_='osd'),
        lambda: setup_manager(ctx=ctx, config=config),
        lambda: create_rbd_pool(ctx=ctx, config=config),
        lambda: run_daemon(ctx=ctx, config=config, type_='mds'),
        lambda: cephfs_setup(ctx=ctx, config=config),
        lambda: watchdog_setup(ctx=ctx, config=config),
        lambda: conf_epoch(ctx=ctx, config=config),
    ]

    with contextutil.nested(*subtasks):
        try:
            if config.get('wait-for-healthy', True):
                healthy(ctx=ctx, config=dict(cluster=config['cluster']))

            yield
        finally:
            # set pg_num_targets back to actual pg_num, so we don't have to
            # wait for pending merges (which can take a while!)
            if not config.get('skip_stop_pg_num_changes', True):
                ctx.managers[config['cluster']].stop_pg_num_changes()

            if config.get('wait-for-scrub', True):
                # wait for pgs to become active+clean in case any
                # recoveries were triggered since the last health check
                ctx.managers[config['cluster']].wait_for_clean()
                osd_scrub_pgs(ctx, config)

            # stop logging health to clog during shutdown, or else we generate
            # a bunch of scary messages unrelated to our actual run.
            firstmon = teuthology.get_first_mon(ctx, config, config['cluster'])
            (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
            mon0_remote.run(
                args=[
                    'sudo',
                    'ceph',
                    '--cluster', config['cluster'],
                    'config', 'set', 'global',
                    'mon_health_to_clog', 'false',
                ],
                check_status=False,
            )
