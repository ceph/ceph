from cStringIO import StringIO

import argparse
import contextlib
import logging
import os
import sys

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

class DaemonState(object):
    def __init__(self, remote, role, id_, *command_args, **command_kwargs):
        self.remote = remote
        self.command_args = command_args
        self.command_kwargs = command_kwargs
        self.role = role
        self.id_ = id_
        self.log = command_kwargs.get('logger', log)
        self.proc = None

    def stop(self):
        """
        Note: this can raise a run.CommandFailedError,
        run.CommandCrashedError, or run.ConnectionLostError.
        """
        if not self.running():
            self.log.error('tried to stop a non-running daemon')
            return
        self.proc.stdin.close()
        self.log.debug('waiting for process to exit')
        run.wait([self.proc])
        self.proc = None
        self.log.info('Stopped')

    def restart(self, *args, **kwargs):
        self.log.info('Restarting')
        if self.proc is not None:
            self.log.debug('stopping old one...')
            self.stop()
        cmd_args = list(self.command_args)
        cmd_args.extend(args)
        cmd_kwargs = self.command_kwargs
        cmd_kwargs.update(kwargs)
        self.proc = self.remote.run(*cmd_args, **cmd_kwargs)
        self.log.info('Started')

    def running(self):
        return self.proc is not None

    def reset(self):
        self.proc = None


class CephState(object):
    def __init__(self):
        self.daemons = {}

    def add_daemon(self, remote, role, id_, *args, **kwargs):
        if role not in self.daemons:
            self.daemons[role] = {}
        if id_ in self.daemons[role]:
            self.daemons[role][id_].stop()
            self.daemons[role][id_] = None
        self.daemons[role][id_] = DaemonState(remote, role, id_, *args, **kwargs)
        self.daemons[role][id_].restart()

    def get_daemon(self, role, id_):
        if role not in self.daemons:
            return None
        return self.daemons[role].get(str(id_), None)

    def iter_daemons_of_role(self, role):
        return self.daemons.get(role, {}).values()

@contextlib.contextmanager
def ceph_log(ctx, config):
    log.info('Creating log directories...')
    archive_dir = '{tdir}/archive'.format(tdir=teuthology.get_testdir(ctx))
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '{adir}/log'.format(adir=archive_dir),
                '{adir}/log/valgrind'.format(adir=archive_dir),
                '{adir}/profiling-logger'.format(adir=archive_dir),
                ],
            wait=False,
            )
        )

    try:
        yield
    finally:

        if ctx.archive is not None:
            log.info('Compressing logs...')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'find',
                        '{adir}/log'.format(adir=archive_dir),
                        '-name',
                        '*.log',
                        '-print0',
                        run.Raw('|'),
                        'xargs',
                        '-0',
                        '--no-run-if-empty',
                        '--',
                        'gzip',
                        '--',
                        ],
                    wait=False,
                    ),
                )

            # log file transfer is done by the generic archive data
            # handling

@contextlib.contextmanager
def ship_utilities(ctx, config):
    assert config is None
    FILES = ['daemon-helper', 'enable-coredump', 'chdir-coredump',
             'valgrind.supp', 'kcon_most']
    testdir = teuthology.get_testdir(ctx)
    for filename in FILES:
        log.info('Shipping %r...', filename)
        src = os.path.join(os.path.dirname(__file__), filename)
        dst = os.path.join(testdir, filename)
        with file(src, 'rb') as f:
            for rem in ctx.cluster.remotes.iterkeys():
                teuthology.write_file(
                    remote=rem,
                    path=dst,
                    data=f,
                    )
                f.seek(0)
                rem.run(
                    args=[
                        'chmod',
                        'a=rx',
                        '--',
                        dst,
                        ],
                    )

    try:
        yield
    finally:
        log.info('Removing shipped files: %s...', ' '.join(FILES))
        filenames = (
            os.path.join(testdir, filename)
            for filename in FILES
            )
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '--',
                    ] + list(filenames),
                wait=False,
                ),
            )

def _update_deb_package_list_and_install(remote, debs, branch):
    """
    updates the package list so that apt-get can
    download the appropriate packages
    """

    # check for ceph release key
    r = remote.run(
        args=[
            'sudo', 'apt-key', 'list', run.Raw('|'), 'grep', 'Ceph',
            ],
        stdout=StringIO(),
        )
    if r.stdout.getvalue().find('Ceph automated package') == -1:
        # if it doesn't exist, add it
        remote.run(
                args=[
                    'wget', '-q', '-O-',
                    'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',
                    run.Raw('|'),
                    'sudo', 'apt-key', 'add', '-',
                    ],
                stdout=StringIO(),
                )

    # get ubuntu release (precise, quantal, etc.)
    r = remote.run(
            args=['lsb_release', '-sc'],
            stdout=StringIO(),
            )

    out = r.stdout.getvalue().strip()
    log.info("release type:" + out)

    remote.run(
        args=[
            'echo', 'deb',
            'http://gitbuilder.ceph.com/ceph-deb-' + out + '-x86_64-basic/ref/' + branch,
            out, 'main', run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/ceph.list'
            ],
        stdout=StringIO(),
        )
    remote.run(
        args=[
            'sudo', 'apt-get', 'update', run.Raw('&&'),
            'sudo', 'apt-get', '-y', '--force-yes',
            'install',
            ] + debs,
        stdout=StringIO(),
        )

def install_debs(ctx, debs, branch):
    """
    installs Debian packages.
    """
    log.info("Installing ceph debian packages: {debs}".format(debs=', '.join(debs)))
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_update_deb_package_list_and_install, remote, debs, branch)

def _remove_deb(remote, debs):
    args=[
        'sudo', 'apt-get', '-y', '--force-yes', 'purge',
        ]
    args.extend(debs)
    args.extend([
            run.Raw('||'),
            'true'
            ])
    remote.run(args=args)
    remote.run(
        args=[
            'sudo', 'apt-get', '-y', '--force-yes',
            'autoremove',
            ],
        stdout=StringIO(),
        )

def remove_debs(ctx, debs):
    log.info("Removing/purging debian packages {debs}".format(debs=', '.join(debs)))
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_remove_deb, remote, debs)

def _remove_sources_list(remote):
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/apt/sources.list.d/ceph.list', run.Raw('&&'),
            'sudo', 'apt-get', 'update',
            ],
        stdout=StringIO(),
       )

def remove_sources(ctx):
    log.info("Removing ceph sources list from apt")
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_remove_sources_list, remote)


@contextlib.contextmanager
def binaries(ctx, config):

    debs = ['ceph',
            'ceph-mds',
            'ceph-common',
            'python-ceph',
            'ceph-test',
            'radosgw',
            'librados2',
            'librbd1',
            ]
    branch = config.get('branch', 'master')
    log.info('branch: {b}'.format(b=branch))
    install_debs(ctx, debs, branch)
    try:
        yield
    finally:
        remove_debs(ctx, debs)
        remove_sources(ctx)

def assign_devs(roles, devs):
    return dict(zip(roles, devs))

@contextlib.contextmanager
def valgrind_post(ctx, config):
    testdir = teuthology.get_testdir(ctx)
    try:
        yield
    finally:
        lookup_procs = list()
        val_path = '{tdir}/archive/log/valgrind'.format(tdir=testdir)
        log.info('Checking for errors in any valgrind logs...');
        for remote in ctx.cluster.remotes.iterkeys():
            #look at valgrind logs for each node
            proc = remote.run(
                args=[
                    'grep', '-r', '<kind>',
                    run.Raw(val_path),
                    run.Raw('|'),
                    'sort',
                    run.Raw('|'),
                    'uniq',
                    ],
                wait = False,
                check_status=False,
                stdout=StringIO(),
                )
            lookup_procs.append((proc, remote))

        valgrind_exception = None
        for (proc, remote) in lookup_procs:
            out = proc.stdout.getvalue()
            for line in out.split('\n'):
                if line == '':
                    continue
                (file, kind) = line.split(':')
                log.debug('file %s kind %s', file, kind)
                if file.find('client') < 0 and file.find('mon') < 0 and kind.find('Lost') > 0:
                    continue
                log.error('saw valgrind issue %s in %s', kind, file)
                valgrind_exception = Exception('saw valgrind issues')

        if valgrind_exception is not None:
            raise valgrind_exception


def mount_osd_data(ctx, remote, osd):
    testdir = teuthology.get_testdir(ctx)
    log.debug('Mounting data for osd.{o} on {r}'.format(o=osd, r=remote))
    if remote in ctx.disk_config.remote_to_roles_to_dev and osd in ctx.disk_config.remote_to_roles_to_dev[remote]:
        dev = ctx.disk_config.remote_to_roles_to_dev[remote][osd]
        journal = ctx.disk_config.remote_to_roles_to_journals[remote][osd]
        mount_options = ctx.disk_config.remote_to_roles_to_dev_mount_options[remote][osd]
        fstype = ctx.disk_config.remote_to_roles_to_dev_fstype[remote][osd]
        mnt = os.path.join('{tdir}/data'.format(tdir=testdir), 'osd.{id}.data'.format(id=osd))

        log.info('Mounting osd.{o}: dev: {n}, mountpoint: {p}, type: {t}, options: {v}'.format(
                 o=osd, n=remote.name, p=mnt, t=fstype, v=mount_options))

        remote.run(
            args=[
                'sudo',
                'mount',
                '-t', fstype,
                '-o', ','.join(mount_options),
                dev,
                mnt,
            ]
            )

        if journal == ('/mnt/osd.%s' % osd):
            tmpfs = '/mnt/osd.%s' % osd
            log.info('Creating journal file on tmpfs at {t}'.format(t=tmpfs))
            remote.run( args=[ 'sudo', 'mount', '-t', 'tmpfs', 'tmpfs', '/mnt' ] )
            remote.run( args=[ 'truncate', '-s', '1500M', tmpfs ] )

@contextlib.contextmanager
def cluster(ctx, config):
    testdir = teuthology.get_testdir(ctx)
    log.info('Creating ceph cluster...')
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '{tdir}/data'.format(tdir=testdir),
                ],
            wait=False,
            )
        )


    devs_to_clean = {}
    remote_to_roles_to_devs = {}
    remote_to_roles_to_journals = {}
    osds = ctx.cluster.only(teuthology.is_type('osd'))
    for remote, roles_for_host in osds.remotes.iteritems():
        devs = teuthology.get_scratch_devices(remote)
        roles_to_devs = {}
        roles_to_journals = {}
        if config.get('fs'):
            log.info('fs option selected, checking for scratch devs')
            log.info('found devs: %s' % (str(devs),))
            devs_id_map = teuthology.get_wwn_id_map(remote, devs)
            iddevs = devs_id_map.values()
            roles_to_devs = assign_devs(
                teuthology.roles_of_type(roles_for_host, 'osd'), iddevs
                )
            if len(roles_to_devs) < len(iddevs):
                iddevs = iddevs[len(roles_to_devs):]
            devs_to_clean[remote] = []

        if config.get('block_journal'):
            log.info('block journal enabled')
            roles_to_journals = assign_devs(
                teuthology.roles_of_type(roles_for_host, 'osd'), iddevs
                )
            log.info('journal map: %s', roles_to_journals)

        if config.get('tmpfs_journal'):
            log.info('tmpfs journal enabled')
            roles_to_journals = {}
            remote.run( args=[ 'sudo', 'mount', '-t', 'tmpfs', 'tmpfs', '/mnt' ] )
            for osd in teuthology.roles_of_type(roles_for_host, 'osd'):
                tmpfs = '/mnt/osd.%s' % osd
                roles_to_journals[osd] = tmpfs
                remote.run( args=[ 'truncate', '-s', '1500M', tmpfs ] )
            log.info('journal map: %s', roles_to_journals)

        log.info('dev map: %s' % (str(roles_to_devs),))
        remote_to_roles_to_devs[remote] = roles_to_devs
        remote_to_roles_to_journals[remote] = roles_to_journals


    log.info('Generating config...')
    remotes_and_roles = ctx.cluster.remotes.items()
    roles = [roles for (remote, roles) in remotes_and_roles]
    ips = [host for (host, port) in (remote.ssh.get_transport().getpeername() for (remote, roles) in remotes_and_roles)]
    conf = teuthology.skeleton_config(ctx, roles=roles, ips=ips)
    for remote, roles_to_journals in remote_to_roles_to_journals.iteritems():
        for role, journal in roles_to_journals.iteritems():
            key = "osd." + str(role)
            if key not in conf:
                conf[key] = {}
            conf[key]['osd journal'] = journal
    for section, keys in config['conf'].iteritems():
        for key, value in keys.iteritems():
            log.info("[%s] %s = %s" % (section, key, value))
            if section not in conf:
                conf[section] = {}
            conf[section][key] = value

    if config.get('tmpfs_journal'):
        conf['journal dio'] = False

    ctx.ceph = argparse.Namespace()
    ctx.ceph.conf = conf

    conf_path = config.get('conf_path', '/etc/ceph/ceph.conf')
    keyring_path = config.get('keyring_path', '/etc/ceph/ceph.keyring')

    log.info('Writing configs...')
    conf_fp = StringIO()
    conf.write(conf_fp)
    conf_fp.seek(0)
    writes = ctx.cluster.run(
        args=[
            'sudo', 'mkdir', '-p', '/etc/ceph', run.Raw('&&'),
            'sudo', 'chmod', '0755', '/etc/ceph', run.Raw('&&'),
            'sudo', 'python',
            '-c',
            'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
            conf_path,
            run.Raw('&&'),
            'sudo', 'chmod', '0644', conf_path,
            ],
        stdin=run.PIPE,
        wait=False,
        )
    teuthology.feed_many_stdins_and_close(conf_fp, writes)
    run.wait(writes)

    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)

    firstmon = teuthology.get_first_mon(ctx, config)

    log.info('Setting up %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            '{tdir}/enable-coredump'.format(tdir=testdir),
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
            '{tdir}/enable-coredump'.format(tdir=testdir),
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
    teuthology.create_simple_monmap(
        ctx,
        remote=mon0_remote,
        conf=conf,
        )

    log.info('Creating admin key on %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            '{tdir}/enable-coredump'.format(tdir=testdir),
            'ceph-coverage',
            coverage_dir,
            'ceph-authtool',
            '--gen-key',
            '--name=client.admin',
            '--set-uid=0',
            '--cap', 'mon', 'allow *',
            '--cap', 'osd', 'allow *',
            '--cap', 'mds', 'allow',
            keyring_path,
            ],
        )

    log.info('Copying monmap to all nodes...')
    keyring = teuthology.get_file(
        remote=mon0_remote,
        path=keyring_path,
        )
    monmap = teuthology.get_file(
        remote=mon0_remote,
        path='{tdir}/monmap'.format(tdir=testdir),
        )

    for rem in ctx.cluster.remotes.iterkeys():
        # copy mon key and initial monmap
        log.info('Sending monmap to node {remote}'.format(remote=rem))
        teuthology.sudo_write_file(
            remote=rem,
            path=keyring_path,
            data=keyring,
            perms='0644'
            )
        teuthology.write_file(
            remote=rem,
            path='{tdir}/monmap'.format(tdir=testdir),
            data=monmap,
            )

    log.info('Setting up mon nodes...')
    mons = ctx.cluster.only(teuthology.is_type('mon'))
    run.wait(
        mons.run(
            args=[
                '{tdir}/enable-coredump'.format(tdir=testdir),
                'ceph-coverage',
                coverage_dir,
                'osdmaptool',
                '-c', conf_path,
                '--clobber',
                '--createsimple', '{num:d}'.format(
                    num=teuthology.num_instances_of_type(ctx.cluster, 'osd'),
                    ),
                '{tdir}/osdmap'.format(tdir=testdir),
                '--pg_bits', '2',
                '--pgp_bits', '4',
                ],
            wait=False,
            ),
        )

    log.info('Setting up osd nodes...')
    for remote, roles_for_host in osds.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            remote.run(
                args=[
                    '{tdir}/enable-coredump'.format(tdir=testdir),
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-authtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=osd.{id}'.format(id=id_),
                    '{tdir}/data/osd.{id}.keyring'.format(tdir=testdir, id=id_),
                    ],
                )

    log.info('Setting up mds nodes...')
    mdss = ctx.cluster.only(teuthology.is_type('mds'))
    for remote, roles_for_host in mdss.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mds'):
            remote.run(
                args=[
                    '{tdir}/enable-coredump'.format(tdir=testdir),
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-authtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=mds.{id}'.format(id=id_),
                    '{tdir}/data/mds.{id}.keyring'.format(tdir=testdir, id=id_),
                    ],
                )

    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client'))
    for remote, roles_for_host in clients.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            client_keyring = '/etc/ceph/ceph.client.{id}.keyring'.format(id=id_)
            remote.run(
                args=[
                    '{tdir}/enable-coredump'.format(tdir=testdir),
                    'ceph-coverage',
                    coverage_dir,
                    'sudo',
                    'ceph-authtool',
                    '--create-keyring',
                    '--gen-key',
                    # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
                    '--name=client.{id}'.format(id=id_),
                    client_keyring,
                    run.Raw('&&'),
                    'sudo',
                    'chmod',
                    '0644',
                    client_keyring,
                    ],
                )

    log.info('Reading keys from all nodes...')
    keys_fp = StringIO()
    keys = []
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for type_ in ['osd', 'mds']:
            for id_ in teuthology.roles_of_type(roles_for_host, type_):
                data = teuthology.get_file(
                    remote=remote,
                    path='{tdir}/data/{type}.{id}.keyring'.format(
                        tdir=testdir,
                        type=type_,
                        id=id_,
                        ),
                    )
                keys.append((type_, id_, data))
                keys_fp.write(data)
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for type_ in ['client']:
            for id_ in teuthology.roles_of_type(roles_for_host, type_):
                data = teuthology.get_file(
                    remote=remote,
                    path='/etc/ceph/ceph.client.{id}.keyring'.format(id=id_)
                    )
                keys.append((type_, id_, data))
                keys_fp.write(data)

    log.info('Adding keys to all mons...')
    writes = mons.run(
        args=[
            'sudo', 'tee', '-a',
            keyring_path,
            ],
        stdin=run.PIPE,
        wait=False,
        stdout=StringIO(),
        )
    keys_fp.seek(0)
    teuthology.feed_many_stdins_and_close(keys_fp, writes)
    run.wait(writes)
    for type_, id_, data in keys:
        run.wait(
            mons.run(
                args=[
                    'sudo',
                    '{tdir}/enable-coredump'.format(tdir=testdir),
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-authtool',
                    keyring_path,
                    '--name={type}.{id}'.format(
                        type=type_,
                        id=id_,
                        ),
                    ] + list(teuthology.generate_caps(type_)),
                wait=False,
                ),
            )

    log.info('Running mkfs on mon nodes...')
    for remote, roles_for_host in mons.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mon'):
            remote.run(
                args=[
                    '{tdir}/enable-coredump'.format(tdir=testdir),
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-mon',
                    '--mkfs',
                    '-i', id_,
                    '--monmap={tdir}/monmap'.format(tdir=testdir),
                    '--osdmap={tdir}/osdmap'.format(tdir=testdir),
                    '--keyring={kpath}'.format(kpath=keyring_path),
                    ],
                )

    log.info('Running mkfs on osd nodes...')
    for remote, roles_for_host in osds.remotes.iteritems():
        roles_to_devs = remote_to_roles_to_devs[remote]
        roles_to_journals = remote_to_roles_to_journals[remote]
        ctx.disk_config = argparse.Namespace()
        ctx.disk_config.remote_to_roles_to_dev = remote_to_roles_to_devs
        ctx.disk_config.remote_to_roles_to_journals = remote_to_roles_to_journals
        ctx.disk_config.remote_to_roles_to_dev_mount_options = {}
        ctx.disk_config.remote_to_roles_to_dev_fstype = {}


        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            log.info(str(roles_to_journals))
            log.info(id_)
            remote.run(
                args=[
                    'mkdir',
                    os.path.join('{tdir}/data'.format(tdir=testdir), 'osd.{id}.data'.format(id=id_)),
                    ],
                )
            if roles_to_devs.get(id_):
                dev = roles_to_devs[id_]
                fs = config.get('fs')
                package = None
                mkfs_options = config.get('mkfs_options')
                mount_options = config.get('mount_options')
                if fs == 'btrfs':
                    package = 'btrfs-tools'
                    if mount_options is None:
                        mount_options = ['noatime','user_subvol_rm_allowed']
                    if mkfs_options is None:
                        mkfs_options = ['-m', 'single',
                                        '-l', '32768',
                                        '-n', '32768']
                if fs == 'xfs':
                    package = 'xfsprogs'
                    if mount_options is None:
                        mount_options = ['noatime']
                    if mkfs_options is None:
                        mkfs_options = ['-f', '-i', 'size=2048']
                if fs == 'ext4' or fs == 'ext3':
                    if mount_options is None:
                        mount_options = ['noatime','user_xattr']

                if mount_options is None:
                    mount_options = []
                if mkfs_options is None:
                    mkfs_options = []
                mkfs = ['mkfs.%s' % fs] + mkfs_options
                log.info('%s on %s on %s' % (mkfs, dev, remote))
                if package is not None:
                    remote.run(
                        args=[
                            'sudo',
                            'apt-get', 'install', '-y', package
                            ],
                        stdout=StringIO(),
                        )
                remote.run(args= ['yes', run.Raw('|')] + ['sudo'] + mkfs + [dev])
                log.info('mount %s on %s -o %s' % (dev, remote,
                                                   ','.join(mount_options)))
                remote.run(
                    args=[
                        'sudo',
                        'mount',
                        '-t', fs,
                        '-o', ','.join(mount_options),
                        dev,
                        os.path.join('{tdir}/data'.format(tdir=testdir), 'osd.{id}.data'.format(id=id_)),
                        ]
                    )
                if not remote in ctx.disk_config.remote_to_roles_to_dev_mount_options:
                    ctx.disk_config.remote_to_roles_to_dev_mount_options[remote] = {}
                ctx.disk_config.remote_to_roles_to_dev_mount_options[remote][id_] = mount_options
                if not remote in ctx.disk_config.remote_to_roles_to_dev_fstype:
                    ctx.disk_config.remote_to_roles_to_dev_fstype[remote] = {}
                ctx.disk_config.remote_to_roles_to_dev_fstype[remote][id_] = fs
                remote.run(
                    args=[
                        'sudo', 'chown', '-R', 'ubuntu.ubuntu',
                        os.path.join('{tdir}/data'.format(tdir=testdir), 'osd.{id}.data'.format(id=id_))
                        ]
                    )
                remote.run(
                    args=[
                        'sudo', 'chmod', '-R', '755',
                        os.path.join('{tdir}/data'.format(tdir=testdir), 'osd.{id}.data'.format(id=id_))
                        ]
                    )
                devs_to_clean[remote].append(
                    os.path.join(
                        '{tdir}/data'.format(tdir=testdir), 'osd.{id}.data'.format(id=id_)
                        )
                    )

        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            remote.run(
                args=[
                    'MALLOC_CHECK_=3',
                    '{tdir}/enable-coredump'.format(tdir=testdir),
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-osd',
                    '--mkfs',
                    '-i', id_,
                    '--monmap', '{tdir}/monmap'.format(tdir=testdir),
                    ],
                )
    run.wait(
        mons.run(
            args=[
                'rm',
                '--',
                '{tdir}/monmap'.format(tdir=testdir),
                '{tdir}/osdmap'.format(tdir=testdir),
                ],
            wait=False,
            ),
        )

    try:
        yield
    finally:
        (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()

        log.info('Checking cluster log for badness...')
        def first_in_ceph_log(pattern, excludes):
            args = [
                'egrep', pattern,
                '%s/archive/log/cluster.%s.log' % (testdir, firstmon),
                ]
            for exclude in excludes:
                args.extend([run.Raw('|'), 'egrep', '-v', exclude])
            args.extend([
                    run.Raw('|'), 'head', '-n', '1',
                    ])
            r = mon0_remote.run(
                stdout=StringIO(),
                args=args,
                )
            stdout = r.stdout.getvalue()
            if stdout != '':
                return stdout
            return None

        if first_in_ceph_log('\[ERR\]|\[WRN\]|\[SEC\]',
                             config['log_whitelist']) is not None:
            log.warning('Found errors (ERR|WRN|SEC) in cluster log')
            ctx.summary['success'] = False
            # use the most severe problem as the failure reason
            if 'failure_reason' not in ctx.summary:
                for pattern in ['\[SEC\]', '\[ERR\]', '\[WRN\]']:
                    match = first_in_ceph_log(pattern, config['log_whitelist'])
                    if match is not None:
                        ctx.summary['failure_reason'] = \
                            '"{match}" in cluster log'.format(
                            match=match.rstrip('\n'),
                            )
                        break

        for remote, dirs in devs_to_clean.iteritems():
            for dir_ in dirs:
                log.info('Unmounting %s on %s' % (dir_, remote))
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

        if config.get('tmpfs_journal'):
            log.info('tmpfs journal enabled - unmounting tmpfs at /mnt')
            for remote, roles_for_host in osds.remotes.iteritems():
                remote.run(
                    args=[ 'sudo', 'umount', '-f', '/mnt' ],
                    check_status=False,
                )

        if ctx.archive is not None:
            # archive mon data, too
            log.info('Archiving mon data...')
            path = os.path.join(ctx.archive, 'data')
            os.makedirs(path)
            for remote, roles in mons.remotes.iteritems():
                for role in roles:
                    if role.startswith('mon.'):
                        teuthology.pull_directory_tarball(remote,
                                       '%s/data/%s' % (testdir, role),
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
                    '{tdir}/data'.format(tdir=testdir),
                    '{tdir}/monmap'.format(tdir=testdir),
                    run.Raw('{tdir}/asok.*'.format(tdir=testdir))
                    ],
                wait=False,
                ),
            )


@contextlib.contextmanager
def run_daemon(ctx, config, type_):
    log.info('Starting %s daemons...' % type_)
    testdir = teuthology.get_testdir(ctx)
    daemons = ctx.cluster.only(teuthology.is_type(type_))
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)

    daemon_signal = 'kill'
    if config.get('coverage') or config.get('valgrind') is not None:
        daemon_signal = 'term'

    num_active = 0
    for remote, roles_for_host in daemons.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, type_):
            name = '%s.%s' % (type_, id_)

            if not (id_.endswith('-s')) and (id_.find('-s-') == -1):
                num_active += 1

            run_cmd = [
                '{tdir}/enable-coredump'.format(tdir=testdir),
                'ceph-coverage',
                coverage_dir,
                '{tdir}/daemon-helper'.format(tdir=testdir),
                daemon_signal,
                ]
            run_cmd_tail = [
                'ceph-%s' % (type_),
                '-f',
                '-i', id_]

            if config.get('valgrind') is not None:
                valgrind_args = None
                if type_ in config['valgrind']:
                    valgrind_args = config['valgrind'][type_]
                if name in config['valgrind']:
                    valgrind_args = config['valgrind'][name]
                run_cmd.extend(teuthology.get_valgrind_args(testdir, name, valgrind_args))

            if type_ in config.get('cpu_profile', []):
                profile_path = '%s/archive/log/%s.%s.prof' % (testdir, type_, id_)
                run_cmd.extend([ 'env', 'CPUPROFILE=%s' % profile_path ])

            run_cmd.extend(run_cmd_tail)

            ctx.daemons.add_daemon(remote, type_, id_,
                                   args=run_cmd,
                                   logger=log.getChild(name),
                                   stdin=run.PIPE,
                                   wait=False,
                                   )

    if type_ == 'mds':
        firstmon = teuthology.get_first_mon(ctx, config)
        (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()

        mon0_remote.run(args=[
            '{tdir}/enable-coredump'.format(tdir=testdir),
            'ceph-coverage',
            coverage_dir,
            'ceph',
            'mds', 'set_max_mds', str(num_active)])

    try:
        yield
    finally:
        log.info('Shutting down %s daemons...' % type_)
        exc_info = (None, None, None)
        for daemon in ctx.daemons.iter_daemons_of_role(type_):
            try:
                daemon.stop()
            except (run.CommandFailedError,
                    run.CommandCrashedError,
                    run.ConnectionLostError):
                exc_info = sys.exc_info()
                log.exception('Saw exception from %s.%s', daemon.role, daemon.id_)
        if exc_info != (None, None, None):
            raise exc_info[0], exc_info[1], exc_info[2]

def healthy(ctx, config):
    log.info('Waiting until ceph is healthy...')
    firstmon = teuthology.get_first_mon(ctx, config)
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    teuthology.wait_until_osds_up(
        ctx,
        cluster=ctx.cluster,
        remote=mon0_remote
        )
    teuthology.wait_until_healthy(
        ctx,
        remote=mon0_remote,
        )


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
            log-whitelist: ['foo.*bar', 'bad message']

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))

    ctx.daemons = CephState()

    # Flavor tells us what gitbuilder to fetch the prebuilt software
    # from. It's a combination of possible keywords, in a specific
    # order, joined by dashes. It is used as a URL path name. If a
    # match is not found, the teuthology run fails. This is ugly,
    # and should be cleaned up at some point.

    dist = 'precise'
    format = 'tarball'
    arch = 'x86_64'
    flavor = 'basic'

    # First element: controlled by user (or not there, by default):
    # used to choose the right distribution, e.g. "oneiric".
    flavor = config.get('flavor', 'basic')

    if config.get('path'):
        # local dir precludes any other flavors
        flavor = 'local'
    else:
        if config.get('valgrind'):
            log.info('Using notcmalloc flavor and running some daemons under valgrind')
            flavor = 'notcmalloc'
        else:
            if config.get('coverage'):
                log.info('Recording coverage for this run.')
                flavor = 'gcov'

    ctx.summary['flavor'] = flavor

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

    with contextutil.nested(
        lambda: ceph_log(ctx=ctx, config=None),
        lambda: ship_utilities(ctx=ctx, config=None),
        lambda: binaries(ctx=ctx, config=dict(
                branch=config.get('branch', 'master'),
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                flavor=flavor,
                dist=config.get('dist', dist),
                format=format,
                arch=arch
                )),
        lambda: valgrind_post(ctx=ctx, config=config),
        lambda: cluster(ctx=ctx, config=dict(
                conf=config.get('conf', {}),
                fs=config.get('fs', None),
                mkfs_options=config.get('mkfs_options', None),
                mount_options=config.get('mount_options',None),
                block_journal=config.get('block_journal', None),
                tmpfs_journal=config.get('tmpfs_journal', None),
                log_whitelist=config.get('log-whitelist', []),
                cpu_profile=set(config.get('cpu_profile', [])),
                )),
        lambda: run_daemon(ctx=ctx, config=config, type_='mon'),
        lambda: run_daemon(ctx=ctx, config=config, type_='osd'),
        lambda: run_daemon(ctx=ctx, config=config, type_='mds'),
        ):
        if config.get('wait-for-healthy', True):
          healthy(ctx=ctx, config=None)
        yield
