from cStringIO import StringIO

import contextlib
import errno
import logging
import os
import shutil
import subprocess
import tempfile

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def ceph_log(ctx, config):
    log.info('Creating log directories...')
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '/tmp/cephtest/archive/log',
                '/tmp/cephtest/archive/profiling-logger',
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
                        '/tmp/cephtest/archive/log',
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
    FILES = ['daemon-helper', 'enable-coredump']
    for filename in FILES:
        log.info('Shipping %r...', filename)
        src = os.path.join(os.path.dirname(__file__), filename)
        dst = os.path.join('/tmp/cephtest', filename)
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
            os.path.join('/tmp/cephtest', filename)
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

def _download_binaries(remote, ceph_bindir_url):
    remote.run(
        args=[
            'install', '-d', '-m0755', '--', '/tmp/cephtest/binary',
            run.Raw('&&'),
            'uname', '-m',
            run.Raw('|'),
            'sed', '-e', 's/^/ceph./; s/$/.tgz/',
            run.Raw('|'),
            'wget',
            '-nv',
            '-O-',
            '--base={url}'.format(url=ceph_bindir_url),
            # need to use --input-file to make wget respect --base
            '--input-file=-',
            run.Raw('|'),
            'tar', '-xzf', '-', '-C', '/tmp/cephtest/binary',
            ],
        )

@contextlib.contextmanager
def binaries(ctx, config):
    path = config.get('path')
    tmpdir = None

    if path is None:
        # fetch from gitbuilder gitbuilder
        log.info('Fetching and unpacking ceph binaries from gitbuilder...')
        sha1, ceph_bindir_url = teuthology.get_ceph_binary_url(
            branch=config.get('branch'),
            tag=config.get('tag'),
            sha1=config.get('sha1'),
            flavor=config.get('flavor'),
            )
        ctx.summary['ceph-sha1'] = sha1
        if ctx.archive is not None:
            with file(os.path.join(ctx.archive, 'ceph-sha1'), 'w') as f:
                f.write(sha1 + '\n')

        with parallel() as p:
            for remote in ctx.cluster.remotes.iterkeys():
                p.spawn(_download_binaries, remote, ceph_bindir_url)
    else:
        with tempfile.TemporaryFile(prefix='teuthology-tarball-', suffix='.tgz') as tar_fp:
            tmpdir = tempfile.mkdtemp(prefix='teuthology-tarball-')
            try:
                log.info('Installing %s to %s...' % (path, tmpdir))
                subprocess.check_call(
                    args=[
                        'make',
                        'install',
                        'DESTDIR={tmpdir}'.format(tmpdir=tmpdir),
                        ],
                    cwd=path,
                    )
                try:
                    os.symlink('.', os.path.join(tmpdir, 'usr', 'local'))
                except OSError as e:
                    if e.errno == errno.EEXIST:
                        pass
                    else:
                        raise
                log.info('Building ceph binary tarball from %s...', tmpdir)
                subprocess.check_call(
                    args=[
                        'tar',
                        'cz',
                        '.',
                        ],
                    cwd=tmpdir,
                    stdout=tar_fp,
                    )
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
            log.info('Pushing tarball...')
            tar_fp.seek(0)
            writes = ctx.cluster.run(
                args=[
                    'install', '-d', '-m0755', '--', '/tmp/cephtest/binary',
                    run.Raw('&&'),
                    'tar', '-xzf', '-', '-C', '/tmp/cephtest/binary'
                    ],
                stdin=run.PIPE,
                wait=False,
                )
            teuthology.feed_many_stdins_and_close(tar_fp, writes)
            run.wait(writes)

    try:
        yield
    finally:
        log.info('Removing ceph binaries...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '--',
                    '/tmp/cephtest/binary',
                    ],
                wait=False,
                ),
            )

@contextlib.contextmanager
def valgrind_post(ctx, config):
    try:
        yield
    finally:
        if config.get('valgrind'):
            lookup_procs = list()
            val_path = '/tmp/cephtest/archive/log/{val_dir}/*'.format(
                val_dir=config.get('valgrind').get('logs', "valgrind"))
            for remote in ctx.cluster.remotes.iterkeys():
                #look at valgrind logs for each node
                proc = remote.run(
                    args=[
                        'grep', "<kind>", run.Raw(val_path), run.Raw('|'),
                        'grep', '-v', '-q', "PossiblyLost"],
                    wait = False,
                    check_status=False
                    )
                lookup_procs.append((proc, remote))

            valgrind_exception = None
            for (proc, remote) in lookup_procs:
                result = proc.exitstatus.get()
                if result is not 1:
                    valgrind_exception = Exception("saw valgrind issues in {node}".format(node=remote.name))

            if valgrind_exception is not None:
                raise valgrind_exception
                

@contextlib.contextmanager
def cluster(ctx, config):
    log.info('Creating ceph cluster...')
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '/tmp/cephtest/data',
                ],
            wait=False,
            )
        )

    log.info('Generating config...')
    remotes_and_roles = ctx.cluster.remotes.items()
    roles = [roles for (remote, roles) in remotes_and_roles]
    ips = [host for (host, port) in (remote.ssh.get_transport().getpeername() for (remote, roles) in remotes_and_roles)]
    conf = teuthology.skeleton_config(roles=roles, ips=ips)
    for section, keys in config['conf'].iteritems():
        for key, value in keys.iteritems():
            log.info("[%s] %s = %s" % (section, key, value))
            if section not in conf:
                conf[section] = {}
            conf[section][key] = value

    log.info('Writing configs...')
    conf_fp = StringIO()
    conf.write(conf_fp)
    conf_fp.seek(0)
    writes = ctx.cluster.run(
        args=[
            'python',
            '-c',
            'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
            '/tmp/cephtest/ceph.conf',
            ],
        stdin=run.PIPE,
        wait=False,
        )
    teuthology.feed_many_stdins_and_close(conf_fp, writes)
    run.wait(writes)

    coverage_dir = '/tmp/cephtest/archive/coverage'

    firstmon = teuthology.get_first_mon(ctx, config)

    log.info('Setting up %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            coverage_dir,
            '/tmp/cephtest/binary/usr/local/bin/cauthtool',
            '--create-keyring',
            '/tmp/cephtest/ceph.keyring',
            ],
        )
    ctx.cluster.only(firstmon).run(
        args=[
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            coverage_dir,
            '/tmp/cephtest/binary/usr/local/bin/cauthtool',
            '--gen-key',
            '--name=mon.',
            '/tmp/cephtest/ceph.keyring',
            ],
        )
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    teuthology.create_simple_monmap(
        remote=mon0_remote,
        conf=conf,
        )

    log.info('Creating admin key on %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            coverage_dir,
            '/tmp/cephtest/binary/usr/local/bin/cauthtool',
            '--gen-key',
            '--name=client.admin',
            '--set-uid=0',
            '--cap', 'mon', 'allow *',
            '--cap', 'osd', 'allow *',
            '--cap', 'mds', 'allow',
            '/tmp/cephtest/ceph.keyring',
            ],
        )

    log.info('Copying monmap to all nodes...')
    keyring = teuthology.get_file(
        remote=mon0_remote,
        path='/tmp/cephtest/ceph.keyring',
        )
    monmap = teuthology.get_file(
        remote=mon0_remote,
        path='/tmp/cephtest/monmap',
        )

    for rem in ctx.cluster.remotes.iterkeys():
        # copy mon key and initial monmap
        log.info('Sending monmap to node {remote}'.format(remote=rem))
        teuthology.write_file(
            remote=rem,
            path='/tmp/cephtest/ceph.keyring',
            data=keyring,
            )
        teuthology.write_file(
            remote=rem,
            path='/tmp/cephtest/monmap',
            data=monmap,
            )

    log.info('Setting up mon nodes...')
    mons = ctx.cluster.only(teuthology.is_type('mon'))
    run.wait(
        mons.run(
            args=[
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                coverage_dir,
                '/tmp/cephtest/binary/usr/local/bin/osdmaptool',
                '--clobber',
                '--createsimple', '{num:d}'.format(
                    num=teuthology.num_instances_of_type(ctx.cluster, 'osd'),
                    ),
                '/tmp/cephtest/osdmap',
                '--pg_bits', '2',
                '--pgp_bits', '4',
                ],
            wait=False,
            ),
        )

    log.info('Setting up osd nodes...')
    osds = ctx.cluster.only(teuthology.is_type('osd'))
    for remote, roles_for_host in osds.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            remote.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=osd.{id}'.format(id=id_),
                    '/tmp/cephtest/data/osd.{id}.keyring'.format(id=id_),
                    ],
                )

    log.info('Setting up mds nodes...')
    mdss = ctx.cluster.only(teuthology.is_type('mds'))
    for remote, roles_for_host in mdss.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mds'):
            remote.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=mds.{id}'.format(id=id_),
                    '/tmp/cephtest/data/mds.{id}.keyring'.format(id=id_),
                    ],
                )

    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client'))
    for remote, roles_for_host in clients.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            remote.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                    '--create-keyring',
                    '--gen-key',
                    # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
                    '--name=client.{id}'.format(id=id_),
                    '/tmp/cephtest/data/client.{id}.keyring'.format(id=id_),
                    ],
                )

    log.info('Reading keys from all nodes...')
    keys_fp = StringIO()
    keys = []
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for type_ in ['osd', 'mds', 'client']:
            for id_ in teuthology.roles_of_type(roles_for_host, type_):
                data = teuthology.get_file(
                    remote=remote,
                    path='/tmp/cephtest/data/{type}.{id}.keyring'.format(
                        type=type_,
                        id=id_,
                        ),
                    )
                keys.append((type_, id_, data))
                keys_fp.write(data)

    log.info('Adding keys to all mons...')
    writes = mons.run(
        args=[
            'cat',
            run.Raw('>>'),
            '/tmp/cephtest/ceph.keyring',
            ],
        stdin=run.PIPE,
        wait=False,
        )
    keys_fp.seek(0)
    teuthology.feed_many_stdins_and_close(keys_fp, writes)
    run.wait(writes)
    for type_, id_, data in keys:
        run.wait(
            mons.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                    '/tmp/cephtest/ceph.keyring',
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
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/binary/usr/local/bin/cmon',
                    '--mkfs',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf',
                    '--monmap=/tmp/cephtest/monmap',
                    '--osdmap=/tmp/cephtest/osdmap',
                    '--keyring=/tmp/cephtest/ceph.keyring',
                    ],
                )

    log.info('Running mkfs on osd nodes...')
    for remote, roles_for_host in osds.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            remote.run(
                args=[
                    'mkdir',
                    os.path.join('/tmp/cephtest/data', 'osd.{id}.data'.format(id=id_)),
                    ],
                )
            remote.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/binary/usr/local/bin/cosd',
                    '--mkfs',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf',
                    '--monmap', '/tmp/cephtest/monmap',
                    ],
                )
    run.wait(
        mons.run(
            args=[
                'rm',
                '--',
                '/tmp/cephtest/monmap',
                '/tmp/cephtest/osdmap',
                ],
            wait=False,
            ),
        )

    try:
        yield
    finally:
        (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
        if ctx.archive is not None:
            log.info('Grabbing cluster log from %s %s...' % (mon0_remote,
                                                             firstmon))
            dest = os.path.join(ctx.archive, 'ceph.log')
            mon0_remote.run(
                args = [
                    'cat',
                    '--',
                    '/tmp/cephtest/data/%s/log' % firstmon
                    ],
                stdout=file(dest, 'wb'),
                )

        log.info('Checking cluster ceph.log for badness...')
        r = mon0_remote.run(args=[
                'if', run.Raw('!'),
                'egrep', '-q', '\[ERR\]|\[WRN\]|\[SEC\]',
                '/tmp/cephtest/data/%s/log' % firstmon,
                run.Raw(';'), 'then', 'echo', 'OK', run.Raw(';'),
                'fi',
                ],
                stdout=StringIO(),
                )
        if r.stdout.getvalue() != "OK\n":
            log.warning('Found errors (ERR|WRN|SEC) in cluster log')
            ctx.summary['success'] = False

        log.info('Cleaning ceph cluster...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '--',
                    '/tmp/cephtest/ceph.conf',
                    '/tmp/cephtest/ceph.keyring',
                    '/tmp/cephtest/data',
                    '/tmp/cephtest/monmap',
                    run.Raw('/tmp/cephtest/asok.*')
                    ],
                wait=False,
                ),
            )


@contextlib.contextmanager
def mon(ctx, config):
    log.info('Starting mon daemons...')
    mon_daemons = {}
    mons = ctx.cluster.only(teuthology.is_type('mon'))
    coverage_dir = '/tmp/cephtest/archive/coverage'

    daemon_signal = 'kill'
    if config.get('coverage'):
        log.info('Recording coverage for this run.')
        daemon_signal = 'term'

    for remote, roles_for_host in mons.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mon'):
            proc_signal = daemon_signal
            run_cmd = ['/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/daemon-helper'
                       ]

            run_cmd_tail = ['/tmp/cephtest/binary/usr/local/bin/cmon',
                    '-f',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf']

            extra_args = None

            if config.get('valgrind') and (config.get('valgrind').get('mon.{id}'.format(id=id_), None) is not None):
                valgrind_args = config.get('valgrind').get('mon.{id}'.format(id=id_))
                log.debug('running mon.{id} under valgrind'.format(id=id_))
                val_path = '/tmp/cephtest/archive/log/{val_dir}'.format(val_dir=config.get('valgrind').get('logs', "valgrind"))
                proc_signal = 'term'
                if 'memcheck' in valgrind_args or \
                        'helgrind' in valgrind_args:
                    extra_args = ['valgrind', '--xml=yes', '--xml-file={vdir}/mon.{id}.log'.format(vdir=val_path, id=id_), valgrind_args]
                else:
                    extra_args = extra_args = ['valgrind', '--log-file={vdir}/mon.{id}.log'.format(vdir=val_path, id=id_), valgrind_args]

            run_cmd.append(proc_signal)
            if extra_args is not None:
                run_cmd.extend(extra_args)
            run_cmd.extend(run_cmd_tail)
            proc = remote.run(
                args=run_cmd,
                logger=log.getChild('mon.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
            mon_daemons[id_] = proc

    try:
        yield
    finally:
        log.info('Shutting down mon daemons...')
        for id_, proc in mon_daemons.iteritems():
            proc.stdin.close()

        run.wait(mon_daemons.itervalues())


@contextlib.contextmanager
def osd(ctx, config):
    log.info('Starting osd daemons...')
    osd_daemons = {}
    osds = ctx.cluster.only(teuthology.is_type('osd'))
    coverage_dir = '/tmp/cephtest/archive/coverage'

    daemon_signal = 'kill'
    if config.get('coverage'):
        log.info('Recording coverage for this run.')
        daemon_signal = 'term'

    for remote, roles_for_host in osds.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            run_cmd = ['/tmp/cephtest/enable-coredump',
                       '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                       coverage_dir,
                       '/tmp/cephtest/daemon-helper'
                       ]
            proc_signal = daemon_signal
            run_cmd_tail = [ '/tmp/cephtest/binary/usr/local/bin/cosd',
                             '-f',
                             '-i', id_,
                             '-c', '/tmp/cephtest/ceph.conf',
                             ]
            
            extra_args = None

            if config.get('valgrind') and config.get('valgrind').get('osd.{id}'.format(id=id_), None) is not None:
                valgrind_args = config.get('valgrind').get('osd.{id}'.format(id=id_))
                log.debug('running osd.{id} under valgrind'.format(id=id_))
                val_path = '/tmp/cephtest/archive/log/{val_dir}'.format(val_dir=config.get('valgrind').get('logs', "valgrind"))
                proc_signal = 'term'
                if 'memcheck' in valgrind_args or \
                        'helgrind' in valgrind_args:
                    extra_args = ['valgrind', '--xml=yes', '--xml-file={vdir}/osd.{id}.log'.format(vdir=val_path, id=id_), valgrind_args]
                else:
                    extra_args = extra_args = ['valgrind', '--log-file={vdir}/osd.{id}.log'.format(vdir=val_path, id=id_), valgrind_args]

            run_cmd.append(proc_signal)
            if extra_args is not None:
                run_cmd.extend(extra_args)
            run_cmd.extend(run_cmd_tail)
            proc = remote.run(
                args=run_cmd,
                logger=log.getChild('osd.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
            osd_daemons[id_] = proc

    try:
        yield
    finally:
        log.info('Shutting down osd daemons...')
        for id_, proc in osd_daemons.iteritems():
            proc.stdin.close()

        run.wait(osd_daemons.itervalues())


@contextlib.contextmanager
def mds(ctx, config):
    log.info('Starting mds daemons...')
    firstmon = teuthology.get_first_mon(ctx, config)
    mds_daemons = {}
    mdss = ctx.cluster.only(teuthology.is_type('mds'))
    coverage_dir = '/tmp/cephtest/archive/coverage'

    daemon_signal = 'kill'
    if config.get('coverage'):
        log.info('Recording coverage for this run.')
        daemon_signal = 'term'

    num_active = 0
    for remote, roles_for_host in mdss.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mds'):
            if not id_.endswith('-s'):
                num_active += 1
            run_cmd = ['/tmp/cephtest/enable-coredump',
                       '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                       coverage_dir,
                       '/tmp/cephtest/daemon-helper'
                       ]
            proc_signal = daemon_signal
            run_cmd_tail = ['/tmp/cephtest/binary/usr/local/bin/cmds',
                            '-f',
                            '-i', id_,
                            '-c', '/tmp/cephtest/ceph.conf',
                            ]

            extra_args = None

            if config.get('valgrind') and (config.get('valgrind').get('mds.{id}'.format(id=id_), None) is not None):
                valgrind_args = config.get('valgrind').get('mds.{id}'.format(id=id_))
                log.debug('running mds.{id} under valgrind'.format(id=id_))
                val_path = '/tmp/cephtest/archive/log/{val_dir}'.format(val_dir=config.get('valgrind').get('logs', "valgrind"))
                proc_signal = 'term'
                if 'memcheck' in valgrind_args or \
                        'helgrind' in valgrind_args:
                    extra_args = ['valgrind', '--xml=yes', '--xml-file={vdir}/mds.{id}.log'.format(vdir=val_path, id=id_), valgrind_args]
                else:
                    extra_args = extra_args = ['valgrind', '--log-file={vdir}/mds.{id}.log'.format(vdir=val_path, id=id_), valgrind_args]

            run_cmd.append(proc_signal)
            if extra_args is not None:
                run_cmd.extend(extra_args)
            run_cmd.extend(run_cmd_tail)
            proc = remote.run(
                args=run_cmd,
                logger=log.getChild('mds.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
            mds_daemons[id_] = proc

    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    mon0_remote.run(args=[
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            coverage_dir,
            '/tmp/cephtest/binary/usr/local/bin/ceph',
            '-c', '/tmp/cephtest/ceph.conf',
            'mds', 'set_max_mds', str(num_active)])

    try:
        yield
    finally:
        log.info('Shutting down mds daemons...')
        for id_, proc in mds_daemons.iteritems():
            proc.stdin.close()

        run.wait(mds_daemons.itervalues())


def healthy(ctx, config):
    log.info('Waiting until ceph is healthy...')
    firstmon = teuthology.get_first_mon(ctx, config)
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    teuthology.wait_until_healthy(
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

    To run some daemons under valgrind, include their names
    and the tool to use in a valgrind section::
        tasks:
        - ceph:
          valgrind:
            mds.1: --tool=memcheck
            osd.1: --tool=memcheck
    Those nodes which are using memcheck or helgrind will get
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

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    config.update(overrides.get('ceph', {}))

    flavor = None
    if config.get('path'):
        # local dir precludes any other flavors
        flavor = 'local'
    else:
        if config.get('coverage'):
            log.info('Recording coverage for this run.')
            flavor = 'gcov'
        else:
            if config.get('valgrind'):
                log.info('Using notcmalloc flavor and running some daemons under valgrind')
                flavor = 'notcmalloc'
    ctx.summary['flavor'] = flavor or 'default'

    if config.get('coverage'):
        coverage_dir = '/tmp/cephtest/archive/coverage'
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

    if config.get('valgrind'):
        val_path = '/tmp/cephtest/archive/log/{val_dir}'.format(val_dir=config.get('valgrind').get('logs', "valgrind"))
        run.wait(
            ctx.cluster.run(
                args=[
                    'mkdir', '-p', val_path
                    ],
                wait=False,
                )
            )


    with contextutil.nested(
        lambda: ceph_log(ctx=ctx, config=None),
        lambda: ship_utilities(ctx=ctx, config=None),
        lambda: binaries(ctx=ctx, config=dict(
                branch=config.get('branch'),
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                path=config.get('path'),
                flavor=flavor,
                )),
        lambda: valgrind_post(ctx=ctx, config=config),
        lambda: cluster(ctx=ctx, config=dict(
                conf=config.get('conf', {})
                )),
        lambda: mon(ctx=ctx, config=config),
        lambda: osd(ctx=ctx, config=config),
        lambda: mds(ctx=ctx, config=config),
        ):
        healthy(ctx=ctx, config=None)
        yield
