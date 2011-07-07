from cStringIO import StringIO

import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import contextutil
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
                        'bzip2',
                        '-9',
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

@contextlib.contextmanager
def binaries(ctx, config):
    log.info('Unpacking ceph binaries...')
    sha1, ceph_bindir_url = teuthology.get_ceph_binary_url(
        branch=config.get('branch'),
        tag=config.get('tag'),
        sha1=config.get('sha1'),
        flavor=config.get('flavor'),
        )
    ctx.summary['flavor'] = config.get('flavor', 'default')
    if ctx.archive is not None:
        with file(os.path.join(ctx.archive, 'ceph-sha1'), 'w') as f:
            f.write(sha1 + '\n')
    ctx.cluster.run(
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

    log.info('Setting up mon.0...')
    ctx.cluster.only('mon.0').run(
        args=[
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            coverage_dir,
            '/tmp/cephtest/binary/usr/local/bin/cauthtool',
            '--create-keyring',
            '/tmp/cephtest/ceph.keyring',
            ],
        )
    ctx.cluster.only('mon.0').run(
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
    (mon0_remote,) = ctx.cluster.only('mon.0').remotes.keys()
    teuthology.create_simple_monmap(
        remote=mon0_remote,
        conf=conf,
        )

    log.info('Creating admin key on mon.0...')
    ctx.cluster.only('mon.0').run(
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
    mons_no_0 = mons.exclude('mon.0')
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
        for type_ in ['osd','mds','client']:
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
            proc = remote.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/daemon-helper',
                    daemon_signal,
                    '/tmp/cephtest/binary/usr/local/bin/cmon',
                    '-f',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf',
                    ],
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
            proc = remote.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/daemon-helper',
                    daemon_signal,
                    '/tmp/cephtest/binary/usr/local/bin/cosd',
                    '-f',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf',
                    ],
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
    mds_daemons = {}
    mdss = ctx.cluster.only(teuthology.is_type('mds'))
    coverage_dir = '/tmp/cephtest/archive/coverage'

    daemon_signal = 'kill'
    if config.get('coverage'):
        log.info('Recording coverage for this run.')
        daemon_signal = 'term'

    for remote, roles_for_host in mdss.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mds'):
            proc = remote.run(
                args=[
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    coverage_dir,
                    '/tmp/cephtest/daemon-helper',
                    daemon_signal,
                    '/tmp/cephtest/binary/usr/local/bin/cmds',
                    '-f',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf',
#                    '--debug-mds','20',
                    ],
                logger=log.getChild('mds.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
            mds_daemons[id_] = proc

    (mon0_remote,) = ctx.cluster.only('mon.0').remotes.keys()
    mon0_remote.run(args=[
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            '/tmp/cephtest/archive/coverage',
            '/tmp/cephtest/binary/usr/local/bin/ceph',
            '-c', '/tmp/cephtest/ceph.conf',
            'mds', 'set_max_mds', str(len(mdss.remotes))])

    try:
        yield
    finally:
        log.info('Shutting down mds daemons...')
        for id_, proc in mds_daemons.iteritems():
            proc.stdin.close()

        run.wait(mds_daemons.itervalues())


def healthy(ctx, config):
    log.info('Waiting until ceph is healthy...')
    (mon0_remote,) = ctx.cluster.only('mon.0').remotes.keys()
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

    To capture code coverage data, use::

        tasks:
        - ceph:
            coverage: true

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

    flavor = None
    if config.get('coverage'):
        log.info('Recording coverage for this run.')
        flavor = 'gcov'

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

    with contextutil.nested(
        lambda: ceph_log(ctx=ctx, config=None),
        lambda: ship_utilities(ctx=ctx, config=None),
        lambda: binaries(ctx=ctx, config=dict(
                branch=config.get('branch'),
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                flavor=flavor,
                )),
        lambda: cluster(ctx=ctx, config=dict(
                conf=config.get('conf', {})
                )),
        lambda: mon(ctx=ctx, config=dict(
                coverage=config.get('coverage'),
                )),
        lambda: osd(ctx=ctx, config=dict(
                coverage=config.get('coverage'),
                )),
        lambda: mds(ctx=ctx, config=dict(
                coverage=config.get('coverage'),
                )),
        ):
        healthy(ctx=ctx, config=None)
        yield
