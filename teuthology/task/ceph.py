from cStringIO import StringIO

import contextlib
import logging
import os
import gevent

from teuthology import misc as teuthology
from orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    assert config is None

    log.info('Checking for old test directory...')
    processes = ctx.cluster.run(
        args=[
            'test', '!', '-e', '/tmp/cephtest',
            ],
        wait=False,
        )
    failed = False
    for proc in processes:
        assert isinstance(proc.exitstatus, gevent.event.AsyncResult)
        try:
            proc.exitstatus.get()
        except run.CommandFailedError:
            log.error('Host %s has stale cephtest directory, check your lock and reboot to clean up.', proc.remote.shortname)
            failed = True
    if failed:
        raise RuntimeError('Stale jobs detected, aborting.')

    log.info('Creating directories...')
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '/tmp/cephtest/binary',
                '/tmp/cephtest/log',
                '/tmp/cephtest/profiling-logger',
                '/tmp/cephtest/data',
                '/tmp/cephtest/class_tmp',
                ],
            wait=False,
            )
        )

    for filename in ['daemon-helper']:
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

    log.info('Untarring ceph binaries...')
    ceph_bindir_url = teuthology.get_ceph_binary_url()
    ctx.cluster.run(
        args=[
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

    log.info('Writing configs...')
    remotes_and_roles = ctx.cluster.remotes.items()
    roles = [roles for (remote, roles) in remotes_and_roles]
    ips = [host for (host, port) in (remote.ssh.get_transport().getpeername() for (remote, roles) in remotes_and_roles)]
    conf = teuthology.skeleton_config(roles=roles, ips=ips)
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

    log.info('Setting up mon.0...')
    ctx.cluster.only('mon.0').run(
        args=[
            '/tmp/cephtest/binary/usr/local/bin/cauthtool',
            '--create-keyring',
            '/tmp/cephtest/ceph.keyring',
            ],
        )
    ctx.cluster.only('mon.0').run(
        args=[
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

    log.info('Copying mon.0 info to all monitors...')
    keyring = teuthology.get_file(
        remote=mon0_remote,
        path='/tmp/cephtest/ceph.keyring',
        )
    monmap = teuthology.get_file(
        remote=mon0_remote,
        path='/tmp/cephtest/monmap',
        )
    mons = ctx.cluster.only(teuthology.is_type('mon'))
    mons_no_0 = mons.exclude('mon.0')

    for rem in mons_no_0.remotes.iterkeys():
        # copy mon key and initial monmap
        log.info('Sending mon0 info to node {remote}'.format(remote=rem))
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
    run.wait(
        mons.run(
            args=[
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

    for remote, roles_for_host in mons.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mon'):
            remote.run(
                args=[
                    '/tmp/cephtest/binary/usr/local/bin/cmon',
                    '--mkfs',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf',
                    '--monmap=/tmp/cephtest/monmap',
                    '--osdmap=/tmp/cephtest/osdmap',
                    '--keyring=/tmp/cephtest/ceph.keyring',
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

    mon_daemons = {}
    log.info('Starting mon daemons...')
    for remote, roles_for_host in mons.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mon'):
            proc = remote.run(
                args=[
                    '/tmp/cephtest/daemon-helper',
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

    log.info('Setting up osd nodes...')
    osds = ctx.cluster.only(teuthology.is_type('osd'))
    for remote, roles_for_host in osds.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            remote.run(
                args=[
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
                    '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                    '--create-keyring',
                    '--gen-key',
                    # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
                    '--name=client.{id}'.format(id=id_),
                    '/tmp/cephtest/data/client.{id}.keyring'.format(id=id_),
                    ],
                )

    log.info('Reading keys from all nodes...')
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

    log.info('Adding keys to mon.0...')
    for type_, id_, data in keys:
        teuthology.write_file(
            remote=mon0_remote,
            path='/tmp/cephtest/temp.keyring',
            data=data,
            )
        mon0_remote.run(
            args=[
                '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                '/tmp/cephtest/temp.keyring',
                '--name={type}.{id}'.format(
                    type=type_,
                    id=id_,
                    ),
                ] + list(teuthology.generate_caps(type_)),
            )
        mon0_remote.run(
            args=[
                '/tmp/cephtest/binary/usr/local/bin/ceph',
                '-c', '/tmp/cephtest/ceph.conf',
                '-k', '/tmp/cephtest/ceph.keyring',
                '-i', '/tmp/cephtest/temp.keyring',
                'auth',
                'add',
                '{type}.{id}'.format(
                    type=type_,
                    id=id_,
                    ),
                ],
            )

    log.info('Setting max_mds...')
    # TODO where does this belong?
    mon0_remote.run(
        args=[
            '/tmp/cephtest/binary/usr/local/bin/ceph',
            '-c', '/tmp/cephtest/ceph.conf',
            '-k', '/tmp/cephtest/ceph.keyring',
            'mds',
            'set_max_mds',
            '{num_mds:d}'.format(
                num_mds=teuthology.num_instances_of_type(ctx.cluster, 'mds'),
                ),
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
                    '/tmp/cephtest/binary/usr/local/bin/cosd',
                    '--mkfs',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf'
                    ],
                )

    osd_daemons = {}
    log.info('Starting osd daemons...')
    for remote, roles_for_host in osds.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            proc = remote.run(
                args=[
                    '/tmp/cephtest/daemon-helper',
                    '/tmp/cephtest/binary/usr/local/bin/cosd',
                    '-f',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf'
                    ],
                logger=log.getChild('osd.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
            osd_daemons[id_] = proc

    mds_daemons = {}
    log.info('Starting mds daemons...')
    for remote, roles_for_host in mdss.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'mds'):
            proc = remote.run(
                args=[
                    '/tmp/cephtest/daemon-helper',
                    '/tmp/cephtest/binary/usr/local/bin/cmds',
                    '-f',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf'
                    ],
                logger=log.getChild('mds.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
            mds_daemons[id_] = proc


    log.info('Waiting until ceph is healthy...')
    teuthology.wait_until_healthy(
        remote=mon0_remote,
        )

    try:
        yield
    finally:
        log.info('Shutting down mds daemons...')
        for id_, proc in mds_daemons.iteritems():
            proc.stdin.close()
        run.wait(mds_daemons.itervalues())

        log.info('Shutting down osd daemons...')
        for id_, proc in osd_daemons.iteritems():
            proc.stdin.close()
        run.wait(osd_daemons.itervalues())

        log.info('Shutting down mon daemons...')
        for id_, proc in mon_daemons.iteritems():
            proc.stdin.close()
        run.wait(mon_daemons.itervalues())
