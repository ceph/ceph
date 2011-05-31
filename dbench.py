from gevent import monkey; monkey.patch_all()
from orchestra import monkey; monkey.patch_all()

from cStringIO import StringIO

import json
import logging
import os
import urllib2
import sys
import yaml

from orchestra import connection, run
# TODO cleanup
import teuthology.misc as teuthology

log = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(
        # level=logging.INFO,
        level=logging.DEBUG,
        )

    with file('dbench.yaml') as f:
        config = yaml.safe_load(f)

    ROLES = config['roles']

    connections = [connection.connect(t) for t in config['targets']]

    log.info('Checking for old test directory...')
    has_junk = False
    for target, conn in zip(config['targets'], connections):
        try:
            run.run(
                client=conn,
                args=[
                    'test', '!', '-e', '/tmp/cephtest',
                    ],
                )
        except run.CommandFailedError as e:
            log.error('Host %s has stale cephtest directory, check your lock and reboot to clean up.', target)
            has_junk = True
    if has_junk:
        sys.exit(1)

    log.info('Creating directories...')
    run.wait(
        run.run(
            client=conn,
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
        for conn in connections
        )

    for filename in ['daemon-helper']:
        log.info('Shipping %r...', filename)
        src = os.path.join(os.path.dirname(__file__), filename)
        dst = os.path.join('/tmp/cephtest', filename)
        with file(src, 'rb') as f:
            for conn in connections:
                teuthology.write_file(
                    conn=conn,
                    path=dst,
                    data=f,
                    )
                f.seek(0)
                run.run(
                    client=conn,
                    args=[
                        'chmod',
                        'a=rx',
                        '--',
                        dst,
                        ],
                    )

    log.info('Untarring ceph binaries...')
    ceph_bin = urllib2.urlopen(teuthology.get_ceph_binary_url())
    teuthology.untar_to_dir(ceph_bin, '/tmp/cephtest/binary', connections)

    log.info('Writing configs...')
    ips = [host for (host, port) in (conn.get_transport().getpeername() for conn in connections)]
    conf = teuthology.skeleton_config(roles=ROLES, ips=ips)
    conf_fp = StringIO()
    conf.write(conf_fp)
    conf_fp.seek(0)
    writes = [
        run.run(
            client=conn,
            args=[
                'python',
                '-c',
                'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
                '/tmp/cephtest/ceph.conf',
                ],
            stdin=run.PIPE,
            wait=False,
            )
        for conn in connections
        ]
    teuthology.feed_many_stdins_and_close(conf_fp, writes)
    run.wait(writes)

    log.info('Setting up mon.0...')
    mon0_idx = teuthology.server_with_role(ROLES, 'mon.0')
    run.run(
        client=connections[mon0_idx],
        args=[
            '/tmp/cephtest/binary/usr/local/bin/cauthtool',
            '--create-keyring',
            '/tmp/cephtest/ceph.keyring',
            ],
        )
    run.run(
        client=connections[mon0_idx],
        args=[
            '/tmp/cephtest/binary/usr/local/bin/cauthtool',
            '--gen-key',
            '--name=mon.',
            '/tmp/cephtest/ceph.keyring',
            ],
        )
    teuthology.create_simple_monmap(
        ssh=connections[mon0_idx],
        conf=conf,
        )

    log.info('Creating admin key on mon.0...')
    run.run(
        client=connections[mon0_idx],
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
        conn=connections[mon0_idx],
        path='/tmp/cephtest/ceph.keyring',
        )
    monmap = teuthology.get_file(
        conn=connections[mon0_idx],
        path='/tmp/cephtest/monmap',
        )
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'mon'):
            if id_ == '0':
                continue

            # copy mon key and initial monmap
            log.info('Sending mon0 info to node {idx}'.format(idx=idx))
            teuthology.write_file(
                conn=connections[idx],
                path='/tmp/cephtest/ceph.keyring',
                data=keyring,
                )
            teuthology.write_file(
                conn=connections[idx],
                path='/tmp/cephtest/monmap',
                data=monmap,
                )

            # no need to do more than once per host
            break

    log.info('Setting up mon nodes...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'mon'):
            run.run(
                client=connections[idx],
                args=[
                    '/tmp/cephtest/binary/usr/local/bin/osdmaptool',
                    '--clobber',
                    '--createsimple', '{num:d}'.format(
                        num=teuthology.num_instances_of_type(ROLES, 'osd'),
                        ),
                    '/tmp/cephtest/osdmap',
                    '--pg_bits', '2',
                    '--pgp_bits', '4',
                    ],
                )
            run.run(
                client=connections[idx],
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
            run.run(
                client=connections[idx],
                args=[
                    'rm',
                    '--',
                    '/tmp/cephtest/monmap',
                    '/tmp/cephtest/osdmap',
                    ],
                )

    mon_daemons = {}
    log.info('Starting mon daemons...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'mon'):
            proc = run.run(
                client=connections[idx],
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
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            run.run(
                client=connections[idx],
                args=[
                    '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=osd.{id}'.format(id=id_),
                    '/tmp/cephtest/data/osd.{id}.keyring'.format(id=id_),
                    ],
                )

    log.info('Setting up mds nodes...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'mds'):
            run.run(
                client=connections[idx],
                args=[
                    '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=mds.{id}'.format(id=id_),
                    '/tmp/cephtest/data/mds.{id}.keyring'.format(id=id_),
                    ],
                )

    log.info('Setting up client nodes...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            run.run(
                client=connections[idx],
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
    for idx, roles_for_host in enumerate(ROLES):
        for type_ in ['osd','mds','client']:
            for id_ in teuthology.roles_of_type(roles_for_host, type_):
                data = teuthology.get_file(
                    conn=connections[idx],
                    path='/tmp/cephtest/data/{type}.{id}.keyring'.format(
                        type=type_,
                        id=id_,
                        ),
                    )
                keys.append((type_, id_, data))

    log.info('Adding keys to mon.0...')
    for type_, id_, data in keys:
        teuthology.write_file(
            conn=connections[mon0_idx],
            path='/tmp/cephtest/temp.keyring',
            data=data,
            )
        run.run(
            client=connections[mon0_idx],
            args=[
                '/tmp/cephtest/binary/usr/local/bin/cauthtool',
                '/tmp/cephtest/temp.keyring',
                '--name={type}.{id}'.format(
                    type=type_,
                    id=id_,
                    ),
                ] + list(teuthology.generate_caps(type_)),
            )
        run.run(
            client=connections[mon0_idx],
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
    run.run(
        client=connections[mon0_idx],
        args=[
            '/tmp/cephtest/binary/usr/local/bin/ceph',
            '-c', '/tmp/cephtest/ceph.conf',
            '-k', '/tmp/cephtest/ceph.keyring',
            'mds',
            'set_max_mds',
            '{num_mds:d}'.format(
                num_mds=teuthology.num_instances_of_type(ROLES, 'mds'),
                ),
            ],
        )

    log.info('Running mkfs on osd nodes...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            run.run(
                client=connections[idx],
                args=[
                    'mkdir',
                    os.path.join('/tmp/cephtest/data', 'osd.{id}.data'.format(id=id_)),
                    ],
                )
            run.run(
                client=connections[idx],
                args=[
                    '/tmp/cephtest/binary/usr/local/bin/cosd',
                    '--mkfs',
                    '-i', id_,
                    '-c', '/tmp/cephtest/ceph.conf'
                    ],
                )

    osd_daemons = {}
    log.info('Starting osd daemons...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            proc = run.run(
                client=connections[idx],
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
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'mds'):
            proc = run.run(
                client=connections[idx],
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
        conn=connections[mon0_idx],
        )


    log.info('Mounting cfuse clients...')
    cfuse_daemons = {}
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            run.run(
                client=connections[idx],
                args=[
                    'mkdir',
                    '--',
                    mnt,
                    ],
                )
            proc = run.run(
                client=connections[idx],
                args=[
                    '/tmp/cephtest/daemon-helper',
                    '/tmp/cephtest/binary/usr/local/bin/cfuse',
                    '-f',
                    '--name', 'client.{id}'.format(id=id_),
                    '-c', '/tmp/cephtest/ceph.conf',
                    # TODO cfuse doesn't understand dash dash '--',
                    mnt,
                    ],
                logger=log.getChild('cfuse.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
            cfuse_daemons[id_] = proc
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            teuthology.wait_until_fuse_mounted(
                conn=connections[idx],
                fuse=cfuse_daemons[id_],
                mountpoint=mnt,
                )

    # TODO kclient mount/umount

    # TODO rbd

    log.info('Setting up autotest...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            run.run(
                client=connections[idx],
                args=[
                    'mkdir', '/tmp/cephtest/autotest',
                    run.Raw('&&'),
                    'wget',
                    '-nv',
                    '--no-check-certificate',
                    'https://github.com/tv42/autotest/tarball/ceph',
                    '-O-',
                    run.Raw('|'),
                    'tar',
                    '-C', '/tmp/cephtest/autotest',
                    '-x',
                    '-z',
                    '-f-',
                    '--strip-components=1',
                    ],
                )
            # once per host is enough
            break

    log.info('Making a separate scratch dir for every client...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            scratch = os.path.join(mnt, 'client.{id}'.format(id=id_))
            run.run(
                client=connections[idx],
                args=[
                    'sudo',
                    'install',
                    '-d',
                    '-m', '0755',
                    '--owner={user}'.format(user='ubuntu'), #TODO
                    '--',
                    scratch,
                    ],
                )

    testname = 'dbench' #TODO
    log.info('Running autotest client test %s...', testname)
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            scratch = os.path.join(mnt, 'client.{id}'.format(id=id_))
            tag = '{testname}.client.{id}'.format(
                testname=testname,
                id=id_,
                )
            control = '/tmp/cephtest/control.{tag}'.format(tag=tag)
            teuthology.write_file(
                conn=connections[idx],
                path=control,
                data='import json; data=json.loads({data!r}); job.run_test(**data)'.format(
                    data=json.dumps(dict(
                            url=testname,
                            dir=scratch,
                            # TODO perhaps tag
                            # results will be in /tmp/cephtest/autotest/client/results/dbench
                            # or /tmp/cephtest/autotest/client/results/dbench.{tag}
                            )),
                    ),
                )
            run.run(
                client=connections[idx],
                args=[
                    '/tmp/cephtest/autotest/client/bin/autotest',
                    '--harness=simple',
                    '--tag={tag}'.format(tag=tag),
                    control,
                    run.Raw('3>&1'),
                    ],
                )

    import code
    import readline
    import rlcompleter
    rlcompleter.__name__ # silence pyflakes
    readline.parse_and_bind('tab: complete')
    code.interact(
        banner='Ceph test interactive mode, press control-D to exit...',
        # TODO simplify this
        local=dict(
            config=config,
            ROLES=ROLES,
            connections=connections,
            ),
        )

    log.info('Unmounting cfuse clients...')
    for idx, roles_for_host in enumerate(ROLES):
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            run.run(
                client=connections[idx],
                args=[
                    'fusermount',
                    '-u',
                    mnt,
                    ],
                )
    run.wait(cfuse_daemons.itervalues())


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
