import logging
import pipes
import os

from teuthology import misc as teuthology
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run ceph all workunits found under the specified path.

    For example::

        tasks:
        - ceph:
        - cfuse: [client.0]
        - workunit:
            clients:
              client.0: [direct_io, xattrs.sh]
              client.1: [snaps]
            branch: foo

    You can also run a list of workunits on all clients:
        tasks:
        - ceph:
        - cfuse:
        - workunit:
            tag: v0.47
            clients:
              all: [direct_io, xattrs.sh, snaps]

    If you have an "all" section it will run all the workunits
    on each client simultaneously, AFTER running any workunits specified
    for individual clients. (This prevents unintended simultaneous runs.)

    To customize tests, you can specify environment variables as a dict::

        tasks:
        - ceph:
        - cfuse:
        - workunit:
            sha1: 9b28948635b17165d17c1cf83d4a870bd138ddf6
            clients:
              all: [snaps]
            env:
              FOO: bar
              BAZ: quux
    """
    assert isinstance(config, dict)
    assert isinstance(config.get('clients'), dict), \
        'configuration must contain a dictionary of clients'

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('workunit', {}))

    refspec = config.get('branch')
    if refspec is None:
        refspec = config.get('sha1')
    if refspec is None:
        refspec = config.get('tag')
    if refspec is None:
        refspec = 'HEAD'

    log.info('Pulling workunits from ref %s', refspec)

    if config.get('env') is not None:
        assert isinstance(config['env'], dict), 'env must be a dictionary'
    clients = config['clients']
    log.info('Making a separate scratch dir for every client...')
    for role in clients.iterkeys():
        assert isinstance(role, basestring)
        if role == "all":
            continue
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        _make_scratch_dir(ctx, role)
    all_spec = False #is there an all grouping?
    with parallel() as p:
        for role, tests in clients.iteritems():
            if role != "all":
                p.spawn(_run_tests, ctx, refspec, role, tests, config.get('env'))
            else:
                all_spec = True

    if all_spec:
        all_tasks = clients["all"]
        _spawn_on_all_clients(ctx, refspec, all_tasks, config.get('env'))

def _make_scratch_dir(ctx, role):
    PREFIX = 'client.'
    id_ = role[len(PREFIX):]
    log.debug("getting remote for {id} role {role_}".format(id=id_, role_=role))
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    dir_owner = remote.shortname.split('@', 1)[0]
    mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
    remote.run(
        args=[
            # cd first so this will fail if the mount point does
            # not exist; pure install -d will silently do the
            # wrong thing
            'cd',
            '--',
            mnt,
            run.Raw('&&'),
            'sudo',
            'install',
            '-d',
            '-m', '0755',
            '--owner={user}'.format(user=dir_owner), 
            '--',
            'client.{id}'.format(id=id_),
            ],
        )

def _spawn_on_all_clients(ctx, refspec, tests, env):
    client_generator = teuthology.all_roles_of_type(ctx.cluster, 'client')
    client_remotes = list()
    for client in client_generator:
        (client_remote,) = ctx.cluster.only('client.{id}'.format(id=client)).remotes.iterkeys()
        client_remotes.append((client_remote, 'client.{id}'.format(id=client)))
        _make_scratch_dir(ctx, "client.{id}".format(id=client))
        
    for unit in tests:
        with parallel() as p:
            for remote, role in client_remotes:
                p.spawn(_run_tests, ctx, refspec, role, [unit], env)

def _run_tests(ctx, refspec, role, tests, env):
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    id_ = role[len(PREFIX):]
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
    # subdir so we can remove and recreate this a lot without sudo
    scratch_tmp = os.path.join(mnt, 'client.{id}'.format(id=id_), 'tmp')
    srcdir = '/tmp/cephtest/workunit.{role}'.format(role=role)
    secretfile = '/tmp/cephtest/data/{role}.secret'.format(role=role)
    teuthology.write_secret_file(remote, role, secretfile)

    ceph_ref = ctx.summary.get('ceph-sha1', 'master')

    remote.run(
        logger=log.getChild(role),
        args=[
            'mkdir', '--', srcdir,
            run.Raw('&&'),
            'wget',
            '-q',
            '-O-',
            'https://github.com/ceph/ceph/tarball/%s' % refspec,
            run.Raw('|'),
            'tar',
            '-C', srcdir,
            '-x',
            '-z',
            '-f-',
            '--wildcards',
            '--no-wildcards-match-slash',
            '--strip-components=3',
            '--',
            '*/qa/workunits/',
            run.Raw('&&'),
            'cd', '--', srcdir,
            run.Raw('&&'),
            'if', 'test', '-e', 'Makefile', run.Raw(';'), 'then', 'make', run.Raw(';'), 'fi',
            run.Raw('&&'),
            'find', '-executable', '-type', 'f', '-printf', r'%P\0'.format(srcdir=srcdir),
            run.Raw('>/tmp/cephtest/workunits.list'),
            ],
        )

    workunits = sorted(teuthology.get_file(remote, '/tmp/cephtest/workunits.list').split('\0'))
    assert workunits

    try:
        assert isinstance(tests, list)
        for spec in tests:
            log.info('Running workunits matching %s on %s...', spec, role)
            prefix = '{spec}/'.format(spec=spec)
            to_run = [w for w in workunits if w == spec or w.startswith(prefix)]
            if not to_run:
                raise RuntimeError('Spec did not match any workunits: {spec!r}'.format(spec=spec))
            for workunit in to_run:
                log.info('Running workunit %s...', workunit)
                args = [
                    'mkdir', '--', scratch_tmp,
                    run.Raw('&&'),
                    'cd', '--', scratch_tmp,
                    run.Raw('&&'),
                    run.Raw('CEPH_REF={ref}'.format(ref=ceph_ref)),
                    run.Raw('PATH="$PATH:/tmp/cephtest/binary/usr/local/bin"'),
                    run.Raw('LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/tmp/cephtest/binary/usr/local/lib"'),
                    run.Raw('CEPH_CONF="/tmp/cephtest/ceph.conf"'),
                    run.Raw('CEPH_SECRET_FILE="{file}"'.format(file=secretfile)),
                    run.Raw('CEPH_ID="{id}"'.format(id=id_)),
                    run.Raw('PYTHONPATH="$PYTHONPATH:/tmp/cephtest/binary/usr/local/lib/python2.7/dist-packages:/tmp/cephtest/binary/usr/local/lib/python2.6/dist-packages"'),
                    ]
                if env is not None:
                    for var, val in env.iteritems():
                        quoted_val = pipes.quote(val)
                        env_arg = '{var}={val}'.format(var=var, val=quoted_val)
                        args.append(run.Raw(env_arg))
                args.extend([
                        '/tmp/cephtest/enable-coredump',
                        '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                        '/tmp/cephtest/archive/coverage',
                        '{srcdir}/{workunit}'.format(
                            srcdir=srcdir,
                            workunit=workunit,
                            ),
                        run.Raw('&&'),
                        'rm', '-rf', '--', scratch_tmp,
                        ])
                remote.run(
                    logger=log.getChild(role),
                    args=args,
                    )
    finally:
        remote.run(
            logger=log.getChild(role),
            args=[
                'rm', '-rf', '--', '/tmp/cephtest/workunits.list', srcdir,
                ],
            )
