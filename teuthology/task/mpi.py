"""
Start mpi processes (and allow commands to be run inside process)
"""
import logging

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Setup MPI and execute commands

    Example that starts an MPI process on specific clients::

        tasks:
        - ceph:
        - ceph-fuse: [client.0, client.1]
        - ssh_keys:
        - mpi: 
            nodes: [client.0, client.1]
            exec: ior ...

    Example that starts MPI processes on all clients::

        tasks:
        - ceph:
        - ceph-fuse:
        - ssh_keys:
        - mpi:
            exec: ior ...

    Example that starts MPI processes on all roles::

        tasks:
        - ceph:
        - ssh_keys:
        - mpi:
            nodes: all
            exec: ...

    Example that specifies a working directory for MPI processes:

        tasks:
        - ceph:
        - ceph-fuse:
        - pexec:
            clients:
              - ln -s {testdir}/mnt.* {testdir}/gmnt
        - ssh_keys:
        - mpi:
            exec: fsx-mpi
            workdir: {testdir}/gmnt
        - pexec:
            clients:
              - rm -f {testdir}/gmnt

    :param ctx: Context
    :param config: Configuration
    """
    assert isinstance(config, dict), 'task mpi got invalid config'
    assert 'exec' in config, 'task mpi got invalid config, missing exec'

    testdir = teuthology.get_testdir(ctx)

    mpiexec = config['exec'].replace('$TESTDIR', testdir)
    hosts = []
    remotes = []
    master_remote = None
    if 'nodes' in config:
        if isinstance(config['nodes'], basestring) and config['nodes'] == 'all':
            for role in  teuthology.all_roles(ctx.cluster):
                remote = teuthology.get_single_remote_value(ctx,role)
                ip,port = remote.ssh.get_transport().getpeername()
                hosts.append(ip)
                remotes.append(remote)
            master_remote = teuthology.get_single_remote_value(ctx,
                    config['nodes'][0])
        elif isinstance(config['nodes'], list):
            for role in config['nodes']:
                remote = teuthology.get_single_remote_value(ctx, role)
                ip,port = remote.ssh.get_transport().getpeername()
                hosts.append(ip)
                remotes.append(remote)
            master_remote = teuthology.get_single_remote_value(ctx,
                    config['nodes'][0])
    else:
        roles = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        master_remote = teuthology.get_single_remote_value(ctx, roles[0])
        for role in roles:
            remote = teuthology.get_single_remote_value(ctx, role)
            ip,port = remote.ssh.get_transport().getpeername()
            hosts.append(ip)
            remotes.append(remote)

    workdir = []
    if 'workdir' in config:
        workdir = ['-wdir', config['workdir'].replace('$TESTDIR', testdir) ]

    log.info('mpi rank 0 is: {name}'.format(name=master_remote.name))

    # write out the mpi hosts file
    log.info('mpi nodes: [%s]' % (', '.join(hosts)))
    teuthology.write_file(remote=master_remote,
                          path='{tdir}/mpi-hosts'.format(tdir=testdir),
                          data='\n'.join(hosts))
    log.info('mpiexec on {name}: {cmd}'.format(name=master_remote.name, cmd=mpiexec))
    args=['mpiexec', '-f', '{tdir}/mpi-hosts'.format(tdir=testdir)]
    args.extend(workdir)
    args.extend(mpiexec.split(' '))
    master_remote.run(args=args, )
    log.info('mpi task completed')
    master_remote.run(args=['rm', '{tdir}/mpi-hosts'.format(tdir=testdir)])
