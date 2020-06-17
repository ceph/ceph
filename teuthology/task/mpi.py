"""
Start mpi processes (and allow commands to be run inside process)
"""
import logging
import re

from teuthology import misc as teuthology

log = logging.getLogger(__name__)


def _check_mpi_version(remotes):
    """
    Retrieve the MPI version from each of `remotes` and raise an exception
    if they are not all the same version.
    """
    versions = set()
    for remote in remotes:
        version_str = remote.sh("mpiexec --version")
        try:
            version = re.search("^\s+Version:\s+(.+)$", version_str, re.MULTILINE).group(1)
        except AttributeError:
            raise RuntimeError("Malformed MPI version output: {0}".format(version_str))
        else:
            versions.add(version)

    if len(versions) != 1:
        raise RuntimeError("MPI version mismatch.  Versions are: {0}".format(", ".join(versions)))
    else:
        log.info("MPI version {0}".format(list(versions)[0]))


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
        if isinstance(config['nodes'], str) and config['nodes'] == 'all':
            for role in  teuthology.all_roles(ctx.cluster):
                (remote,) = ctx.cluster.only(role).remotes.keys()
                ip,port = remote.ssh.get_transport().getpeername()
                hosts.append(ip)
                remotes.append(remote)
            (master_remote,) = ctx.cluster.only(config['nodes'][0]).remotes.keys()
        elif isinstance(config['nodes'], list):
            for role in config['nodes']:
                (remote,) = ctx.cluster.only(role).remotes.keys()
                ip,port = remote.ssh.get_transport().getpeername()
                hosts.append(ip)
                remotes.append(remote)
            (master_remote,) = ctx.cluster.only(config['nodes'][0]).remotes.keys()
    else:
        roles = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        (master_remote,) = ctx.cluster.only(roles[0]).remotes.keys()
        for role in roles:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            ip,port = remote.ssh.get_transport().getpeername()
            hosts.append(ip)
            remotes.append(remote)

    # mpich is sensitive to different versions on different nodes
    _check_mpi_version(remotes)

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
