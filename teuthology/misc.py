from cStringIO import StringIO

import os
import logging
import configobj
import time

from orchestra import run

log = logging.getLogger(__name__)

def get_ceph_binary_url():
    machine = os.uname()[4]
    BRANCH = 'master'
    CEPH_TARBALL_DEFAULT_URL = 'http://ceph.newdream.net/gitbuilder/output/ref/origin_{branch}/ceph.{machine}.tgz'.format(
        branch=BRANCH,
        machine=machine,
        )
    return CEPH_TARBALL_DEFAULT_URL

def feed_many_stdins(fp, processes):
    while True:
        data = fp.read(8192)
        if not data:
            break
        for proc in processes:
            proc.stdin.write(data)

def feed_many_stdins_and_close(fp, processes):
    feed_many_stdins(fp, processes)
    for proc in processes:
        proc.stdin.close()

def untar_to_dir(fp, dst, conns):
    """
    Untar a ``.tar.gz`` to given hosts and directories.

    :param fp: a file-like object
    :param conns_and_dirs: a list of 2-tuples `(client, path)`
    """
    untars = [
        run.run(
            client=ssh,
            logger=log.getChild('cephbin'),
            args=['tar', '-xzf', '-', '-C', dst],
            wait=False,
            stdin=run.PIPE,
            )
        for ssh in conns
        ]
    feed_many_stdins_and_close(fp, untars)
    run.wait(untars)

def get_mons(roles, ips):
    mons = {}
    for idx, roles in enumerate(roles):
        for role in roles:
            if not role.startswith('mon.'):
                continue
            mon_id = int(role[len('mon.'):])
            addr = '{ip}:{port}'.format(
                ip=ips[idx],
                port=6789+mon_id,
                )
            mons[role] = addr
    assert mons
    return mons

def generate_caps(type_):
    defaults = dict(
        osd=dict(
            mon='allow *',
            osd='allow *',
            ),
        mds=dict(
            mon='allow *',
            osd='allow *',
            mds='allow',
            ),
        client=dict(
            mon='allow rw',
            osd='allow rwx pool=data,rbd',
            mds='allow',
            ),
        )
    for subsystem, capability in defaults[type_].items():
        yield '--cap'
        yield subsystem
        yield capability

def skeleton_config(roles, ips):
    """
    Returns a ConfigObj that's prefilled with a skeleton config.

    Use conf[section][key]=value or conf.merge to change it.

    Use conf.write to write it out, override .filename first if you want.
    """
    path = os.path.join(os.path.dirname(__file__), 'ceph.conf')
    conf = configobj.ConfigObj(path, file_error=True)
    mons = get_mons(roles=roles, ips=ips)
    for role, addr in mons.iteritems():
        conf.setdefault(role, {})
        conf[role]['mon addr'] = addr
    return conf

def roles_of_type(roles_for_host, type_):
    prefix = '{type}.'.format(type=type_)
    for name in roles_for_host:
        if not name.startswith(prefix):
            continue
        id_ = name[len(prefix):]
        yield id_

def num_instances_of_type(roles, type_):
    prefix = '{type}.'.format(type=type_)
    num = sum(sum(1 for role in hostroles if role.startswith(prefix)) for hostroles in roles)
    return num

def server_with_role(all_roles, role):
    for idx, host_roles in enumerate(all_roles):
        if role in host_roles:
            return idx

def create_simple_monmap(ssh, conf):
    """
    Writes a simple monmap based on current ceph.conf into <tmpdir>/monmap.

    Assumes ceph_conf is up to date.

    Assumes mon sections are named "mon.*", with the dot.
    """
    def gen_addresses():
        for section, data in conf.iteritems():
            PREFIX = 'mon.'
            if not section.startswith(PREFIX):
                continue
            name = section[len(PREFIX):]
            addr = data['mon addr']
            yield (name, addr)

    addresses = list(gen_addresses())
    assert addresses, "There are no monitors in config!"
    log.debug('Ceph mon addresses: %s', addresses)

    args = [
        '/tmp/cephtest/binary/usr/local/bin/monmaptool',
        '--create',
        '--clobber',
        ]
    for (name, addr) in addresses:
        args.extend(('--add', name, addr))
    args.extend([
            '--print',
            '/tmp/cephtest/monmap',
            ])
    run.run(
        client=ssh,
        args=args,
        )

def write_file(conn, path, data):
    run.run(
        client=conn,
        args=[
            'python',
            '-c',
            'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
            path,
            ],
        stdin=data,
        )

def get_file(conn, path):
    """
    Read a file from remote host into memory.
    """
    proc = run.run(
        client=conn,
        args=[
            'cat',
            '--',
            path,
            ],
        stdout=StringIO(),
        )
    data = proc.stdout.getvalue()
    return data

def wait_until_healthy(conn):
    """Wait until a Ceph cluster is healthy."""
    while True:
        r = run.run(
            client=conn,
            args=[
                '/tmp/cephtest/binary/usr/local/bin/ceph',
                '-c', '/tmp/cephtest/ceph.conf',
                'health',
                '--concise',
                ],
            stdout=StringIO(),
            logger=log.getChild('health'),
            )
        out = r.stdout.getvalue()
        log.debug('Ceph health: %s', out.rstrip('\n'))
        if out.split(None, 1)[0] == 'HEALTH_OK':
            break
        time.sleep(1)

def wait_until_fuse_mounted(conn, fuse, mountpoint):
    while True:
        proc = run.run(
            client=conn,
            args=[
                'stat',
                '--file-system',
                '--printf=%T\n',
                '--',
                mountpoint,
                ],
            stdout=StringIO(),
            )
        fstype = proc.stdout.getvalue().rstrip('\n')
        if fstype == 'fuseblk':
            break
        log.debug('cfuse not yet mounted, got fs type {fstype!r}'.format(fstype=fstype))

        # it shouldn't have exited yet; exposes some trivial problems
        assert not fuse.exitstatus.ready()

        time.sleep(5)
    log.info('cfuse is mounted on %s', mountpoint)
