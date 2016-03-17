"""
Miscellaneous teuthology functions.
Used by other modules, but mostly called from tasks.
"""
from cStringIO import StringIO

import argparse
import os
import logging
import configobj
import getpass
import socket
import subprocess
import sys
import tarfile
import time
import urllib2
import urlparse
import yaml
import json
import re
import tempfile
import pprint

from teuthology import safepath
from teuthology.exceptions import (CommandCrashedError, CommandFailedError,
                                   ConnectionLostError)
from .orchestra import run
from .config import config
from .contextutil import safe_while
from .packaging import DEFAULT_OS_VERSION

log = logging.getLogger(__name__)

import datetime
stamp = datetime.datetime.now().strftime("%y%m%d%H%M")
is_vm = lambda x: x.startswith('vpm') or x.startswith('ubuntu@vpm')

is_arm = lambda x: x.startswith('tala') or x.startswith(
    'ubuntu@tala') or x.startswith('saya') or x.startswith('ubuntu@saya')

hostname_expr_templ = '(?P<user>.*@)?(?P<shortname>.*)\.{lab_domain}'


def canonicalize_hostname(hostname, user='ubuntu'):
    hostname_expr = hostname_expr_templ.format(
        lab_domain=config.lab_domain.replace('.', '\.'))
    match = re.match(hostname_expr, hostname)
    if match:
        match_d = match.groupdict()
        shortname = match_d['shortname']
        if user is None:
            user_ = user
        else:
            user_ = match_d.get('user') or user
    else:
        shortname = hostname.split('.')[0]
        user_ = user

    user_at = user_.strip('@') + '@' if user_ else ''

    ret = '{user_at}{short}.{lab_domain}'.format(
        user_at=user_at,
        short=shortname,
        lab_domain=config.lab_domain,
    )
    return ret


def decanonicalize_hostname(hostname):
    hostname_expr = hostname_expr_templ.format(
        lab_domain=config.lab_domain.replace('.', '\.'))
    match = re.match(hostname_expr, hostname)
    if match:
        hostname = match.groupdict()['shortname']
    return hostname


def config_file(string):
    """
    Create a config file

    :param string: name of yaml file used for config.
    :returns: Dictionary of configuration information.
    """
    config_dict = {}
    try:
        with file(string) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                config_dict.update(new)
    except IOError as e:
        raise argparse.ArgumentTypeError(str(e))
    return config_dict


class MergeConfig(argparse.Action):
    """
    Used by scripts to mergeg configurations.   (nuke, run, and
    schedule, for example)
    """
    def __call__(self, parser, namespace, values, option_string=None):
        """
        Perform merges of all the day in the config dictionaries.
        """
        config_dict = getattr(namespace, self.dest)
        for new in values:
            deep_merge(config_dict, new)


def merge_configs(config_paths):
    """ Takes one or many paths to yaml config files and merges them
        together, returning the result.
    """
    conf_dict = dict()
    for conf_path in config_paths:
        if not os.path.exists(conf_path):
            log.debug("The config path {0} does not exist, skipping.".format(conf_path))
            continue
        with file(conf_path) as partial_file:
            partial_dict = yaml.safe_load(partial_file)
        try:
            conf_dict = deep_merge(conf_dict, partial_dict)
        except Exception:
            # TODO: Should this log as well?
            pprint.pprint("failed to merge {0} into {1}".format(conf_dict, partial_dict))
            raise

    return conf_dict


def get_testdir(ctx=None):
    """
    :param ctx: Unused; accepted for compatibility
    :returns: A test directory
    """
    if 'test_path' in config:
        return config['test_path']
    return config.get(
        'test_path',
        '/home/%s/cephtest' % get_test_user()
    )


def get_test_user(ctx=None):
    """
    :param ctx: Unused; accepted for compatibility
    :returns:   str -- the user to run tests as on remote hosts
    """
    return config.get('test_user', 'ubuntu')


def get_archive_dir(ctx):
    """
    :returns: archive directory (a subdirectory of the test directory)
    """
    test_dir = get_testdir(ctx)
    return os.path.normpath(os.path.join(test_dir, 'archive'))


def get_http_log_path(archive_dir, job_id=None):
    """
    :param archive_dir: directory to be searched
    :param job_id: id of job that terminates the name of the log path
    :returns: http log path
    """
    http_base = config.archive_server
    if not http_base:
        return None

    sep = os.path.sep
    archive_dir = archive_dir.rstrip(sep)
    archive_subdir = archive_dir.split(sep)[-1]
    if archive_subdir.endswith(str(job_id)):
        archive_subdir = archive_dir.split(sep)[-2]

    if job_id is None:
        return os.path.join(http_base, archive_subdir, '')
    return os.path.join(http_base, archive_subdir, str(job_id), '')


def get_results_url(run_name, job_id=None):
    """
    :param run_name: The name of the test run
    :param job_id: The job_id of the job. Optional.
    :returns: URL to the run (or job, if job_id is passed) in the results web
              UI. For example, Inktank uses Pulpito.
    """
    if not config.results_ui_server:
        return None
    base_url = config.results_ui_server

    if job_id is None:
        return os.path.join(base_url, run_name, '')
    return os.path.join(base_url, run_name, str(job_id), '')


def get_ceph_binary_url(package=None,
                        branch=None, tag=None, sha1=None, dist=None,
                        flavor=None, format=None, arch=None):
    """
    return the url of the ceph binary found on gitbuildder.
    """
    BASE = 'http://{host}/{package}-{format}-{dist}-{arch}-{flavor}/'.format(
        host=config.gitbuilder_host,
        package=package,
        flavor=flavor,
        arch=arch,
        format=format,
        dist=dist
    )

    if sha1 is not None:
        assert branch is None, "cannot set both sha1 and branch"
        assert tag is None, "cannot set both sha1 and tag"
    else:
        # gitbuilder uses remote-style ref names for branches, mangled to
        # have underscores instead of slashes; e.g. origin_master
        if tag is not None:
            ref = tag
            assert branch is None, "cannot set both branch and tag"
        else:
            if branch is None:
                branch = 'master'
            ref = branch

        sha1_url = urlparse.urljoin(BASE, 'ref/{ref}/sha1'.format(ref=ref))
        log.debug('Translating ref to sha1 using url %s', sha1_url)

        try:
            sha1_fp = urllib2.urlopen(sha1_url)
            sha1 = sha1_fp.read().rstrip('\n')
            sha1_fp.close()
        except urllib2.HTTPError as e:
            log.error('Failed to get url %s', sha1_url)
            raise e

    log.debug('Using %s %s sha1 %s', package, format, sha1)
    bindir_url = urlparse.urljoin(BASE, 'sha1/{sha1}/'.format(sha1=sha1))
    return (sha1, bindir_url)


def feed_many_stdins(fp, processes):
    """
    :param fp: input file
    :param processes: list of processes to be written to.
    """
    while True:
        data = fp.read(8192)
        if not data:
            break
        for proc in processes:
            proc.stdin.write(data)


def feed_many_stdins_and_close(fp, processes):
    """
    Feed many and then close processes.

    :param fp: input file
    :param processes: list of processes to be written to.
    """
    feed_many_stdins(fp, processes)
    for proc in processes:
        proc.stdin.close()


def get_mons(roles, ips):
    """
    Get monitors and their associated ports
    """
    mons = {}
    mon_ports = {}
    mon_id = 0
    is_mon = is_type('mon')
    for idx, roles in enumerate(roles):
        for role in roles:
            if not is_mon(role):
                continue
            if ips[idx] not in mon_ports:
                mon_ports[ips[idx]] = 6789
            else:
                mon_ports[ips[idx]] += 1
            addr = '{ip}:{port}'.format(
                ip=ips[idx],
                port=mon_ports[ips[idx]],
            )
            mon_id += 1
            mons[role] = addr
    assert mons
    return mons


def generate_caps(type_):
    """
    Each call will return the next capability for each system type
    (essentially a subset of possible role values).  Valid types are osd,
    mds and client.
    """
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
            osd='allow rwx',
            mds='allow',
        ),
    )
    for subsystem, capability in defaults[type_].items():
        yield '--cap'
        yield subsystem
        yield capability


def skeleton_config(ctx, roles, ips, cluster='ceph'):
    """
    Returns a ConfigObj that is prefilled with a skeleton config.

    Use conf[section][key]=value or conf.merge to change it.

    Use conf.write to write it out, override .filename first if you want.
    """
    path = os.path.join(os.path.dirname(__file__), 'ceph.conf.template')
    t = open(path, 'r')
    skconf = t.read().format(testdir=get_testdir(ctx))
    conf = configobj.ConfigObj(StringIO(skconf), file_error=True)
    mons = get_mons(roles=roles, ips=ips)
    for role, addr in mons.iteritems():
        mon_cluster, _, _ = split_role(role)
        if mon_cluster != cluster:
            continue
        name = ceph_role(role)
        conf.setdefault(name, {})
        conf[name]['mon addr'] = addr
    # set up standby mds's
    is_mds = is_type('mds', cluster)
    for roles_subset in roles:
        for role in roles_subset:
            if is_mds(role):
                name = ceph_role(role)
                conf.setdefault(name, {})
                if '-s-' in name:
                    standby_mds = name[name.find('-s-') + 3:]
                    conf[name]['mds standby for name'] = standby_mds
    return conf


def ceph_role(role):
    """
    Return the ceph name for the role, without any cluster prefix, e.g. osd.0.
    """
    _, type_, id_ = split_role(role)
    return type_ + '.' + id_


def split_role(role):
    """
    Return a tuple of cluster, type, and id
    If no cluster is included in the role, the default cluster, 'ceph', is used
    """
    cluster = 'ceph'
    if role.count('.') > 1:
        cluster, role = role.split('.', 1)
    type_, id_ = role.split('.', 1)
    return cluster, type_, id_


def roles_of_type(roles_for_host, type_):
    """
    Generator of ids.

    Each call returns the next possible role of the type specified.
    :param roles_for host: list of roles possible
    :param type_: type of role
    """
    for role in cluster_roles_of_type(roles_for_host, type_, None):
        _, _, id_ = split_role(role)
        yield id_


def cluster_roles_of_type(roles_for_host, type_, cluster):
    """
    Generator of roles.

    Each call returns the next possible role of the type specified.
    :param roles_for host: list of roles possible
    :param type_: type of role
    :param cluster: cluster name
    """
    is_type_in_cluster = is_type(type_, cluster)
    for role in roles_for_host:
        if not is_type_in_cluster(role):
            continue
        yield role


def all_roles(cluster):
    """
    Generator of role values.  Each call returns another role.

    :param cluster: Cluster extracted from the ctx.
    """
    for _, roles_for_host in cluster.remotes.iteritems():
        for name in roles_for_host:
            yield name


def all_roles_of_type(cluster, type_):
    """
    Generator of role values.  Each call returns another role of the
    type specified.

    :param cluster: Cluster extracted from the ctx.
    :type_: role type
    """
    for _, roles_for_host in cluster.remotes.iteritems():
        for id_ in roles_of_type(roles_for_host, type_):
            yield id_


def is_type(type_, cluster=None):
    """
    Returns a matcher function for whether role is of type given.

    :param cluster: cluster name to check in matcher (default to no check for cluster)
    """
    def _is_type(role):
        """
        Return type based on the starting role name.

        If there is more than one period, strip the first part
        (ostensibly a cluster name) and check the remainder for the prefix.
        """
        role_cluster, role_type, _ = split_role(role)
        if cluster is not None and role_cluster != cluster:
            return False
        return role_type == type_
    return _is_type


def num_instances_of_type(cluster, type_, ceph_cluster='ceph'):
    """
    Total the number of instances of the role type specified in all remotes.

    :param cluster: Cluster extracted from ctx.
    :param type_: role
    :param ceph_cluster: filter for ceph cluster name
    """
    remotes_and_roles = cluster.remotes.items()
    roles = [roles for (remote, roles) in remotes_and_roles]
    is_ceph_type = is_type(type_, ceph_cluster)
    num = sum(sum(1 for role in hostroles if is_ceph_type(role))
              for hostroles in roles)
    return num


def create_simple_monmap(ctx, remote, conf, path=None):
    """
    Writes a simple monmap based on current ceph.conf into path, or
    <testdir>/monmap by default.

    Assumes ceph_conf is up to date.

    Assumes mon sections are named "mon.*", with the dot.

    :return the FSID (as a string) of the newly created monmap
    """
    def gen_addresses():
        """
        Monitor address generator.

        Each invocation returns the next monitor address
        """
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

    testdir = get_testdir(ctx)
    args = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'monmaptool',
        '--create',
        '--clobber',
    ]
    for (name, addr) in addresses:
        args.extend(('--add', name, addr))
    if not path:
        path = '{tdir}/monmap'.format(tdir=testdir)
    args.extend([
        '--print',
        path
    ])

    r = remote.run(
        args=args,
        stdout=StringIO()
    )
    monmap_output = r.stdout.getvalue()
    fsid = re.search("generated fsid (.+)$",
                     monmap_output, re.MULTILINE).group(1)
    return fsid


def write_file(remote, path, data):
    """
    Write data to a remote file

    :param remote: Remote site.
    :param path: Path on the remote being written to.
    :param data: Data to be written.
    """
    remote.run(
        args=[
            'python',
            '-c',
            'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
            path,
        ],
        stdin=data,
    )


def sudo_write_file(remote, path, data, perms=None, owner=None):
    """
    Write data to a remote file as super user

    :param remote: Remote site.
    :param path: Path on the remote being written to.
    :param data: Data to be written.
    :param perms: Permissions on the file being written
    :param owner: Owner for the file being written

    Both perms and owner are passed directly to chmod.
    """
    permargs = []
    if perms:
        permargs = [run.Raw('&&'), 'sudo', 'chmod', perms, path]
    owner_args = []
    if owner:
        owner_args = [run.Raw('&&'), 'sudo', 'chown', owner, path]
    remote.run(
        args=[
            'sudo',
            'python',
            '-c',
            'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
            path,
        ] + owner_args + permargs,
        stdin=data,
    )


def copy_file(from_remote, from_path, to_remote, to_path=None):
    """
    Copies a file from one remote to another.
    """
    if to_path is None:
        to_path = from_path
    from_remote.run(args=[
        'sudo', 'scp', '-v', from_path, "{host}:{file}".format(
            host=to_remote.name, file=to_path)
    ])


def move_file(remote, from_path, to_path, sudo=False):
    """
    Move a file from one path to another on a remote site

    The file needs to be stat'ed first, to make sure we
    maintain the same permissions
    """
    args = []
    if sudo:
        args.append('sudo')
    args.extend([
        'stat',
        '-c',
        '\"%a\"',
        to_path
    ])
    proc = remote.run(
        args=args,
        stdout=StringIO(),
    )
    perms = proc.stdout.getvalue().rstrip().strip('\"')

    args = []
    if sudo:
        args.append('sudo')
    args.extend([
        'mv',
        '--',
        from_path,
        to_path,
    ])
    proc = remote.run(
        args=args,
        stdout=StringIO(),
    )

    # reset the file back to the original permissions
    args = []
    if sudo:
        args.append('sudo')
    args.extend([
        'chmod',
        perms,
        to_path,
    ])
    proc = remote.run(
        args=args,
        stdout=StringIO(),
    )


def delete_file(remote, path, sudo=False, force=False):
    """
    rm a file on a remote site.
    """
    args = []
    if sudo:
        args.append('sudo')
    args.extend(['rm'])
    if force:
        args.extend(['-f'])
    args.extend([
        '--',
        path,
    ])
    remote.run(
        args=args,
        stdout=StringIO(),
    )


def remove_lines_from_file(remote, path, line_is_valid_test,
                           string_to_test_for):
    """
    Remove lines from a file.  This involves reading the file in, removing
    the appropriate lines, saving the file, and then replacing the original
    file with the new file.  Intermediate files are used to prevent data loss
    on when the main site goes up and down.
    """
    # read in the specified file
    in_data = get_file(remote, path, False)
    out_data = ""

    first_line = True
    # use the 'line_is_valid_test' function to remove unwanted lines
    for line in in_data.split('\n'):
        if line_is_valid_test(line, string_to_test_for):
            if not first_line:
                out_data += '\n'
            else:
                first_line = False

            out_data += '{line}'.format(line=line)

        else:
            log.info('removing line: {bad_line}'.format(bad_line=line))

    # get a temp file path on the remote host to write to,
    # we don't want to blow away the remote file and then have the
    # network drop out
    temp_file_path = remote.mktemp()

    # write out the data to a temp file
    write_file(remote, temp_file_path, out_data)

    # then do a 'mv' to the actual file location
    move_file(remote, temp_file_path, path)


def append_lines_to_file(remote, path, lines, sudo=False):
    """
    Append lines to a file.
    An intermediate file is used in the same manner as in
    Remove_lines_from_list.
    """

    temp_file_path = remote.mktemp()

    data = get_file(remote, path, sudo)

    # add the additional data and write it back out, using a temp file
    # in case of connectivity of loss, and then mv it to the
    # actual desired location
    data += lines
    write_file(remote, temp_file_path, data)

    # then do a 'mv' to the actual file location
    move_file(remote, temp_file_path, path, sudo)

def prepend_lines_to_file(remote, path, lines, sudo=False):
    """
    Prepend lines to a file.
    An intermediate file is used in the same manner as in
    Remove_lines_from_list.
    """

    temp_file_path = remote.mktemp()

    data = get_file(remote, path, sudo)

    # add the additional data and write it back out, using a temp file
    # in case of connectivity of loss, and then mv it to the
    # actual desired location
    data = lines + data
    write_file(remote, temp_file_path, data)

    # then do a 'mv' to the actual file location
    move_file(remote, temp_file_path, path, sudo)


def create_file(remote, path, data="", permissions=str(644), sudo=False):
    """
    Create a file on the remote host.
    """
    args = []
    if sudo:
        args.append('sudo')
    args.extend([
        'touch',
        path,
        run.Raw('&&')
    ])
    if sudo:
        args.append('sudo')
    args.extend([
        'chmod',
        permissions,
        '--',
        path
    ])
    remote.run(
        args=args,
        stdout=StringIO(),
    )
    # now write out the data if any was passed in
    if "" != data:
        append_lines_to_file(remote, path, data, sudo)


def get_file(remote, path, sudo=False, dest_dir='/tmp'):
    """
    Get the contents of a remote file. Do not use for large files; use
    Remote.get_file() instead.
    """
    local_path = remote.get_file(path, sudo=sudo, dest_dir=dest_dir)
    with open(local_path) as file_obj:
        file_data = file_obj.read()
    os.remove(local_path)
    return file_data


def pull_directory(remote, remotedir, localdir):
    """
    Copy a remote directory to a local directory.
    """
    log.debug('Transferring archived files from %s:%s to %s',
              remote.shortname, remotedir, localdir)
    if not os.path.exists(localdir):
        os.mkdir(localdir)
    _, local_tarfile = tempfile.mkstemp(dir=localdir)
    remote.get_tar(remotedir, local_tarfile, sudo=True)
    with open(local_tarfile, 'r+') as fb1:
        tar = tarfile.open(mode='r|gz', fileobj=fb1)
        while True:
            ti = tar.next()
            if ti is None:
                break

            if ti.isdir():
                # ignore silently; easier to just create leading dirs below
                pass
            elif ti.isfile():
                sub = safepath.munge(ti.name)
                safepath.makedirs(root=localdir, path=os.path.dirname(sub))
                tar.makefile(ti, targetpath=os.path.join(localdir, sub))
            else:
                if ti.isdev():
                    type_ = 'device'
                elif ti.issym():
                    type_ = 'symlink'
                elif ti.islnk():
                    type_ = 'hard link'
                else:
                    type_ = 'unknown'
                    log.info('Ignoring tar entry: %r type %r', ti.name, type_)
                    continue
    os.remove(local_tarfile)


def pull_directory_tarball(remote, remotedir, localfile):
    """
    Copy a remote directory to a local tarball.
    """
    log.debug('Transferring archived files from %s:%s to %s',
              remote.shortname, remotedir, localfile)
    remote.get_tar(remotedir, localfile, sudo=True)


def get_wwn_id_map(remote, devs):
    """
    Extract ww_id_map information from ls output on the associated devs.

    Sample dev information:    /dev/sdb: /dev/disk/by-id/wwn-0xf00bad

    :returns: map of devices to device id links
    """
    stdout = None
    try:
        r = remote.run(
            args=[
                'ls',
                '-l',
                '/dev/disk/by-id/wwn-*',
            ],
            stdout=StringIO(),
        )
        stdout = r.stdout.getvalue()
    except Exception:
        log.info('Failed to get wwn devices! Using /dev/sd* devices...')
        return dict((d, d) for d in devs)

    devmap = {}

    # lines will be:
    # lrwxrwxrwx 1 root root  9 Jan 22 14:58
    # /dev/disk/by-id/wwn-0x50014ee002ddecaf -> ../../sdb
    for line in stdout.splitlines():
        comps = line.split(' ')
        # comps[-1] should be:
        # ../../sdb
        rdev = comps[-1]
        # translate to /dev/sdb
        dev = '/dev/{d}'.format(d=rdev.split('/')[-1])

        # comps[-3] should be:
        # /dev/disk/by-id/wwn-0x50014ee002ddecaf
        iddev = comps[-3]

        if dev in devs:
            devmap[dev] = iddev

    return devmap


def get_scratch_devices(remote):
    """
    Read the scratch disk list from remote host
    """
    devs = []
    try:
        file_data = get_file(remote, "/scratch_devs")
        devs = file_data.split()
    except Exception:
        r = remote.run(
            args=['ls', run.Raw('/dev/[sv]d?')],
            stdout=StringIO()
        )
        devs = r.stdout.getvalue().strip().split('\n')

    # Remove root device (vm guests) from the disk list
    for dev in devs:
        if 'vda' in dev:
            devs.remove(dev)
            log.warn("Removing root device: %s from device list" % dev)

    log.debug('devs={d}'.format(d=devs))

    retval = []
    for dev in devs:
        try:
            # FIXME: Split this into multiple calls.
            remote.run(
                args=[
                    # node exists
                    'stat',
                    dev,
                    run.Raw('&&'),
                    # readable
                    'sudo', 'dd', 'if=%s' % dev, 'of=/dev/null', 'count=1',
                    run.Raw('&&'),
                    # not mounted
                    run.Raw('!'),
                    'mount',
                    run.Raw('|'),
                    'grep', '-q', dev,
                ]
            )
            retval.append(dev)
        except CommandFailedError:
            log.debug("get_scratch_devices: %s is in use" % dev)
    return retval


def wait_until_healthy(ctx, remote):
    """
    Wait until a Ceph cluster is healthy. Give up after 15min.
    """
    testdir = get_testdir(ctx)
    with safe_while(tries=(900 / 6), action="wait_until_healthy") as proceed:
        while proceed():
            r = remote.run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'ceph',
                    'health',
                ],
                stdout=StringIO(),
                logger=log.getChild('health'),
            )
            out = r.stdout.getvalue()
            log.debug('Ceph health: %s', out.rstrip('\n'))
            if out.split(None, 1)[0] == 'HEALTH_OK':
                break
            time.sleep(1)


def wait_until_osds_up(ctx, cluster, remote):
    """Wait until all Ceph OSDs are booted."""
    num_osds = num_instances_of_type(cluster, 'osd')
    testdir = get_testdir(ctx)
    while True:
        r = remote.run(
            args=[
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'ceph',
                'osd', 'dump', '--format=json'
            ],
            stdout=StringIO(),
            logger=log.getChild('health'),
        )
        out = r.stdout.getvalue()
        j = json.loads('\n'.join(out.split('\n')[1:]))
        up = len(filter(lambda o: 'up' in o['state'], j['osds']))
        log.debug('%d of %d OSDs are up' % (up, num_osds))
        if up == num_osds:
            break
        time.sleep(1)


def reboot(node, timeout=300, interval=30):
    """
    Reboots a given system, then waits for it to come back up and
    re-establishes the ssh connection.

    :param node: The teuthology.orchestra.remote.Remote object of the node
    :param timeout: The amount of time, in seconds, after which to give up
                    waiting for the node to return
    :param interval: The amount of time, in seconds, to wait between attempts
                     to re-establish with the node. This should not be set to
                     less than maybe 10, to make sure the node actually goes
                     down first.
    """
    log.info("Rebooting {host}...".format(host=node.hostname))
    node.run(args=['sudo', 'shutdown', '-r', 'now'])
    reboot_start_time = time.time()
    while time.time() - reboot_start_time < timeout:
        time.sleep(interval)
        if node.is_online or node.reconnect():
            return
    raise RuntimeError(
        "{host} did not come up after reboot within {time}s".format(
            host=node.hostname, time=timeout))


def reconnect(ctx, timeout, remotes=None):
    """
    Connect to all the machines in ctx.cluster.

    Presumably, some of them won't be up. Handle this
    by waiting for them, unless the wait time exceeds
    the specified timeout.

    ctx needs to contain the cluster of machines you
    wish it to try and connect to, as well as a config
    holding the ssh keys for each of them. As long as it
    contains this data, you can construct a context
    that is a subset of your full cluster.
    """
    log.info('Re-opening connections...')
    starttime = time.time()

    if remotes:
        need_reconnect = remotes
    else:
        need_reconnect = ctx.cluster.remotes.keys()

    while need_reconnect:
        for remote in need_reconnect:
            log.info('trying to connect to %s', remote.name)
            success = remote.reconnect()
            if not success:
                if time.time() - starttime > timeout:
                    raise RuntimeError("Could not reconnect to %s" %
                                       remote.name)
            else:
                need_reconnect.remove(remote)

        log.debug('waited {elapsed}'.format(
            elapsed=str(time.time() - starttime)))
        time.sleep(1)


def get_clients(ctx, roles):
    """
    return all remote roles that are clients.
    """
    for role in roles:
        assert isinstance(role, basestring)
        assert 'client.' in role
        _, _, id_ = split_role(role)
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        yield (id_, remote)


def get_user():
    """
    Return the username in the format user@host.
    """
    return getpass.getuser() + '@' + socket.gethostname()


def get_mon_names(ctx, cluster='ceph'):
    """
    :returns: a list of monitor names
    """
    is_mon = is_type('mon', cluster)
    host_mons = [[role for role in roles if is_mon(role)]
                 for roles in ctx.cluster.remotes.values()]
    return [mon for mons in host_mons for mon in mons]


def get_first_mon(ctx, config, cluster='ceph'):
    """
    return the "first" mon role (alphanumerically, for lack of anything better)
    """
    mons = get_mon_names(ctx, cluster)
    if mons:
        return sorted(mons)[0]
    assert False, 'no mon for cluster found'


def replace_all_with_clients(cluster, config):
    """
    Converts a dict containing a key all to one
    mapping all clients to the value of config['all']
    """
    assert isinstance(config, dict), 'config must be a dict'
    if 'all' not in config:
        return config
    norm_config = {}
    assert len(config) == 1, \
        "config cannot have 'all' and specific clients listed"
    for client in all_roles_of_type(cluster, 'client'):
        norm_config['client.{id}'.format(id=client)] = config['all']
    return norm_config


def deep_merge(a, b):
    """
    Deep Merge.  If a and b are both lists, all elements in b are
    added into a.  If a and b are both dictionaries, elements in b are
    recursively added to a.
    :param a: object items will be merged into
    :param b: object items will be merged from
    """
    if a is None:
        return b
    if b is None:
        return a
    if isinstance(a, list):
        assert isinstance(b, list)
        a.extend(b)
        return a
    if isinstance(a, dict):
        assert isinstance(b, dict)
        for (k, v) in b.iteritems():
            if k in a:
                a[k] = deep_merge(a[k], v)
            else:
                a[k] = v
        return a
    return b


def get_valgrind_args(testdir, name, preamble, v):
    """
    Build a command line for running valgrind.

    testdir - test results directory
    name - name of daemon (for naming hte log file)
    preamble - stuff we should run before valgrind
    v - valgrind arguments
    """
    if v is None:
        return preamble
    if not isinstance(v, list):
        v = [v]
    val_path = '/var/log/ceph/valgrind'.format(tdir=testdir)
    if '--tool=memcheck' in v or '--tool=helgrind' in v:
        extra_args = [
            'valgrind',
            '--trace-children=no',
            '--child-silent-after-fork=yes',
            '--num-callers=50',
            '--suppressions={tdir}/valgrind.supp'.format(tdir=testdir),
            '--xml=yes',
            '--xml-file={vdir}/{n}.log'.format(vdir=val_path, n=name),
            '--time-stamp=yes',
        ]
    else:
        extra_args = [
            'valgrind',
            '--trace-children=no',
            '--child-silent-after-fork=yes',
            '--suppressions={tdir}/valgrind.supp'.format(tdir=testdir),
            '--log-file={vdir}/{n}.log'.format(vdir=val_path, n=name),
            '--time-stamp=yes',
        ]
    args = [
        'cd', testdir,
        run.Raw('&&'),
    ] + preamble + extra_args + v
    log.debug('running %s under valgrind with args %s', name, args)
    return args


def ssh_keyscan(hostnames):
    """
    Fetch the SSH public key of one or more hosts
    """
    if isinstance(hostnames, basestring):
        raise TypeError("'hostnames' must be a list")
    hostnames = [canonicalize_hostname(name, user=None) for name in
                 hostnames]
    args = ['ssh-keyscan', '-T', '1', '-t', 'rsa'] + hostnames
    p = subprocess.Popen(
        args=args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    p.wait()

    keys_dict = dict()
    for line in p.stderr.readlines():
        line = line.strip()
        if line and not line.startswith('#'):
            log.error(line)
    for line in p.stdout.readlines():
        host, key = line.strip().split(' ', 1)
        keys_dict[host] = key
    return keys_dict


def ssh_keyscan_wait(hostname):
    """
    Run ssh-keyscan against a host, return True if it succeeds,
    False otherwise. Try again if ssh-keyscan timesout.
    :param hostname: on which ssh-keyscan is run
    """
    with safe_while(sleep=6, tries=100,
                    action="ssh_keyscan_wait " + hostname) as proceed:
        success = False
        while proceed():
            keys_dict = ssh_keyscan([hostname])
            if len(keys_dict) == 1:
                success = True
                break
            log.info("try ssh_keyscan again for " + str(hostname))
        return success

def stop_daemons_of_type(ctx, type_, cluster='ceph'):
    """
    :param type_: type of daemons to be stopped.
    """
    log.info('Shutting down %s daemons...' % type_)
    exc_info = (None, None, None)
    for daemon in ctx.daemons.iter_daemons_of_role(type_, cluster):
        try:
            daemon.stop()
        except (CommandFailedError,
                CommandCrashedError,
                ConnectionLostError):
            exc_info = sys.exc_info()
            log.exception('Saw exception from %s.%s', daemon.role, daemon.id_)
    if exc_info != (None, None, None):
        raise exc_info[0], exc_info[1], exc_info[2]


def get_system_type(remote, distro=False, version=False):
    """
    If distro, return distro.
    If version, return version (lsb_release -rs)
    If both, return both.
    If neither, return 'deb' or 'rpm' if distro is known to be one of those
    Finally, if unknown, return the unfiltered distro (from lsb_release -is)
    """
    r = remote.run(
        args=[
            'sudo', 'lsb_release', '-is',
        ],
        stdout=StringIO(),
    )
    system_value = r.stdout.getvalue().strip()
    log.debug("System to be installed: %s" % system_value)
    if version:
        v = remote.run(args=['sudo', 'lsb_release', '-rs'], stdout=StringIO())
        version = v.stdout.getvalue().strip()
    if distro and version:
        return system_value.lower(), version
    if distro:
        return system_value.lower()
    if version:
        return version
    if system_value in ['Ubuntu', 'Debian']:
        return "deb"
    if system_value in ['CentOS', 'Fedora', 'RedHatEnterpriseServer',
                        'openSUSE project', 'SUSE LINUX']:
        return "rpm"
    return system_value

def get_pkg_type(os_type):
    if os_type in ('centos', 'fedora', 'opensuse', 'rhel', 'sles'):
        return 'rpm'
    else:
        return 'deb'

def get_distro(ctx):
    """
    Get the name of the distro that we are using (usually the os_type).
    """
    os_type = None
    if ctx.os_type:
        return ctx.os_type

    try:
        os_type = ctx.config.get('os_type', None)
    except AttributeError:
        pass

    # if os_type is None, return the default of ubuntu
    return os_type or "ubuntu"


def get_distro_version(ctx):
    """
    Get the verstion of the distro that we are using (release number).
    """
    distro = get_distro(ctx)
    if ctx.os_version is not None:
        return str(ctx.os_version)
    try:
        os_version = ctx.config.get('os_version', DEFAULT_OS_VERSION[distro])
    except AttributeError:
        os_version = DEFAULT_OS_VERSION[distro]
    return str(os_version)


def get_multi_machine_types(machinetype):
    """
    Converts machine type string to list based on common deliminators
    """
    machinetypes = []
    machine_type_deliminator = [',', ' ', '\t']
    for deliminator in machine_type_deliminator:
        if deliminator in machinetype:
            machinetypes = machinetype.split(deliminator)
            break
    if not machinetypes:
        machinetypes.append(machinetype)
    return machinetypes


def is_in_dict(searchkey, searchval, d):
    """
    Test if searchkey/searchval are in dictionary.  searchval may
    itself be a dict, in which case, recurse.  searchval may be
    a subset at any nesting level (that is, all subkeys in searchval
    must be found in d at the same level/nest position, but searchval
    is not required to fully comprise d[searchkey]).

    >>> is_in_dict('a', 'foo', {'a':'foo', 'b':'bar'})
    True

    >>> is_in_dict(
    ...     'a',
    ...     {'sub1':'key1', 'sub2':'key2'},
    ...     {'a':{'sub1':'key1', 'sub2':'key2', 'sub3':'key3'}}
    ... )
    True

    >>> is_in_dict('a', 'foo', {'a':'bar', 'b':'foo'})
    False

    >>> is_in_dict('a', 'foo', {'a':{'a': 'foo'}})
    False
    """
    val = d.get(searchkey, None)
    if isinstance(val, dict) and isinstance(searchval, dict):
        for foundkey, foundval in searchval.iteritems():
            if not is_in_dict(foundkey, foundval, val):
                return False
        return True
    else:
        return searchval == val


def sh(command, log_limit=1024):
    """
    Run the shell command and return the output in ascii (stderr and
    stdout).  If the command fails, raise an exception. The command
    and its output are logged, on success and on error.
    """
    log.debug(":sh: " + command)
    proc = subprocess.Popen(
        args=command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        bufsize=1)
    lines = []
    truncated = False
    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            lines.append(line)
            line = line.strip()
            if len(line) > log_limit:
                truncated = True
                log.debug(str(line)[:log_limit] +
                          "... (truncated to the first " + str(log_limit) +
                          " characters)")
            else:
                log.debug(str(line))
    output = "".join(lines)
    if proc.wait() != 0:
        if truncated:
            log.error(command + " replay full stdout/stderr"
                      " because an error occurred and some of"
                      " it was truncated")
            log.error(output)
        raise subprocess.CalledProcessError(
            returncode=proc.returncode,
            cmd=command,
            output=output
        )
    return output.decode('utf-8')
