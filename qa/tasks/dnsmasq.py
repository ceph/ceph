"""
Task for dnsmasq configuration
"""
import contextlib
import logging

from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology import contextutil
from teuthology import packaging
from util import get_remote_for_role

log = logging.getLogger(__name__)

@contextlib.contextmanager
def install_dnsmasq(remote):
    """
    If dnsmasq is not installed, install it for the duration of the task.
    """
    try:
        existing = packaging.get_package_version(remote, 'dnsmasq')
    except:
        existing = None

    if existing is None:
        packaging.install_package('dnsmasq', remote)
    try:
        yield
    finally:
        if existing is None:
            packaging.remove_package('dnsmasq', remote)

@contextlib.contextmanager
def backup_resolv(remote, path):
    """
    Store a backup of resolv.conf in the testdir and restore it after the task.
    """
    remote.run(args=['cp', '/etc/resolv.conf', path])
    try:
        yield
    finally:
        # restore with 'cp' to avoid overwriting its security context
        remote.run(args=['sudo', 'cp', path, '/etc/resolv.conf'])
        remote.run(args=['rm', path])

@contextlib.contextmanager
def replace_resolv(remote, path):
    """
    Update resolv.conf to point the nameserver at localhost.
    """
    misc.write_file(remote, path, "nameserver 127.0.0.1\n")
    try:
        # install it
        remote.run(args=['sudo', 'cp', path, '/etc/resolv.conf'])
        yield
    finally:
        remote.run(args=['rm', path])

@contextlib.contextmanager
def setup_dnsmasq(remote, testdir, cnames):
    """ configure dnsmasq on the given remote, adding each cname given """
    log.info('Configuring dnsmasq on remote %s..', remote.name)

    # add address entries for each cname
    dnsmasq = "server=8.8.8.8\nserver=8.8.4.4\n"
    address_template = "address=/{cname}/{ip_address}\n"
    for cname, ip_address in cnames.iteritems():
        dnsmasq += address_template.format(cname=cname, ip_address=ip_address)

    # write to temporary dnsmasq file
    dnsmasq_tmp = '/'.join((testdir, 'ceph.tmp'))
    misc.write_file(remote, dnsmasq_tmp, dnsmasq)

    # move into /etc/dnsmasq.d/
    dnsmasq_path = '/etc/dnsmasq.d/ceph'
    remote.run(args=['sudo', 'mv', dnsmasq_tmp, dnsmasq_path])
    # restore selinux context if necessary
    remote.run(args=['sudo', 'restorecon', dnsmasq_path], check_status=False)

    # restart dnsmasq
    remote.run(args=['sudo', 'systemctl', 'restart', 'dnsmasq'])
    # verify dns name is set
    remote.run(args=['ping', '-c', '4', cnames.keys()[0]])

    try:
        yield
    finally:
        log.info('Removing dnsmasq configuration from remote %s..', remote.name)
        # remove /etc/dnsmasq.d/ceph
        remote.run(args=['sudo', 'rm', dnsmasq_path])
        # restart dnsmasq
        remote.run(args=['sudo', 'systemctl', 'restart', 'dnsmasq'])

@contextlib.contextmanager
def task(ctx, config):
    """
    Configures dnsmasq to add cnames for teuthology remotes. The task expects a
    dictionary, where each key is a role. If all cnames for that role use the
    same address as that role, the cnames can be given as a list. For example,
    this entry configures dnsmasq on the remote associated with client.0, adding
    two cnames for the ip address associated with client.0:

        - dnsmasq:
            client.0:
            - client0.example.com
            - c0.example.com

    If the addresses do not all match the given role, a dictionary can be given
    to specify the ip address by its target role. For example:

        - dnsmasq:
            client.0:
              client.0.example.com: client.0
              client.1.example.com: client.1

    Cnames that end with a . are treated as prefix for the existing hostname.
    For example, if the remote for client.0 has a hostname of 'example.com',
    this task will add cnames for dev.example.com and test.example.com:

        - dnsmasq:
            client.0: [dev., test.]
    """
    # apply overrides
    overrides = config.get('overrides', {})
    misc.deep_merge(config, overrides.get('dnsmasq', {}))

    # multiple roles may map to the same remote, so collect names by remote
    remote_names = {}
    for role, cnames in config.iteritems():
        remote = get_remote_for_role(ctx, role)
        if remote is None:
            raise ConfigError('no remote for role %s' % role)

        names = remote_names.get(remote, {})

        if isinstance(cnames, list):
            # when given a list of cnames, point to local ip
            for cname in cnames:
                if cname.endswith('.'):
                    cname += remote.hostname
                names[cname] = remote.ip_address
        elif isinstance(cnames, dict):
            # when given a dict, look up the remote ip for each
            for cname, client in cnames.iteritems():
                r = get_remote_for_role(ctx, client)
                if r is None:
                    raise ConfigError('no remote for role %s' % client)
                if cname.endswith('.'):
                    cname += r.hostname
                names[cname] = r.ip_address

        remote_names[remote] = names

    testdir = misc.get_testdir(ctx)
    resolv_bak = '/'.join((testdir, 'resolv.bak'))
    resolv_tmp = '/'.join((testdir, 'resolv.tmp'))

    # run subtasks for each unique remote
    subtasks = []
    for remote, cnames in remote_names.iteritems():
        subtasks.extend([ lambda r=remote: install_dnsmasq(r) ])
        subtasks.extend([ lambda r=remote: backup_resolv(r, resolv_bak) ])
        subtasks.extend([ lambda r=remote: replace_resolv(r, resolv_tmp) ])
        subtasks.extend([ lambda r=remote, cn=cnames: setup_dnsmasq(r, testdir, cn) ])

    with contextutil.nested(*subtasks):
        yield
