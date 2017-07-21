"""
Task for dnsmasq configuration
"""
import contextlib
import logging

from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology import contextutil
from util import get_remote_for_role

log = logging.getLogger(__name__)

@contextlib.contextmanager
def setup_dnsmasq(remote, cnames):
    """ configure dnsmasq on the given remote, adding each cname given """
    log.info('Configuring dnsmasq on remote %s..', remote.name)

    # back up existing resolv.conf
    resolv_conf = misc.get_file(remote, '/etc/resolv.conf')
    # point resolv.conf to local dnsmasq
    misc.sudo_write_file(remote, '/etc/resolv.conf',
                         "nameserver 127.0.0.1\n")

    # add address entries to /etc/dnsmasq.d/ceph
    dnsmasq = "server=8.8.8.8\nserver=8.8.4.4\n"
    address_template = "address=/{cname}/{ip_address}\n"
    for cname, ip_address in cnames.iteritems():
        dnsmasq += address_template.format(cname=cname, ip_address=ip_address)
    misc.sudo_write_file(remote, '/etc/dnsmasq.d/ceph', dnsmasq)

    remote.run(args=['cat', '/etc/dnsmasq.d/ceph'])
    # restart dnsmasq
    remote.run(args=['sudo', 'systemctl', 'restart', 'dnsmasq'])
    remote.run(args=['sudo', 'systemctl', 'status', 'dnsmasq'])
    # verify dns name is set
    remote.run(args=['ping', '-c', '4', cnames.keys()[0]])

    yield

    log.info('Removing dnsmasq configuration from remote %s..', remote.name)
    # restore resolv.conf
    misc.sudo_write_file(remote, '/etc/resolv.conf', resolv_conf)
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
                names[cname] = remote.ip_address
        elif isinstance(cnames, dict):
            # when given a dict, look up the remote ip for each
            for cname, client in cnames.iteritems():
                r = get_remote_for_role(ctx, client)
                if r is None:
                    raise ConfigError('no remote for role %s' % client)
                names[cname] = r.ip_address

        remote_names[remote] = names

    # run a subtask for each unique remote
    subtasks = []
    for remote, cnames in remote_names.iteritems():
        subtasks.extend([ lambda r=remote, cn=cnames: setup_dnsmasq(r, cn) ])

    with contextutil.nested(*subtasks):
        yield
