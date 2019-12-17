"""
Functions to netsplit test machines.

At present, you must specify monitors to disconnect, and it
drops those IP pairs. This means OSDs etc on the hosts which use
the same IP will also be blocked! If you are using multiple IPs on the
same host within the cluster, daemons on those other IPs will get
through.
"""
import logging
import re

log = logging.getLogger(__name__)

def get_ip_and_ports(ctx, daemon):
    assert daemon.startswith('mon.')
    addr = ctx.ceph['ceph'].mons['{a}'.format(a=daemon)]
    ips = re.findall("[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+[:[0-9]*]*", addr)
    assert len(ips) > 0
    plain_ip = re.match("[0-9\.]*", ips[0]).group()
    assert plain_ip is not None
    port_list = []
    for ip in ips:
        ip_str, port_str = re.match("([0-9\.]*)([:[0-9]*]*)", ip).groups()
        assert ip_str == plain_ip
        if len(port_str) > 0:
            port_list.append(port_str)
    return (plain_ip, port_list)

def disconnect(ctx, config):
    assert len(config) == 2 # we can only disconnect pairs right now
    # and we can only disconnect mons right now
    assert config[0].startswith('mon.')
    assert config[1].startswith('mon.')
    (ip1, _) = get_ip_and_ports(ctx, config[0])
    (ip2, _) = get_ip_and_ports(ctx, config[1])

    (host1,) = ctx.cluster.only(config[0]).remotes.iterkeys()
    (host2,) = ctx.cluster.only(config[1]).remotes.iterkeys()
    assert host1 is not None
    assert host2 is not None

    host1.run(
        args = ["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "-s",
                ip2, "-j", "DROP"]
    )
    host2.run(
        args = ["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "-s",
                ip1, "-j", "DROP"]
    )

def reconnect(ctx, config):
    assert len(config) == 2 # we can only disconnect pairs right now
    # and we can only disconnect mons right now
    assert config[0].startswith('mon.')
    assert config[1].startswith('mon.')

    (ip1, _) = get_ip_and_ports(ctx, config[0])
    (ip2, _) = get_ip_and_ports(ctx, config[1])

    (host1,) = ctx.cluster.only(config[0]).remotes.iterkeys()
    (host2,) = ctx.cluster.only(config[1]).remotes.iterkeys()
    assert host1 is not None
    assert host2 is not None

    host1.run(
        args = ["sudo", "iptables", "-D", "INPUT", "-p", "tcp", "-s",
                ip2, "-j", "DROP"]
    )
    host2.run(
        args = ["sudo", "iptables", "-D", "INPUT", "-p", "tcp", "-s",
                ip1, "-j", "DROP"]
    )
