import argparse
import contextlib
import logging

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def assign_ports(ctx, config):
    """
    Assign port numberst starting with port 7280.
    """
    port = 8000
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for role in roles_for_host:
            if role in config:
                role_endpoints[role] = (remote.name.split('@')[1], port)
                port += 1

    return role_endpoints

@contextlib.contextmanager
def task(ctx, config):

    ctx.rgw = argparse.Namespace()
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                      for id_ in teuthology.all_roles_of_type(
                          ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    ctx.rgw.config = config

    log.debug("local rgw config is {}".format(ctx.rgw.config))

    clients_from_config = ctx.rgw.config.keys()
    # choose first client as default
    client = clients_from_config[0]
    log.debug('client is: %r', client)

    #role_endpoints = assign_ports(ctx, config)
    #ctx.rgw.role_endpoints = role_endpoints
    ctx.rgw.use_fastcgi = True
    ctx.rgw.frontend = config.pop('frontend', 'civetweb')

    ctx.rgw.role_endpoints = {}
    ctx.rgw.role_endpoints[client] = ('localhost', 8000)
    log.debug("role_endpoints {}".format(ctx.rgw.role_endpoints))

    yield
