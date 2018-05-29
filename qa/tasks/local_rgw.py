import argparse
import contextlib
import logging

from teuthology import misc as teuthology
from util import get_remote_for_role

log = logging.getLogger(__name__)

class RGWEndpoint:
    def __init__(self, hostname=None, port=None, cert=None):
        self.hostname = hostname
        self.port = port
        self.cert = cert

    def url(self):
        proto = 'https' if self.cert else 'http'
        return '{proto}://{hostname}:{port}/'.format(proto=proto, hostname=self.hostname, port=self.port)

def assign_endpoints(ctx, config, default_cert):
    """
    Assign port numbers starting with port 8000.
    """
    port = 8000
    role_endpoints = {}

    for role, client_config in config.iteritems():
        client_config = client_config or {}
        remote = get_remote_for_role(ctx, role)

        cert = client_config.get('ssl certificate', default_cert)
        if cert:
            # find the certificate created by the ssl task
            if not hasattr(ctx, 'ssl_certificates'):
                raise ConfigError('rgw: no ssl task found for option "ssl certificate"')
            ssl_certificate = ctx.ssl_certificates.get(cert, None)
            if not ssl_certificate:
                raise ConfigError('rgw: missing ssl certificate "{}"'.format(cert))
        else:
            ssl_certificate = None

        role_endpoints[role] = RGWEndpoint(remote.hostname, port, ssl_certificate)
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

    ctx.rgw.use_fastcgi = True
    ctx.rgw.frontend = config.pop('frontend', 'civetweb')
    default_cert = config.pop('ssl certificate', None)

    role_endpoints = assign_endpoints(ctx, config,default_cert)
    ctx.rgw.role_endpoints = role_endpoints
    log.debug("role_endpoints %r", ctx.rgw.role_endpoints)

    yield
