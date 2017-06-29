import argparse
import contextlib

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
    #role_endpoints = assign_ports(ctx, config)
    #ctx.rgw.role_endpoints = role_endpoints
    ctx.rgw.use_fastcgi = True
    yield
