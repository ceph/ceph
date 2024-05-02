import logging
import yaml

from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def _shell(ctx, cluster_name, remote, args, extra_cephadm_args=[], **kwargs):
    teuthology.get_testdir(ctx)
    return remote.run(
        args=[
            'sudo',
            ctx.cephadm,
            '--image', ctx.ceph[cluster_name].image,
            'shell',
            '-c', '/etc/ceph/{}.conf'.format(cluster_name),
            '-k', '/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
            '--fsid', ctx.ceph[cluster_name].fsid,
            ] + extra_cephadm_args + [
            '--',
            ] + args,
        **kwargs
    )


def apply(ctx, config):
    """
    Apply spec

      tasks:
        - rgw_module.apply:
            specs:
            - rgw_realm: myrealm1
              rgw_zonegroup: myzonegroup1
              rgw_zone: myzone1
              placement:
                hosts:
                 - ceph-node-0
                 - ceph-node-1
              spec:
                rgw_frontend_port: 5500
    """
    cluster_name = config.get('cluster', 'ceph')
    specs = config.get('specs', [])
    y = yaml.dump_all(specs)
    log.info(f'Applying spec(s):\n{y}')
    _shell(
        ctx, cluster_name, ctx.ceph[cluster_name].bootstrap_remote,
        ['ceph', 'rgw', 'realm', 'bootstrap', '-i', '-'],
        stdin=y,
    )
