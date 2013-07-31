import contextlib
import json
import logging

from ..orchestra import run
from teuthology import misc as teuthology
from teuthology.task_util.rgw import rgwadmin

log = logging.getLogger(__name__)

def find_task_config(ctx, name):
    for entry in ctx.config['tasks']:
        log.info("checking entry: %s", entry)
        if name in entry:
            return entry[name]
    return dict()

@contextlib.contextmanager
def task(ctx, config):
    """
    Turn on a radosgw sync agent in test mode. Specify:
    host: to run on, or leave it blank for client.0
    source: the source region and zone
    target: the target region and zone

    tasks:
    - ceph:
    - rgw: <insert rgw region stuff here>
    - rgw_sync_agent:
        host: client.0
        source: client.0
        target: client.1
    """

    assert config is not None, "rgw_sync_agent requires a config"
    if not 'host' in config:
        log.info("setting blank host to be client.0")
        config['host'] = "client.0"

    log.info("config is %s", config)

    ceph_conf = find_task_config(ctx, 'ceph')['conf']
    log.info("ceph_conf is %s", ceph_conf)

    source = config['source']
    target = config['target']

    log.info("source is %s", source)
    log.info("target is %s", target)
    source_region_name = ceph_conf[source]['rgw region']
    target_region_name = ceph_conf[target]['rgw region']
    
    rgw_conf = find_task_config(ctx, 'rgw')
    log.info("rgw_conf is %s", rgw_conf)
    source_system_user = rgw_conf[source]['system user']['name']
    source_access_key = rgw_conf[source]['system user']['access key']
    source_secret_key = rgw_conf[source]['system user']['secret key']
    target_system_user = rgw_conf[target]['system user']['name']
    target_access_key = rgw_conf[target]['system user']['access key']
    target_secret_key = rgw_conf[target]['system user']['secret key']

    (sync_host,) = ctx.cluster.only(config['host']).remotes
    (source_host,) = ctx.cluster.only(source).remotes
    
    (error, source_region_json) = rgwadmin(ctx, source,
                                           cmd=['-n', source, 'region', 'get',
                                                '--rgw-region', source_region_name])
    log.info("got source_region_json %s", source_region_json)

    (source_host,) = source_region_json['endpoints']
    source_zone = source_region_json['master_zone']

    (error, target_region_json) = rgwadmin(ctx, target,
                                           cmd=['-n', target, 'region', 'get',
                                                '--rgw-region', target_region_name])

    (target_host,) = target_region_json['endpoints']
    target_zone = target_region_json['master_zone']
    
    log.info("got target_region_json %s", target_region_json)


    testdir = teuthology.get_testdir(ctx)

    sync_host.run(
        args=[
            'cd', testdir, run.Raw('&&'),
            'git', 'clone', 'https://github.com/ceph/radosgw-agent.git', run.Raw('&&'),
            'cd', "radosgw-agent", run.Raw('&&'),
            './bootstrap'
            ]
        )

    sync_proc = sync_host.run(
        args=[
            '{tdir}/daemon-helper'.format(tdir=testdir), 'kill',
            '{tdir}/radosgw-agent/radosgw-agent'.format(tdir=testdir),
            '--src-access-key', source_access_key,
            '--src-secret-key', source_secret_key,
            '--src-host', source_host, '--src-zone', source_zone,
            '--dest-access-key', target_access_key,
            '--dest-secret-key', target_secret_key,
            '--dest-host', target_host, '--dest-zone', target_zone,
            '--daemon-id', '{host}.syncdaemon'.format(host=sync_host.name),
            '--test-server-host', 'localhost', '--test-server-port', '8181',
            '--log-file', '{tdir}/archive/rgw_sync_agent.log'.format(tdir=testdir)
            ],
        wait=False,
        stdin=run.PIPE,
        logger=log.getChild(config['host'])
        )
    
    yield
    
    try:
        log.info("shutting down sync agent")
        sync_proc.stdin.close()
        log.info("waiting on sync agent")
        sync_proc.exitstatus.get()
    finally:
        log.info("cleaning up sync agent directory")
        sync_host.run(
            args=[
                'rm', '-r', '{tdir}/radosgw-agent'.format(tdir=testdir)
                ]
            )
    
