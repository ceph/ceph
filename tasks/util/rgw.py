from cStringIO import StringIO
import logging
import json
import requests
from urlparse import urlparse

from teuthology.orchestra.connection import split_user
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

# simple test to indicate if multi-region testing should occur
def multi_region_enabled(ctx):
    # this is populated by the radosgw-agent task, seems reasonable to
    # use that as an indicator that we're testing multi-region sync
    return 'radosgw_agent' in ctx

def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False,
             format='json'):
    log.info('rgwadmin: {client} : {cmd}'.format(client=client,cmd=cmd))
    testdir = teuthology.get_testdir(ctx)
    pre = [
        'adjust-ulimits',
        'ceph-coverage'.format(tdir=testdir),
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'radosgw-admin'.format(tdir=testdir),
        '--log-to-stderr',
        '--format', format,
        '-n',  client,
        ]
    pre.extend(cmd)
    log.info('rgwadmin: cmd=%s' % pre)
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    proc = remote.run(
        args=pre,
        check_status=check_status,
        stdout=StringIO(),
        stderr=StringIO(),
        stdin=stdin,
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.info(' json result: %s' % j)
        except ValueError:
            j = out
            log.info(' raw result: %s' % j)
    return (r, j)

def get_user_summary(out, user):
    """Extract the summary for a given user"""
    user_summary = None
    for summary in out['summary']:
        if summary.get('user') == user:
            user_summary = summary

    if not user_summary:
        raise AssertionError('No summary info found for user: %s' % user)

    return user_summary

def get_user_successful_ops(out, user):
    summary = out['summary']
    if len(summary) == 0:
        return 0
    return get_user_summary(out, user)['total']['successful_ops']

def get_zone_host_and_port(ctx, client, zone):
    _, region_map = rgwadmin(ctx, client, check_status=True,
                             cmd=['-n', client, 'region-map', 'get'])
    regions = region_map['zonegroups']
    for region in regions:
        for zone_info in region['val']['zones']:
            if zone_info['name'] == zone:
                endpoint = urlparse(zone_info['endpoints'][0])
                host, port = endpoint.hostname, endpoint.port
                if port is None:
                    port = 80
                return host, port
    assert False, 'no endpoint for zone {zone} found'.format(zone=zone)

def get_master_zone(ctx, client):
    _, region_map = rgwadmin(ctx, client, check_status=True,
                             cmd=['-n', client, 'region-map', 'get'])
    regions = region_map['zonegroups']
    for region in regions:
        is_master = (region['val']['is_master'] == "true")
        log.info('region={r} is_master={ism}'.format(r=region, ism=is_master))
        if not is_master:
          continue
        master_zone = region['val']['master_zone']
        log.info('master_zone=%s' % master_zone)
        for zone_info in region['val']['zones']:
            if zone_info['name'] == master_zone:
                return master_zone
    log.info('couldn\'t find master zone')
    return None

def get_master_client(ctx, clients):
    master_zone = get_master_zone(ctx, clients[0]) # can use any client for this as long as system configured correctly
    if not master_zone:
        return None

    for client in clients:
        zone = zone_for_client(ctx, client)
        if zone == master_zone:
            return client

    return None

def get_zone_system_keys(ctx, client, zone):
    _, zone_info = rgwadmin(ctx, client, check_status=True,
                            cmd=['-n', client,
                                 'zone', 'get', '--rgw-zone', zone])
    system_key = zone_info['system_key']
    return system_key['access_key'], system_key['secret_key']

def zone_for_client(ctx, client):
    ceph_config = ctx.ceph['ceph'].conf.get('global', {})
    ceph_config.update(ctx.ceph['ceph'].conf.get('client', {}))
    ceph_config.update(ctx.ceph['ceph'].conf.get(client, {}))
    return ceph_config.get('rgw zone')

def region_for_client(ctx, client):
    ceph_config = ctx.ceph['ceph'].conf.get('global', {})
    ceph_config.update(ctx.ceph['ceph'].conf.get('client', {}))
    ceph_config.update(ctx.ceph['ceph'].conf.get(client, {}))
    return ceph_config.get('rgw region')

def radosgw_data_log_window(ctx, client):
    ceph_config = ctx.ceph['ceph'].conf.get('global', {})
    ceph_config.update(ctx.ceph['ceph'].conf.get('client', {}))
    ceph_config.update(ctx.ceph['ceph'].conf.get(client, {}))
    return ceph_config.get('rgw data log window', 30)

def radosgw_agent_sync_data(ctx, agent_host, agent_port, full=False):
    log.info('sync agent {h}:{p}'.format(h=agent_host, p=agent_port))
    method = "full" if full else "incremental"
    return requests.post('http://{addr}:{port}/data/{method}'.format(addr = agent_host, port = agent_port, method = method))

def radosgw_agent_sync_metadata(ctx, agent_host, agent_port, full=False):
    log.info('sync agent {h}:{p}'.format(h=agent_host, p=agent_port))
    method = "full" if full else "incremental"
    return requests.post('http://{addr}:{port}/metadata/{method}'.format(addr = agent_host, port = agent_port, method = method))

def radosgw_agent_sync_all(ctx, full=False, data=False):
    if ctx.radosgw_agent.procs:
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            zone_for_client(ctx, agent_client)
            sync_host, sync_port = get_sync_agent(ctx, agent_client)
            log.debug('doing a sync via {host1}'.format(host1=sync_host))
            radosgw_agent_sync_metadata(ctx, sync_host, sync_port, full)
            if (data):
                radosgw_agent_sync_data(ctx, sync_host, sync_port, full)

def host_for_role(ctx, role):
    for target, roles in zip(ctx.config['targets'].iterkeys(), ctx.config['roles']):
        if role in roles:
            _, host = split_user(target)
            return host

def get_sync_agent(ctx, source):
    for task in ctx.config['tasks']:
        if 'radosgw-agent' not in task:
            continue
        for client, conf in task['radosgw-agent'].iteritems():
            if conf['src'] == source:
                return host_for_role(ctx, source), conf.get('port', 8000)
    return None, None
