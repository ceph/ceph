from cStringIO import StringIO
import logging
import json
import requests
from requests.packages.urllib3.util import Retry
from urlparse import urlparse

from teuthology.orchestra.connection import split_user
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def multi_region_enabled(ctx):
    # this is populated by the radosgw-agent task, seems reasonable to
    # use that as an indicator that we're testing multi-region sync
    return 'radosgw_agent' in ctx

def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False,
             format='json'):
    log.info('rgwadmin: {client} : {cmd}'.format(client=client,cmd=cmd))
    testdir = teuthology.get_testdir(ctx)
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    client_with_id = daemon_type + '.' + client_id
    pre = [
        'adjust-ulimits',
        'ceph-coverage'.format(tdir=testdir),
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'radosgw-admin'.format(tdir=testdir),
        '--log-to-stderr',
        '--format', format,
        '-n',  client_with_id,
        '--cluster', cluster_name,
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
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    client_with_id = daemon_type + '.' + client_id
    _, period = rgwadmin(ctx, client, check_status=True,
                         cmd=['period', 'get'])
    period_map = period['period_map']
    zonegroups = period_map['zonegroups']
    for zonegroup in zonegroups:
        for zone_info in zonegroup['zones']:
            if zone_info['name'] == zone:
                endpoint = urlparse(zone_info['endpoints'][0])
                host, port = endpoint.hostname, endpoint.port
                if port is None:
                    port = 80
                return host, port
    assert False, 'no endpoint for zone {zone} found'.format(zone=zone)

def get_master_zone(ctx, client):
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    client_with_id = daemon_type + '.' + client_id
    _, period = rgwadmin(ctx, client, check_status=True,
                         cmd=['period', 'get'])
    period_map = period['period_map']
    zonegroups = period_map['zonegroups']
    for zonegroup in zonegroups:
        is_master = (zonegroup['is_master'] == "true")
        log.info('zonegroup={z} is_master={ism}'.format(z=zonegroup, ism=is_master))
        if not is_master:
          continue
        master_zone = zonegroup['master_zone']
        log.info('master_zone=%s' % master_zone)
        for zone_info in zonegroup['zones']:
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
                            cmd=['zone', 'get', '--rgw-zone', zone])
    system_key = zone_info['system_key']
    return system_key['access_key'], system_key['secret_key']

def zone_for_client(ctx, client):
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    ceph_config = ctx.ceph[cluster_name].conf.get('global', {})
    ceph_config.update(ctx.ceph[cluster_name].conf.get('client', {}))
    ceph_config.update(ctx.ceph[cluster_name].conf.get(client, {}))
    return ceph_config.get('rgw zone')

def region_for_client(ctx, client):
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    ceph_config = ctx.ceph[cluster_name].conf.get('global', {})
    ceph_config.update(ctx.ceph[cluster_name].conf.get('client', {}))
    ceph_config.update(ctx.ceph[cluster_name].conf.get(client, {}))
    return ceph_config.get('rgw region')

def radosgw_data_log_window(ctx, client):
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    ceph_config = ctx.ceph[cluster_name].conf.get('global', {})
    ceph_config.update(ctx.ceph[cluster_name].conf.get('client', {}))
    ceph_config.update(ctx.ceph[cluster_name].conf.get(client, {}))
    return ceph_config.get('rgw data log window', 30)

def radosgw_agent_sync_data(ctx, agent_host, agent_port, full=False):
    log.info('sync agent {h}:{p}'.format(h=agent_host, p=agent_port))
    # use retry with backoff to tolerate slow startup of radosgw-agent
    s = requests.Session()
    s.mount('http://{addr}:{port}/'.format(addr = agent_host, port = agent_port),
            requests.adapters.HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))
    method = "full" if full else "incremental"
    return s.post('http://{addr}:{port}/data/{method}'.format(addr = agent_host, port = agent_port, method = method))

def radosgw_agent_sync_metadata(ctx, agent_host, agent_port, full=False):
    log.info('sync agent {h}:{p}'.format(h=agent_host, p=agent_port))
    # use retry with backoff to tolerate slow startup of radosgw-agent
    s = requests.Session()
    s.mount('http://{addr}:{port}/'.format(addr = agent_host, port = agent_port),
            requests.adapters.HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))
    method = "full" if full else "incremental"
    return s.post('http://{addr}:{port}/metadata/{method}'.format(addr = agent_host, port = agent_port, method = method))

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

def extract_zone_info(ctx, client, client_config):
    """
    Get zone information.
    :param client: dictionary of client information
    :param client_config: dictionary of client configuration information
    :returns: zone extracted from client and client_config information
    """
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    client_with_id = daemon_type + '.' + client_id
    ceph_config = ctx.ceph[cluster_name].conf.get('global', {})
    ceph_config.update(ctx.ceph[cluster_name].conf.get('client', {}))
    ceph_config.update(ctx.ceph[cluster_name].conf.get(client_with_id, {}))
    for key in ['rgw zone', 'rgw region', 'rgw zone root pool']:
        assert key in ceph_config, \
            'ceph conf must contain {key} for {client}'.format(key=key,
                                                               client=client)
    region = ceph_config['rgw region']
    zone = ceph_config['rgw zone']
    zone_info = dict()
    for key in ['rgw control pool', 'rgw gc pool', 'rgw log pool',
                'rgw intent log pool', 'rgw usage log pool',
                'rgw user keys pool', 'rgw user email pool',
                'rgw user swift pool', 'rgw user uid pool',
                'rgw domain root']:
        new_key = key.split(' ', 1)[1]
        new_key = new_key.replace(' ', '_')

        if key in ceph_config:
            value = ceph_config[key]
            log.debug('{key} specified in ceph_config ({val})'.format(
                key=key, val=value))
            zone_info[new_key] = value
        else:
            zone_info[new_key] = '.' + region + '.' + zone + '.' + new_key

    index_pool = '.' + region + '.' + zone + '.' + 'index_pool'
    data_pool = '.' + region + '.' + zone + '.' + 'data_pool'
    data_extra_pool = '.' + region + '.' + zone + '.' + 'data_extra_pool'
    compression_type = ceph_config.get('rgw compression type', '')

    zone_info['placement_pools'] = [{'key': 'default_placement',
                                     'val': {'index_pool': index_pool,
                                             'data_pool': data_pool,
                                             'data_extra_pool': data_extra_pool,
                                             'compression': compression_type}
                                     }]

    # these keys are meant for the zones argument in the region info.  We
    # insert them into zone_info with a different format and then remove them
    # in the fill_in_endpoints() method
    for key in ['rgw log meta', 'rgw log data']:
        if key in ceph_config:
            zone_info[key] = ceph_config[key]

    # these keys are meant for the zones argument in the region info.  We
    # insert them into zone_info with a different format and then remove them
    # in the fill_in_endpoints() method
    for key in ['rgw log meta', 'rgw log data']:
        if key in ceph_config:
            zone_info[key] = ceph_config[key]

    return region, zone, zone_info

def extract_region_info(region, region_info):
    """
    Extract region information from the region_info parameter, using get
    to set default values.

    :param region: name of the region
    :param region_info: region information (in dictionary form).
    :returns: dictionary of region information set from region_info, using
            default values for missing fields.
    """
    assert isinstance(region_info['zones'], list) and region_info['zones'], \
        'zones must be a non-empty list'
    return dict(
        name=region,
        api_name=region_info.get('api name', region),
        is_master=region_info.get('is master', False),
        log_meta=region_info.get('log meta', False),
        log_data=region_info.get('log data', False),
        master_zone=region_info.get('master zone', region_info['zones'][0]),
        placement_targets=region_info.get('placement targets',
                                          [{'name': 'default_placement',
                                            'tags': []}]),
        default_placement=region_info.get('default placement',
                                          'default_placement'),
        )

def get_config_master_client(ctx, config, regions):

    role_zones = dict([(client, extract_zone_info(ctx, client, c_config))
                       for client, c_config in config.iteritems()])
    log.debug('roles_zones = %r', role_zones)
    region_info = dict([
        (region_name, extract_region_info(region_name, r_config))
        for region_name, r_config in regions.iteritems()])

     # read master zonegroup and master_zone
    for zonegroup, zg_info in region_info.iteritems():
        if zg_info['is_master']:
            master_zonegroup = zonegroup
            master_zone = zg_info['master_zone']
            break

    for client in config.iterkeys():
        (zonegroup, zone, zone_info) = role_zones[client]
        if zonegroup == master_zonegroup and zone == master_zone:
            return client

    return None

