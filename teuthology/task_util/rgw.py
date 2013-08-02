from cStringIO import StringIO
import logging
import json
from urlparse import urlparse

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False):
    log.info('radosgw-admin: %s' % cmd)
    testdir = teuthology.get_testdir(ctx)
    pre = [
        '{tdir}/adjust-ulimits'.format(tdir=testdir),
        'ceph-coverage'.format(tdir=testdir),
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'radosgw-admin'.format(tdir=testdir),
        '--log-to-stderr',
        '--format', 'json',
        ]
    pre.extend(cmd)
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

def get_zone_host_and_port(ctx, client, zone):
    _, region_map = rgwadmin(ctx, client, check_status=True,
                             cmd=['-n', client, 'region-map', 'get'])
    regions = region_map['regions']
    for region in regions:
        for zone_info in region['val']['zones']:
            if zone_info['name'] == zone:
                endpoint = urlparse(zone_info['endpoints'][0])
                host, port = endpoint.hostname, endpoint.port
                if port is None:
                    port = 80
                return host, port
    assert False, 'no endpoint for zone {zone} found'.format(zone=zone)

def get_zone_system_keys(ctx, client, zone):
    _, zone_info = rgwadmin(ctx, client, check_status=True,
                            cmd=['-n', client,
                                 'zone', 'get', '--rgw-zone', zone])
    system_key = zone_info['system_key']
    return system_key['access_key'], system_key['secret_key']
