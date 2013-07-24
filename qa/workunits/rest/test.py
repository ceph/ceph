#!/usr/bin/python

import exceptions
import os
# import nosetests
import requests
import subprocess
import sys
import time
import xml.etree.ElementTree

BASEURL = os.environ.get('BASEURL', 'http://localhost:5000/api/v0.1')

class MyException(Exception):
    pass

def fail(r, msg):
    print >> sys.stderr, 'FAILURE: url ', r.url
    print >> sys.stderr, msg
    print >> sys.stderr, 'Response content: ', r.content
    print >> sys.stderr, 'Headers: ', r.headers
    sys.exit(1)

def expect(url, method, respcode, contenttype, extra_hdrs=None, data=None):

    fdict = {'get':requests.get, 'put':requests.put}
    f = fdict[method.lower()]
    r = f(BASEURL + '/' + url, headers=extra_hdrs, data=data)

    print '{0}: {1} {2}'.format(url, contenttype, r.status_code)

    if r.status_code != respcode:
        fail(r, 'expected {0}, got {1}'.format(respcode, r.status_code))
    r_contenttype = r.headers['content-type']

    if contenttype in ['json', 'xml']:
        contenttype = 'application/' + contenttype
    elif contenttype:
        contenttype = 'text/' + contenttype

    if contenttype and r_contenttype != contenttype:
        fail(r,  'expected {0}, got "{1}"'.format(contenttype, r_contenttype))

    if contenttype.startswith('application'):
        if r_contenttype == 'application/json':
            # may raise
            try:
                assert(r.json != None)
            except Exception as e:
                fail(r, 'Invalid JSON returned: "{0}"'.format(str(e)))

        if r_contenttype == 'application/xml':
            try:
                # if it's there, squirrel it away for use in the caller
                r.tree = xml.etree.ElementTree.fromstring(r.content)
            except Exception as e:
                fail(r, 'Invalid XML returned: "{0}"'.format(str(e)))

    return r


JSONHDR={'accept':'application/json'}
XMLHDR={'accept':'application/xml'}

if __name__ == '__main__':

    expect('auth/export', 'GET', 200, 'plain')
    expect('auth/export.json', 'GET', 200, 'json')
    expect('auth/export.xml', 'GET', 200, 'xml')
    expect('auth/export', 'GET', 200, 'json', JSONHDR)
    expect('auth/export', 'GET', 200, 'xml', XMLHDR)

    expect('auth/add?entity=client.xx&'
           'caps=mon&caps=allow&caps=osd&caps=allow *', 'PUT', 200, 'json',
            JSONHDR)

    r = expect('auth/export?entity=client.xx', 'GET', 200, 'plain')
    # must use text/plain; default is application/x-www-form-urlencoded
    expect('auth/add?entity=client.xx', 'PUT', 200, 'plain',
           {'Content-Type':'text/plain'}, data=r.content)

    r = expect('auth/list', 'GET', 200, 'plain')
    assert('client.xx' in r.content)

    r = expect('auth/list.json', 'GET', 200, 'json')
    dictlist = r.json['output']['auth_dump']
    xxdict = [d for d in dictlist if d['entity'] == 'client.xx'][0]
    assert(xxdict)
    assert('caps' in xxdict)
    assert('mon' in xxdict['caps'])
    assert('osd' in xxdict['caps'])

    expect('auth/get-key?entity=client.xx', 'GET', 200, 'json', JSONHDR)
    expect('auth/print-key?entity=client.xx', 'GET', 200, 'json', JSONHDR)
    expect('auth/print_key?entity=client.xx', 'GET', 200, 'json', JSONHDR)

    expect('auth/caps?entity=client.xx&caps=osd&caps=allow rw', 'PUT', 200,
           'json', JSONHDR)
    r = expect('auth/list.json', 'GET', 200, 'json')
    dictlist = r.json['output']['auth_dump']
    xxdict = [d for d in dictlist if d['entity'] == 'client.xx'][0]
    assert(xxdict)
    assert('caps' in xxdict)
    assert(not 'mon' in xxdict['caps'])
    assert('osd' in xxdict['caps'])
    assert(xxdict['caps']['osd'] == 'allow rw')

    # export/import/export, compare
    r = expect('auth/export', 'GET', 200, 'plain')
    exp1 = r.content
    assert('client.xx' in exp1)
    r = expect('auth/import', 'PUT', 200, 'plain',
               {'Content-Type':'text/plain'}, data=r.content)
    r2 = expect('auth/export', 'GET', 200, 'plain')
    assert(exp1 == r2.content)
    expect('auth/del?entity=client.xx', 'PUT', 200, 'json', JSONHDR)

    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    assert('epoch' in r.json['output'])

    assert('GLOBAL' in expect('df', 'GET', 200, 'plain').content)
    assert('CATEGORY' in expect('df?detail=detail', 'GET', 200, 'plain').content)
    # test param with no value (treated as param=param)
    assert('CATEGORY' in expect('df?detail', 'GET', 200, 'plain').content)

    r = expect('df', 'GET', 200, 'json', JSONHDR)
    assert('total_used' in r.json['output']['stats'])
    r = expect('df', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/stats/stats/total_used') is not None)

    r = expect('df?detail', 'GET', 200, 'json', JSONHDR)
    assert('rd_kb' in r.json['output']['pools'][0]['stats'])
    r = expect('df?detail', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/stats/pools/pool/stats/rd_kb') is not None)

    expect('fsid', 'GET', 200, 'json', JSONHDR)
    expect('health', 'GET', 200, 'json', JSONHDR)
    expect('health?detail', 'GET', 200, 'json', JSONHDR)
    expect('health?detail', 'GET', 200, 'plain')

    # XXX no ceph -w equivalent yet

    expect('mds/cluster_down', 'PUT', 200, '')
    expect('mds/cluster_down', 'PUT', 200, '')
    expect('mds/cluster_up', 'PUT', 200, '')
    expect('mds/cluster_up', 'PUT', 200, '')

    expect('mds/compat/rm_incompat?feature=4', 'PUT', 200, '')
    expect('mds/compat/rm_incompat?feature=4', 'PUT', 200, '')

    r = expect('mds/compat/show', 'GET', 200, 'json', JSONHDR)
    assert('incompat' in r.json['output'])
    r = expect('mds/compat/show', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/mds_compat/incompat') is not None)

    # EEXIST from CLI
    expect('mds/deactivate?who=2', 'PUT', 400, '')

    r = expect('mds/dump.json', 'GET', 200, 'json')
    assert('created' in r.json['output'])
    current_epoch = r.json['output']['epoch']
    r = expect('mds/dump.xml', 'GET', 200, 'xml')
    assert(r.tree.find('output/mdsmap/created') is not None)

    r = expect('mds/getmap', 'GET', 200, '')
    assert(len(r.content) != 0)
    expect('mds/setmap?epoch={0}'.format(current_epoch + 1), 'PUT', 200,
           'plain', {'Content-Type':'text/plain'},
           data=r.content)
    expect('mds/newfs?metadata=0&data=1&sure=--yes-i-really-mean-it', 'PUT',
           200, '')
    expect('osd/pool/create?pool=data2&pg_num=10', 'PUT', 200, '')
    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    pools = r.json['output']['pools']
    poolnum = None
    for p in pools:
        if p['pool_name'] == 'data2':
            poolnum = p['pool']
            assert(p['pg_num'] == 10)
            break
    assert(poolnum is not None)
    expect('mds/add_data_pool?poolid={0}'.format(poolnum), 'PUT', 200, '')
    expect('mds/remove_data_pool?poolid={0}'.format(poolnum), 'PUT', 200, '')
    expect('osd/pool/delete?pool=data2&pool2=data2'
           '&sure=--yes-i-really-really-mean-it', 'PUT', 200, '')
    expect('mds/set_max_mds?maxmds=4', 'PUT', 200, '')
    r = expect('mds/dump.json', 'GET', 200, 'json')
    assert(r.json['output']['max_mds'] == 4)
    expect('mds/set_max_mds?maxmds=3', 'PUT', 200, '')
    r = expect('mds/stat.json', 'GET', 200, 'json')
    assert('info' in r.json['output']['mdsmap'])
    r = expect('mds/stat.xml', 'GET', 200, 'xml')
    assert(r.tree.find('output/mds_stat/mdsmap/info') is not None)

    # more content tests below, just check format here
    expect('mon/dump.json', 'GET', 200, 'json')
    expect('mon/dump.xml', 'GET', 200, 'xml')

    r = expect('mon/getmap', 'GET', 200, '')
    assert(len(r.content) != 0)
    r = expect('mon_status.json', 'GET', 200, 'json')
    assert('name' in r.json['output'])
    r = expect('mon_status.xml', 'GET', 200, 'xml')
    assert(r.tree.find('output/mon_status/name') is not None)

    bl = '192.168.0.1:0/1000'
    expect('osd/blacklist?blacklistop=add&addr=' + bl, 'PUT', 200, '')
    r = expect('osd/blacklist/ls.json', 'GET', 200, 'json')
    assert([b for b in r.json['output'] if b['addr'] == bl])
    expect('osd/blacklist?blacklistop=rm&addr=' + bl, 'PUT', 200, '')
    r = expect('osd/blacklist/ls.json', 'GET', 200, 'json')
    assert([b for b in r.json['output'] if b['addr'] == bl] == [])

    expect('osd/crush/tunables?profile=legacy', 'PUT', 200, '')
    expect('osd/crush/tunables?profile=bobtail', 'PUT', 200, '')

    expect('osd/scrub?who=0', 'PUT', 200, '')
    expect('osd/deep-scrub?who=0', 'PUT', 200, '')
    expect('osd/repair?who=0', 'PUT', 200, '')

    expect('osd/set?key=noup', 'PUT', 200, '')

    expect('osd/down?ids=0', 'PUT', 200, '')
    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['osds'][0]['osd'] == 0)
    assert(r.json['output']['osds'][0]['up'] == 0)

    expect('osd/unset?key=noup', 'PUT', 200, '')

    for i in range(0,100):
        r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
        assert(r.json['output']['osds'][0]['osd'] == 0)
        if r.json['output']['osds'][0]['up'] == 1:
            break
        else:
            print >> sys.stderr, "waiting for osd.0 to come back up"
            time.sleep(10)

    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['osds'][0]['osd'] == 0)
    assert(r.json['output']['osds'][0]['up'] == 1)

    r = expect('osd/find?id=1', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['osd'] == 1)

    expect('osd/out?ids=1', 'PUT', 200, '')
    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['osds'][1]['osd'] == 1)
    assert(r.json['output']['osds'][1]['in'] == 0)

    expect('osd/in?ids=1', 'PUT', 200, '')
    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['osds'][1]['osd'] == 1)
    assert(r.json['output']['osds'][1]['in'] == 1)

    r = expect('osd/find?id=0', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['osd'] == 0)

    r = expect('osd/getmaxosd', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/getmaxosd/max_osd') is not None)
    r = expect('osd/getmaxosd', 'GET', 200, 'json', JSONHDR)
    saved_maxosd = r.json['output']['max_osd']
    expect('osd/setmaxosd?newmax=10', 'PUT', 200, '')
    r = expect('osd/getmaxosd', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['max_osd'] == 10)
    expect('osd/setmaxosd?newmax={0}'.format(saved_maxosd), 'PUT', 200, '')
    r = expect('osd/getmaxosd', 'GET', 200, 'json', JSONHDR)
    assert(r.json['output']['max_osd'] == saved_maxosd)

    r = expect('osd/create', 'PUT', 200, 'json', JSONHDR)
    assert('osdid' in r.json['output'])
    osdid = r.json['output']['osdid']
    expect('osd/lost?id={0}'.format(osdid), 'PUT', 400, '')
    expect('osd/lost?id={0}&sure=--yes-i-really-mean-it'.format(osdid),
           'PUT', 200, 'json', JSONHDR)
    expect('osd/rm?ids={0}'.format(osdid), 'PUT', 200, '')
    r = expect('osd/ls', 'GET', 200, 'json', JSONHDR)
    assert(isinstance(r.json['output'], list))
    r = expect('osd/ls', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/osds/osd') is not None)


    expect('osd/pause', 'PUT', 200, '')
    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    assert('pauserd,pausewr' in r.json['output']['flags'])
    expect('osd/unpause', 'PUT', 200, '')
    r = expect('osd/dump', 'GET', 200, 'json', JSONHDR)
    assert('pauserd,pausewr' not in r.json['output']['flags'])

    r = expect('osd/tree', 'GET', 200, 'json', JSONHDR)
    assert('nodes' in r.json['output'])
    r = expect('osd/tree', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/tree/nodes') is not None)
    expect('osd/pool/mksnap?pool=data&snap=datasnap', 'PUT', 200, '')
    r = subprocess.call('rados -p data lssnap | grep -q datasnap', shell=True)
    assert(r == 0)
    expect('osd/pool/rmsnap?pool=data&snap=datasnap', 'PUT', 200, '')

    expect('osd/pool/create?pool=data2&pg_num=10', 'PUT', 200, '')
    r = expect('osd/lspools', 'GET', 200, 'json', JSONHDR)
    assert([p for p in r.json['output'] if p['poolname'] == 'data2'])
    expect('osd/pool/rename?srcpool=data2&destpool=data3', 'PUT', 200, '')
    r = expect('osd/lspools', 'GET', 200, 'json', JSONHDR)
    assert([p for p in r.json['output'] if p['poolname'] == 'data3'])
    expect('osd/pool/delete?pool=data3', 'PUT', 400, '')
    expect('osd/pool/delete?pool=data3&pool2=data3&sure=--yes-i-really-really-mean-it', 'PUT', 200, '')

    r = expect('osd/stat', 'GET', 200, 'json', JSONHDR)
    assert('num_up_osds' in r.json['output'])
    r = expect('osd/stat', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/osdmap/num_up_osds') is not None)

    r = expect('osd/ls', 'GET', 200, 'json', JSONHDR)
    for osdid in r.json['output']:
        # XXX no tell yet
        # expect('tell?target=osd.{0}&args=version'.format(osdid), 'PUT',
        #         200, '')
        print >> sys.stderr, 'would be telling osd.{0} version'.format(osdid)

    expect('pg/debug?debugop=unfound_objects_exist', 'GET', 200, '')
    expect('pg/debug?debugop=degraded_pgs_exist', 'GET', 200, '')
    expect('pg/deep-scrub?pgid=0.0', 'PUT', 200, '')
    r = expect('pg/dump', 'GET', 200, 'json', JSONHDR)
    assert('pg_stats_sum' in r.json['output'])
    r = expect('pg/dump', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/pg_map/pg_stats_sum') is not None)

    expect('pg/dump_json', 'GET', 200, 'json', JSONHDR)
    expect('pg/dump_pools_json', 'GET', 200, 'json', JSONHDR)
    expect('pg/dump_stuck?stuckops=inactive', 'GET', 200, '')
    expect('pg/dump_stuck?stuckops=unclean', 'GET', 200, '')
    expect('pg/dump_stuck?stuckops=stale', 'GET', 200, '')

    r = expect('pg/getmap', 'GET', 200, '')
    assert(len(r.content) != 0)

    r = expect('pg/map?pgid=0.0', 'GET', 200, 'json', JSONHDR)
    assert('acting' in r.json['output'])
    assert(r.json['output']['pgid'] == '0.0')
    r = expect('pg/map?pgid=0.0', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/pg_map/acting') is not None)
    assert(r.tree.find('output/pg_map/pgid').text == '0.0')

    expect('pg/repair?pgid=0.0', 'PUT', 200, '')
    expect('pg/scrub?pgid=0.0', 'PUT', 200, '')

    expect('pg/send_pg_creates', 'PUT', 200, '')

    expect('pg/set_full_ratio?ratio=0.90', 'PUT', 200, '')
    r = expect('pg/dump', 'GET', 200, 'json', JSONHDR)
    assert(float(r.json['output']['full_ratio']) == 0.90)
    expect('pg/set_full_ratio?ratio=0.95', 'PUT', 200, '')
    expect('pg/set_nearfull_ratio?ratio=0.90', 'PUT', 200, '')
    r = expect('pg/dump', 'GET', 200, 'json', JSONHDR)
    assert(float(r.json['output']['near_full_ratio']) == 0.90)
    expect('pg/set_full_ratio?ratio=0.85', 'PUT', 200, '')

    r = expect('pg/stat', 'GET', 200, 'json', JSONHDR)
    assert('pg_stats_sum' in r.json['output'])
    r = expect('pg/stat', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/pg_map/pg_stats_sum') is not None)

    expect('quorum?quorumcmd=enter', 'PUT', 200, 'json', JSONHDR)
    expect('quorum?quorumcmd=enter', 'PUT', 200, 'xml', XMLHDR)
    expect('quorum_status', 'GET', 200, 'json', JSONHDR)
    expect('quorum_status', 'GET', 200, 'xml', XMLHDR)

    # report's CRC needs to be handled
    # r = expect('report', 'GET', 200, 'json', JSONHDR)
    # assert('osd_stats' in r.json['output'])
    # r = expect('report', 'GET', 200, 'xml', XMLHDR)
    # assert(r.tree.find('output/report/osdmap') is not None)

    r = expect('status', 'GET', 200, 'json', JSONHDR)
    assert('osdmap' in r.json['output'])
    r = expect('status', 'GET', 200, 'xml', XMLHDR)
    assert(r.tree.find('output/status/osdmap') is not None)

    # XXX tell not implemented yet
    # r = expect('tell?target=osd.0&args=version', 'PUT', 200, '')
    # assert('ceph version' in r.content)
    # expect('tell?target=osd.999&args=version', 'PUT', 400, '')
    # expect('tell?target=osd.foo&args=version', 'PUT', 400, '')


    # r = expect('tell?target=osd.0&args=dump_get_recovery_stats', 'PUT', '200', '')
    # assert('Started' in r.content)

    expect('osd/reweight?id=0&weight=0.9', 'PUT', 200, '')
    expect('osd/reweight?id=0&weight=-1', 'PUT', 400, '')
    expect('osd/reweight?id=0&weight=1', 'PUT', 200, '')

    for v in ['pg_num', 'pgp_num', 'size', 'min_size', 'crash_replay_interval',
              'crush_ruleset']:
        r = expect('osd/pool/get.json?pool=data&var=' + v, 'GET', 200, 'json')
        assert(v in r.json['output'])

    r = expect('osd/pool/get.json?pool=data&var=size', 'GET', 200, 'json')
    assert(r.json['output']['size'] == 2)

    expect('osd/pool/set?pool=data&var=size&val=3', 'PUT', 200, 'plain')
    r = expect('osd/pool/get.json?pool=data&var=size', 'GET', 200, 'json')
    assert(r.json['output']['size'] == 3)

    expect('osd/pool/set?pool=data&var=size&val=2', 'PUT', 200, 'plain')
    r = expect('osd/pool/get.json?pool=data&var=size', 'GET', 200, 'json')
    assert(r.json['output']['size'] == 2)

    r = expect('osd/pool/get.json?pool=rbd&var=crush_ruleset', 'GET', 200, 'json')
    assert(r.json['output']['crush_ruleset'] == 2)

    expect('osd/thrash?num_epochs=10', 'PUT', 200, '')
    print 'OK'
