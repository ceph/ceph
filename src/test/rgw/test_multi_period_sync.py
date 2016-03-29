import os
import json
import argparse
import sys
import ConfigParser

from rgw_multi import *

log_level = 20

def test_multi_period_incremental_sync():
    global realm
    if len(realm.clusters) < 3:
        from nose.plugins.skip import SkipTest
        raise SkipTest("test_multi_period_incremental_sync skipped. Requires 3 or more clusters.")

    global user
    buckets, zone_bucket = create_bucket_per_zone(user, realm)

    all_zones = []
    for z in zone_bucket:
        all_zones.append(z)

    for zone, bucket_name in zone_bucket.iteritems():
        for objname in [ 'p1', '_p1' ]:
            k = new_key(user, zone, bucket_name, objname)
            k.set_contents_from_string('asdasd')
    realm.meta_checkpoint()

    # kill zone 3 gateway to freeze sync status to incremental in first period
    z3 = realm.get_zone(2)
    z3.cluster.stop_rgw()

    # change master to zone 2 -> period 2
    realm.set_master_zone(realm.get_zone(1))

    for zone, bucket_name in zone_bucket.iteritems():
        if zone == z3:
            continue
        for objname in [ 'p2', '_p2' ]:
            k = new_key(user, zone, bucket_name, objname)
            k.set_contents_from_string('qweqwe')

    # wait for zone 1 to sync
    realm.zone_meta_checkpoint(realm.get_zone(0))

    # change master back to zone 1 -> period 3
    realm.set_master_zone(realm.get_zone(0))

    for zone, bucket_name in zone_bucket.iteritems():
        if zone == z3:
            continue
        for objname in [ 'p3', '_p3' ]:
            k = new_key(user, zone, bucket_name, objname)
            k.set_contents_from_string('zxczxc')

    # restart zone 3 gateway and wait for sync
    z3.cluster.start_rgw()
    realm.meta_checkpoint()

    # verify that we end up with the same objects
    for source_zone, bucket in zone_bucket.iteritems():
        for target_zone in all_zones:
            if source_zone.zone_name == target_zone.zone_name:
                continue

            realm.zone_bucket_checkpoint(target_zone, source_zone, bucket.name)

            check_bucket_eq(user, source_zone, target_zone, bucket)


def init(parse_args):
    cfg = ConfigParser.RawConfigParser({
                                         'num_zones': 3,
                                         'no_bootstrap': 'false',
                                         'log_level': 20,
                                         })
    try:
        path = os.environ['RGW_MULTI_TEST_CONF']
    except KeyError:
        path = tpath('test_multi.conf')

    try:
        with file(path) as f:
            cfg.readfp(f)
    except:
        print 'WARNING: error reading test config. Path can be set through the RGW_MULTI_TEST_CONF env variable'
        pass

    parser = argparse.ArgumentParser(
            description='Run rgw multi-site tests',
            usage='test_multi [--num-zones <num>] [--no-bootstrap]')

    section = 'DEFAULT'
    parser.add_argument('--num-zones', type=int, default=cfg.getint(section, 'num_zones'))
    parser.add_argument('--no-bootstrap', action='store_true', default=cfg.getboolean(section, 'no_bootstrap'))
    parser.add_argument('--log-level', type=int, default=cfg.getint(section, 'log_level'))

    argv = []

    if parse_args:
        argv = sys.argv[1:]

    args = parser.parse_args(argv)

    global log_level
    log_level = args.log_level

    global rgw_multi
    rgw_multi = RGWMulti(int(args.num_zones))
    rgw_multi.setup(not args.no_bootstrap)

    global realm
    realm = rgw_multi.realm

    global user
    user = rgw_multi.user

def setup_module():
    init(False)

if __name__ == "__main__":
    init(True)
