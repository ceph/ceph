import os
import json
import argparse
import sys
import ConfigParser

from rgw_multi import *

from rgw_multi import *

log_level = 20

def test_bucket_create():
    global realm
    global user
    buckets, _ = create_bucket_per_zone(user, realm)
    realm.meta_checkpoint()

    for zone in realm.get_zones():
        assert check_all_buckets_exist(user, zone, buckets)

def test_bucket_recreate():
    global realm
    global user
    buckets, _ = create_bucket_per_zone(user, realm)
    realm.meta_checkpoint()

    for zone in realm.get_zones():
        assert check_all_buckets_exist(user, zone, buckets)

    # recreate buckets on all zones, make sure they weren't removed
    for zone in realm.get_zones():
        for bucket_name in buckets:
            conn = zone.get_connection(user)
            bucket = conn.create_bucket(bucket_name)

    for zone in realm.get_zones():
        assert check_all_buckets_exist(user, zone, buckets)

    realm.meta_checkpoint()

    for zone in realm.get_zones():
        assert check_all_buckets_exist(user, zone, buckets)

def test_bucket_remove():
    global realm
    global user
    buckets, zone_bucket = create_bucket_per_zone(user, realm)
    realm.meta_checkpoint()

    for zone in realm.get_zones():
        assert check_all_buckets_exist(user, zone, buckets)

    for zone, bucket_name in zone_bucket.iteritems():
        conn = zone.get_connection(user)
        conn.delete_bucket(bucket_name)

    realm.meta_checkpoint()

    for zone in realm.get_zones():
        assert check_all_buckets_dont_exist(user, zone, buckets)

def test_object_sync():
    global realm
    global user
    buckets, zone_bucket = create_bucket_per_zone(user, realm)

    all_zones = []
    for z in zone_bucket:
        all_zones.append(z)

    objnames = [ 'myobj', '_myobj', ':', '&' ]
    content = 'asdasd'

    # don't wait for meta sync just yet
    for zone, bucket_name in zone_bucket.iteritems():
        for objname in objnames:
            k = new_key(user, zone, bucket_name, objname)
            k.set_contents_from_string(content)

    realm.meta_checkpoint()

    for source_zone, bucket in zone_bucket.iteritems():
        for target_zone in all_zones:
            if source_zone.zone_name == target_zone.zone_name:
                continue

            realm.zone_bucket_checkpoint(target_zone, source_zone, bucket.name)

            check_bucket_eq(user, source_zone, target_zone, bucket)

def test_object_delete():
    global realm
    global user
    buckets, zone_bucket = create_bucket_per_zone(user, realm)

    all_zones = []
    for z in zone_bucket:
        all_zones.append(z)

    objname = 'myobj'
    content = 'asdasd'

    # don't wait for meta sync just yet
    for zone, bucket in zone_bucket.iteritems():
        k = new_key(user, zone, bucket, objname)
        k.set_contents_from_string(content)

    realm.meta_checkpoint()

    # check object exists
    for source_zone, bucket in zone_bucket.iteritems():
        for target_zone in all_zones:
            if source_zone.zone_name == target_zone.zone_name:
                continue

            realm.zone_bucket_checkpoint(target_zone, source_zone, bucket.name)

            check_bucket_eq(user, source_zone, target_zone, bucket)

    # check object removal
    for source_zone, bucket in zone_bucket.iteritems():
        k = get_key(user, source_zone, bucket, objname)
        k.delete()
        for target_zone in all_zones:
            if source_zone.zone_name == target_zone.zone_name:
                continue

            realm.zone_bucket_checkpoint(target_zone, source_zone, bucket.name)

            check_bucket_eq(user, source_zone, target_zone, bucket)


def init(parse_args):
    cfg = ConfigParser.RawConfigParser({
                                         'num_zones': 2,
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
