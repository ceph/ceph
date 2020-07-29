import json
import logging

import boto
import boto.s3.connection

import datetime
import dateutil

from itertools import zip_longest  # type: ignore

from nose.tools import eq_ as eq

from .multisite import *
from .tests import *
from .zone_es import *

log = logging.getLogger(__name__)


def check_es_configured():
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    es_zones = zonegroup.zones_by_type.get("elasticsearch")
    if not es_zones:
        raise SkipTest("Requires at least one ES zone")

def is_es_zone(zone_conn):
    if not zone_conn:
        return False

    return zone_conn.zone.tier_type() == "elasticsearch"

def verify_search(bucket_name, src_keys, result_keys, f):
    check_keys = []
    for k in src_keys:
        if bucket_name:
            if bucket_name != k.bucket.name:
                continue
        if f(k):
            check_keys.append(k)
    check_keys.sort(key = lambda l: (l.bucket.name, l.name, l.version_id))

    log.debug('check keys:' + dump_json(check_keys))
    log.debug('result keys:' + dump_json(result_keys))

    for k1, k2 in zip_longest(check_keys, result_keys):
        assert k1
        assert k2
        check_object_eq(k1, k2)

def do_check_mdsearch(conn, bucket, src_keys, req_str, src_filter):
    if bucket:
        bucket_name = bucket.name
    else:
        bucket_name = ''
    req = MDSearch(conn, bucket_name, req_str)
    result_keys = req.search(sort_key = lambda k: (k.bucket.name, k.name, k.version_id))
    verify_search(bucket_name, src_keys, result_keys, src_filter)

def init_env(create_obj, num_keys = 5, buckets_per_zone = 1, bucket_init_cb = None):
    check_es_configured()

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns, buckets_per_zone = buckets_per_zone)

    if bucket_init_cb:
        for zone_conn, bucket in zone_bucket:
            bucket_init_cb(zone_conn, bucket)

    src_keys = []

    owner = None

    obj_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

    # don't wait for meta sync just yet
    for zone, bucket in zone_bucket:
        for count in range(num_keys):
            objname = obj_prefix + str(count)
            k = new_key(zone, bucket.name, objname)
            # k.set_contents_from_string(content + 'x' * count)
            if not create_obj:
                continue

            create_obj(k, count)

            if not owner:
                for list_key in bucket.list_versions():
                    owner = list_key.owner
                    break

            k = bucket.get_key(k.name, version_id = k.version_id)
            k.owner = owner # owner is not set when doing get_key()

            src_keys.append(k)

    zonegroup_meta_checkpoint(zonegroup)

    sources = []
    targets = []
    for target_conn in zonegroup_conns.zones:
        if not is_es_zone(target_conn):
            sources.append(target_conn)
            continue

        targets.append(target_conn)

    buckets = []
    # make sure all targets are synced
    for source_conn, bucket in zone_bucket:
        buckets.append(bucket)
        for target_conn in targets:
            zone_bucket_checkpoint(target_conn.zone, source_conn.zone, bucket.name)

    return targets, sources, buckets, src_keys

def test_es_object_search():
    min_size = 10
    content = 'a' * min_size

    def create_obj(k, i):
        k.set_contents_from_string(content + 'x' * i)

    targets, _, buckets, src_keys = init_env(create_obj, num_keys = 5, buckets_per_zone = 2)

    for target_conn in targets:

        # bucket checks
        for bucket in buckets:
            # check name
            do_check_mdsearch(target_conn.conn, None, src_keys , 'bucket == ' + bucket.name, lambda k: k.bucket.name == bucket.name)
            do_check_mdsearch(target_conn.conn, bucket, src_keys , 'bucket == ' + bucket.name, lambda k: k.bucket.name == bucket.name)

        # check on all buckets
        for key in src_keys:
            # limiting to checking specific key name, otherwise could get results from
            # other runs / tests
            do_check_mdsearch(target_conn.conn, None, src_keys , 'name == ' + key.name, lambda k: k.name == key.name)

        # check on specific bucket
        for bucket in buckets:
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name < ' + key.name, lambda k: k.name < key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name <= ' + key.name, lambda k: k.name <= key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name == ' + key.name, lambda k: k.name == key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name >= ' + key.name, lambda k: k.name >= key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name > ' + key.name, lambda k: k.name > key.name)

            do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name == ' + src_keys[0].name + ' or name >= ' + src_keys[2].name,
                              lambda k: k.name == src_keys[0].name or k.name >= src_keys[2].name)

            # check etag
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'etag < ' + key.etag[1:-1], lambda k: k.etag < key.etag)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'etag == ' + key.etag[1:-1], lambda k: k.etag == key.etag)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'etag > ' + key.etag[1:-1], lambda k: k.etag > key.etag)

            # check size
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size < ' + str(key.size), lambda k: k.size < key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size <= ' + str(key.size), lambda k: k.size <= key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size == ' + str(key.size), lambda k: k.size == key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size >= ' + str(key.size), lambda k: k.size >= key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size > ' + str(key.size), lambda k: k.size > key.size)

def date_from_str(s):
    return dateutil.parser.parse(s)

def test_es_object_search_custom():
    min_size = 10
    content = 'a' * min_size

    def bucket_init(zone_conn, bucket):
        req = MDSearchConfig(zone_conn.conn, bucket.name)
        req.set_config('x-amz-meta-foo-str; string, x-amz-meta-foo-int; int, x-amz-meta-foo-date; date')

    def create_obj(k, i):
        date = datetime.datetime.now() + datetime.timedelta(seconds=1) * i
        date_str = date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        k.set_contents_from_string(content + 'x' * i, headers = { 'X-Amz-Meta-Foo-Str': str(i * 5),
                                                                  'X-Amz-Meta-Foo-Int': str(i * 5),
                                                                  'X-Amz-Meta-Foo-Date': date_str})

    targets, _, buckets, src_keys = init_env(create_obj, num_keys = 5, buckets_per_zone = 1, bucket_init_cb = bucket_init)


    for target_conn in targets:

        # bucket checks
        for bucket in buckets:
            str_vals = []
            for key in src_keys:
                # check string values
                val = key.get_metadata('foo-str')
                str_vals.append(val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-str < ' + val, lambda k: k.get_metadata('foo-str') < val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-str <= ' + val, lambda k: k.get_metadata('foo-str') <= val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-str == ' + val, lambda k: k.get_metadata('foo-str') == val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-str >= ' + val, lambda k: k.get_metadata('foo-str') >= val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-str > ' + val, lambda k: k.get_metadata('foo-str') > val)

                # check int values
                sval = key.get_metadata('foo-int')
                val = int(sval)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-int < ' + sval, lambda k: int(k.get_metadata('foo-int')) < val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-int <= ' + sval, lambda k: int(k.get_metadata('foo-int')) <= val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-int == ' + sval, lambda k: int(k.get_metadata('foo-int')) == val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-int >= ' + sval, lambda k: int(k.get_metadata('foo-int')) >= val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-int > ' + sval, lambda k: int(k.get_metadata('foo-int')) > val)

                # check int values
                sval = key.get_metadata('foo-date')
                val = date_from_str(sval)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-date < ' + sval, lambda k: date_from_str(k.get_metadata('foo-date')) < val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-date <= ' + sval, lambda k: date_from_str(k.get_metadata('foo-date')) <= val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-date == ' + sval, lambda k: date_from_str(k.get_metadata('foo-date')) == val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-date >= ' + sval, lambda k: date_from_str(k.get_metadata('foo-date')) >= val)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-date > ' + sval, lambda k: date_from_str(k.get_metadata('foo-date')) > val)

            # 'or' query
            for i in range(len(src_keys) // 2):
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-str <= ' + str_vals[i] + ' or x-amz-meta-foo-str >= ' + str_vals[-i],
                        lambda k: k.get_metadata('foo-str') <= str_vals[i] or k.get_metadata('foo-str') >= str_vals[-i] )

            # 'and' query
            for i in range(len(src_keys) // 2):
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'x-amz-meta-foo-str >= ' + str_vals[i] + ' and x-amz-meta-foo-str <= ' + str_vals[i + 1],
                        lambda k: k.get_metadata('foo-str') >= str_vals[i] and k.get_metadata('foo-str') <= str_vals[i + 1] )
            # more complicated query
            for i in range(len(src_keys) // 2):
                do_check_mdsearch(target_conn.conn, None, src_keys , 'bucket == ' + bucket.name + ' and x-amz-meta-foo-str >= ' + str_vals[i] +
                                                                     ' and (x-amz-meta-foo-str <= ' + str_vals[i + 1] + ')',
                        lambda k: k.bucket.name == bucket.name and (k.get_metadata('foo-str') >= str_vals[i] and
                                                                    k.get_metadata('foo-str') <= str_vals[i + 1]) )

def test_es_bucket_conf():
    min_size = 0

    def bucket_init(zone_conn, bucket):
        req = MDSearchConfig(zone_conn.conn, bucket.name)
        req.set_config('x-amz-meta-foo-str; string, x-amz-meta-foo-int; int, x-amz-meta-foo-date; date')

    targets, sources, buckets, _ = init_env(None, num_keys = 5, buckets_per_zone = 1, bucket_init_cb = bucket_init)

    for source_conn in sources:
        for bucket in buckets:
            req = MDSearchConfig(source_conn.conn, bucket.name)
            conf = req.get_config()

            d = {}

            for entry in conf:
              d[entry['Key']] = entry['Type']

            eq(len(d), 3)
            eq(d['x-amz-meta-foo-str'], 'str')
            eq(d['x-amz-meta-foo-int'], 'int')
            eq(d['x-amz-meta-foo-date'], 'date')

            req.del_config()

            conf = req.get_config()

            eq(len(conf), 0)

        break # no need to iterate over all zones
