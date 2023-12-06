import json
import requests.compat
import logging

import boto
import boto.s3.connection

import dateutil.parser

from nose.tools import eq_ as eq
from itertools import zip_longest  # type: ignore

from .multisite import *
from .tools import *

log = logging.getLogger(__name__)

def get_key_ver(k):
    if not k.version_id:
        return 'null'
    return k.version_id

def check_object_eq(k1, k2, check_extra = True):
    assert k1
    assert k2
    log.debug('comparing key name=%s', k1.name)
    eq(k1.name, k2.name)
    eq(k1.metadata, k2.metadata)
    # eq(k1.cache_control, k2.cache_control)
    eq(k1.content_type, k2.content_type)
    # eq(k1.content_encoding, k2.content_encoding)
    # eq(k1.content_disposition, k2.content_disposition)
    # eq(k1.content_language, k2.content_language)
    eq(k1.etag, k2.etag)
    mtime1 = dateutil.parser.parse(k1.last_modified)
    mtime2 = dateutil.parser.parse(k2.last_modified)
    assert abs((mtime1 - mtime2).total_seconds()) < 1 # handle different time resolution
    if check_extra:
        eq(k1.owner.id, k2.owner.id)
        eq(k1.owner.display_name, k2.owner.display_name)
    # eq(k1.storage_class, k2.storage_class)
    eq(k1.size, k2.size)
    eq(get_key_ver(k1), get_key_ver(k2))
    # eq(k1.encrypted, k2.encrypted)

def make_request(conn, method, bucket, key, query_args, headers):
    result = conn.make_request(method, bucket=bucket, key=key, query_args=query_args, headers=headers)
    if result.status // 100 != 2:
        raise boto.exception.S3ResponseError(result.status, result.reason, result.read())
    return result


class MDSearch:
    def __init__(self, conn, bucket_name, query, query_args = None, marker = None):
        self.conn = conn
        self.bucket_name = bucket_name or ''
        if bucket_name:
            self.bucket = boto.s3.bucket.Bucket(name=bucket_name)
        else:
            self.bucket = None
        self.query = query
        self.query_args = query_args
        self.max_keys = None
        self.marker = marker

    def raw_search(self):
        q = self.query or ''
        query_args = append_query_arg(self.query_args, 'query', requests.compat.quote_plus(q))
        if self.max_keys is not None:
            query_args = append_query_arg(query_args, 'max-keys', self.max_keys)
        if self.marker:
            query_args = append_query_arg(query_args, 'marker', self.marker)

        query_args = append_query_arg(query_args, 'format', 'json')

        headers = {}

        result = make_request(self.conn, "GET", bucket=self.bucket_name, key='', query_args=query_args, headers=headers)

        l = []

        result_dict = json.loads(result.read())

        for entry in result_dict['Objects']:
            bucket = self.conn.get_bucket(entry['Bucket'], validate = False)
            k = boto.s3.key.Key(bucket, entry['Key'])

            k.version_id = entry['Instance']
            k.etag = entry['ETag']
            k.owner = boto.s3.user.User(id=entry['Owner']['ID'], display_name=entry['Owner']['DisplayName'])
            k.last_modified = entry['LastModified']
            k.size = entry['Size']
            k.content_type = entry['ContentType']
            k.versioned_epoch = entry['VersionedEpoch']

            k.metadata = {}
            for e in entry['CustomMetadata']:
                k.metadata[e['Name']] = str(e['Value']) # int values will return as int, cast to string for compatibility with object meta response

            l.append(k)

        return result_dict, l

    def search(self, drain = True, sort = True, sort_key = None):
        l = []

        is_done = False

        while not is_done:
            result, result_keys = self.raw_search()

            l = l + result_keys

            is_done = not (drain and (result['IsTruncated'] == "true"))
            marker = result['Marker']

        if sort:
            if not sort_key:
                sort_key = lambda k: (k.name, -k.versioned_epoch)
            l.sort(key = sort_key)

        return l


class MDSearchConfig:
    def __init__(self, conn, bucket_name):
        self.conn = conn
        self.bucket_name = bucket_name or ''
        if bucket_name:
            self.bucket = boto.s3.bucket.Bucket(name=bucket_name)
        else:
            self.bucket = None

    def send_request(self, conf, method):
        query_args = 'mdsearch'
        headers = None
        if conf:
            headers = { 'X-Amz-Meta-Search': conf }

        query_args = append_query_arg(query_args, 'format', 'json')

        return make_request(self.conn, method, bucket=self.bucket_name, key='', query_args=query_args, headers=headers)

    def get_config(self):
        result = self.send_request(None, 'GET')
        return json.loads(result.read())

    def set_config(self, conf):
        self.send_request(conf, 'POST')

    def del_config(self):
        self.send_request(None, 'DELETE')


class ESZoneBucket:
    def __init__(self, zone_conn, name, conn):
        self.zone_conn = zone_conn
        self.name = name
        self.conn = conn

        self.bucket = boto.s3.bucket.Bucket(name=name)

    def get_all_versions(self):

        marker = None
        is_done = False

        req = MDSearch(self.conn, self.name, 'bucket == ' + self.name, marker=marker)

        for k in req.search():
            yield k




class ESZone(Zone):
    def __init__(self, name, es_endpoint, zonegroup = None, cluster = None, data = None, zone_id = None, gateways = None):
        self.es_endpoint = es_endpoint
        super(ESZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def is_read_only(self):
        return True

    def tier_type(self):
        return "elasticsearch"

    def create(self, cluster, args = None, check_retcode = True):
        """ create the object with the given arguments """

        if args is None:
            args = ''

        tier_config = ','.join([ 'endpoint=' + self.es_endpoint, 'explicit_custom_meta=false' ])

        args += [ '--tier-type', self.tier_type(), '--tier-config', tier_config ] 

        return self.json_command(cluster, 'create', args, check_retcode=check_retcode)

    def has_buckets(self):
        return False

    def has_roles(self):
        return False

    class Conn(ZoneConn):
        def __init__(self, zone, credentials):
            super(ESZone.Conn, self).__init__(zone, credentials)

        def get_bucket(self, bucket_name):
            return ESZoneBucket(self, bucket_name, self.conn)

        def create_bucket(self, name):
            # should not be here, a bug in the test suite
            log.critical('Conn.create_bucket() should not be called in ES zone')
            assert False

        def check_bucket_eq(self, zone_conn, bucket_name):
            assert(zone_conn.zone.tier_type() == "rados")

            log.info('comparing bucket=%s zones={%s, %s}', bucket_name, self.name, self.name)
            b1 = self.get_bucket(bucket_name)
            b2 = zone_conn.get_bucket(bucket_name)

            log.debug('bucket1 objects:')
            for o in b1.get_all_versions():
                log.debug('o=%s', o.name)
            log.debug('bucket2 objects:')
            for o in b2.get_all_versions():
                log.debug('o=%s', o.name)

            for k1, k2 in zip_longest(b1.get_all_versions(), b2.get_all_versions()):
                if k1 is None:
                    log.critical('key=%s is missing from zone=%s', k2.name, self.name)
                    assert False
                if k2 is None:
                    log.critical('key=%s is missing from zone=%s', k1.name, zone_conn.name)
                    assert False

                check_object_eq(k1, k2)


            log.info('success, bucket identical: bucket=%s zones={%s, %s}', bucket_name, self.name, zone_conn.name)

            return True

        def create_role(self, path, rolename, policy_document, tag_list):
            assert False

        def delete_role(self, role_name):
            assert False

        def has_role(self, role_name):
            assert False

    def get_conn(self, credentials):
        return self.Conn(self, credentials)


class ESZoneConfig:
    def __init__(self, cfg, section):
        self.endpoint = cfg.get(section, 'endpoint')

