import json
import requests.compat
import logging

import boto
import boto.s3.connection

import dateutil.parser
import datetime

import re

from nose.tools import eq_ as eq
try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest

from six.moves.urllib.parse import urlparse

from .multisite import *
from .tools import *

log = logging.getLogger(__name__)

def get_key_ver(k):
    if not k.version_id:
        return 'null'
    return k.version_id

def unquote(s):
    if s[0] == '"' and s[-1] == '"':
        return s[1:-1]
    return s

def check_object_eq(k1, k2, check_extra = True):
    assert k1
    assert k2
    log.debug('comparing key name=%s', k1.name)
    eq(k1.name, k2.name)
    eq(k1.metadata, k2.metadata)
    # eq(k1.cache_control, k2.cache_control)
    eq(k1.content_type, k2.content_type)
    eq(k1.content_encoding, k2.content_encoding)
    eq(k1.content_disposition, k2.content_disposition)
    eq(k1.content_language, k2.content_language)

    eq(unquote(k1.etag), unquote(k2.etag))

    mtime1 = dateutil.parser.parse(k1.last_modified)
    mtime2 = dateutil.parser.parse(k2.last_modified)
    log.debug('k1.last_modified=%s k2.last_modified=%s', k1.last_modified, k2.last_modified)
    assert abs((mtime1 - mtime2).total_seconds()) < 1 # handle different time resolution
    # if check_extra:
        # eq(k1.owner.id, k2.owner.id)
        # eq(k1.owner.display_name, k2.owner.display_name)
    # eq(k1.storage_class, k2.storage_class)
    eq(k1.size, k2.size)
    eq(get_key_ver(k1), get_key_ver(k2))
    # eq(k1.encrypted, k2.encrypted)

def make_request(conn, method, bucket, key, query_args, headers):
    result = conn.make_request(method, bucket=bucket, key=key, query_args=query_args, headers=headers)
    if result.status / 100 != 2:
        raise boto.exception.S3ResponseError(result.status, result.reason, result.read())
    return result

class CloudKey:
    def __init__(self, zone_bucket, k):
        self.zone_bucket = zone_bucket

        # we need two keys: when listing buckets, we get keys that only contain partial data
        # but we need to have the full data so that we could use all the meta-rgwx- headers
        # that are needed in order to create a correct representation of the object
        self.key = k
        self.rgwx_key = k # assuming k has all the meta info on, if not then we'll update it in update()
        self.update()

    def update(self):
        k = self.key
        rk = self.rgwx_key

        self.size = rk.size
        orig_name = rk.metadata.get('rgwx-source-key')
        if not orig_name:
            self.rgwx_key = self.zone_bucket.bucket.get_key(k.name, version_id = k.version_id)
            rk = self.rgwx_key
            orig_name = rk.metadata.get('rgwx-source-key')

        self.name = orig_name
        self.version_id = rk.metadata.get('rgwx-source-version-id')

        ve = rk.metadata.get('rgwx-versioned-epoch')
        if ve:
            self.versioned_epoch = int(ve)
        else:
            self.versioned_epoch = 0

        mt = rk.metadata.get('rgwx-source-mtime')
        if mt:
            self.last_modified = datetime.datetime.utcfromtimestamp(float(mt)).strftime('%a, %d %b %Y %H:%M:%S GMT')
        else:
            self.last_modified = k.last_modified

        et = rk.metadata.get('rgwx-source-etag')
        if rk.etag.find('-') >= 0 or et.find('-') >= 0:
            # in this case we will use the source etag as it was uploaded via multipart upload
            # in one of the zones, so there's no way to make sure etags are calculated the same
            # way. In the other case we'd just want to keep the etag that was generated in the
            # regular upload mechanism, which should be consistent in both ends
            self.etag = et
        else:
            self.etag = rk.etag

        if k.etag[0] == '"' and self.etag[0] != '"': # inconsistent etag quoting when listing bucket vs object get
            self.etag = '"' + self.etag + '"'

        new_meta = {}
        for meta_key, meta_val in k.metadata.iteritems():
            if not meta_key.startswith('rgwx-'):
                new_meta[meta_key] = meta_val

        self.metadata = new_meta

        self.cache_control = k.cache_control
        self.content_type = k.content_type
        self.content_encoding = k.content_encoding
        self.content_disposition = k.content_disposition
        self.content_language = k.content_language


    def get_contents_as_string(self):
        r = self.key.get_contents_as_string()

        # the previous call changed the status of the source object, as it loaded
        # its metadata

        self.rgwx_key = self.key
        self.update()

        return r

def append_query_arg(s, n, v):
    if not v:
        return s
    nv = '{n}={v}'.format(n=n, v=v)
    if not s:
        return nv
    return '{s}&{nv}'.format(s=s, nv=nv)


class CloudZoneBucket:
    def __init__(self, zone_conn, target_path, name):
        self.zone_conn = zone_conn
        self.name = name
        self.cloud_conn = zone_conn.zone.cloud_conn

        target_path = target_path[:]
        if target_path[-1] != '/':
            target_path += '/'
        target_path = target_path.replace('${bucket}', name)

        tp = target_path.split('/', 1)

        if len(tp) == 1:
            self.target_bucket = target_path
            self.target_prefix = ''
        else:
            self.target_bucket = tp[0]
            self.target_prefix = tp[1]

        log.debug('target_path=%s target_bucket=%s target_prefix=%s', target_path, self.target_bucket, self.target_prefix)
        self.bucket = self.cloud_conn.get_bucket(self.target_bucket)

    def get_all_versions(self):
        l = []

        for k in self.bucket.get_all_keys(prefix=self.target_prefix):
            new_key = CloudKey(self, k)

            log.debug('appending o=[\'%s\', \'%s\', \'%d\']', new_key.name, new_key.version_id, new_key.versioned_epoch)
            l.append(new_key)


        sort_key = lambda k: (k.name, -k.versioned_epoch)
        l.sort(key = sort_key)

        for new_key in l:
            yield new_key

    def get_key(self, name, version_id=None):
        return CloudKey(self, self.bucket.get_key(name, version_id=version_id))


def parse_endpoint(endpoint):
    o = urlparse(endpoint)

    netloc = o.netloc.split(':')
    
    host = netloc[0]

    if len(netloc) > 1:
        port = int(netloc[1])
    else:
        port = o.port

    is_secure = False

    if o.scheme == 'https':
        is_secure = True

    if not port:
        if is_secure:
            port = 443
        else:
            port = 80

    return host, port, is_secure


class CloudZone(Zone):
    def __init__(self, name, cloud_endpoint, credentials, source_bucket, target_path,
            zonegroup = None, cluster = None, data = None, zone_id = None, gateways = None):
        self.cloud_endpoint = cloud_endpoint
        self.credentials = credentials
        self.source_bucket = source_bucket
        self.target_path = target_path

        self.target_path = self.target_path.replace('${zone}', name)
        # self.target_path = self.target_path.replace('${zone_id}', zone_id)
        self.target_path = self.target_path.replace('${zonegroup}', zonegroup.name)
        self.target_path = self.target_path.replace('${zonegroup_id}', zonegroup.id)

        log.debug('target_path=%s', self.target_path)

        host, port, is_secure = parse_endpoint(cloud_endpoint)

        self.cloud_conn = boto.connect_s3(
                aws_access_key_id = credentials.access_key,
                aws_secret_access_key = credentials.secret,
                host = host,
                port = port,
                is_secure = is_secure,
                calling_format = boto.s3.connection.OrdinaryCallingFormat())
        super(CloudZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)


    def is_read_only(self):
        return True

    def tier_type(self):
        return "cloud"

    def create(self, cluster, args = None, check_retcode = True):
        """ create the object with the given arguments """

        if args is None:
            args = ''

        tier_config = ','.join([ 'connection.endpoint=' + self.cloud_endpoint,
                                 'connection.access_key=' + self.credentials.access_key,
                                 'connection.secret=' + self.credentials.secret,
                                 'target_path=' + re.escape(self.target_path)])

        args += [ '--tier-type', self.tier_type(), '--tier-config', tier_config ] 

        return self.json_command(cluster, 'create', args, check_retcode=check_retcode)

    def has_buckets(self):
        return False

    class Conn(ZoneConn):
        def __init__(self, zone, credentials):
            super(CloudZone.Conn, self).__init__(zone, credentials)

        def get_bucket(self, bucket_name):
            return CloudZoneBucket(self, self.zone.target_path, bucket_name)

        def create_bucket(self, name):
            # should not be here, a bug in the test suite
            log.critical('Conn.create_bucket() should not be called in cloud zone')
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

    def get_conn(self, credentials):
        return self.Conn(self, credentials)


class CloudZoneConfig:
    def __init__(self, cfg, section):
        self.endpoint = cfg.get(section, 'endpoint')
        access_key = cfg.get(section, 'access_key')
        secret = cfg.get(section, 'secret')
        self.credentials = Credentials(access_key, secret)
        try:
            self.target_path = cfg.get(section, 'target_path')
        except:
            self.target_path = 'rgw-${zonegroup_id}/${bucket}'

        try:
            self.source_bucket = cfg.get(section, 'source_bucket')
        except:
            self.source_bucket = '*'

