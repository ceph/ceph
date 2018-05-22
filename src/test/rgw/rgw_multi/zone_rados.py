import logging
from boto.s3.deletemarker import DeleteMarker

try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest

from nose.tools import eq_ as eq

from .multisite import *

log = logging.getLogger(__name__)

def check_object_eq(k1, k2, check_extra = True):
    assert k1
    assert k2
    log.debug('comparing key name=%s', k1.name)
    eq(k1.name, k2.name)
    eq(k1.version_id, k2.version_id)
    eq(k1.is_latest, k2.is_latest)
    eq(k1.last_modified, k2.last_modified)
    if isinstance(k1, DeleteMarker):
        assert isinstance(k2, DeleteMarker)
        return

    eq(k1.get_contents_as_string(), k2.get_contents_as_string())
    eq(k1.metadata, k2.metadata)
    eq(k1.cache_control, k2.cache_control)
    eq(k1.content_type, k2.content_type)
    eq(k1.content_encoding, k2.content_encoding)
    eq(k1.content_disposition, k2.content_disposition)
    eq(k1.content_language, k2.content_language)
    eq(k1.etag, k2.etag)
    if check_extra:
        eq(k1.owner.id, k2.owner.id)
        eq(k1.owner.display_name, k2.owner.display_name)
    eq(k1.storage_class, k2.storage_class)
    eq(k1.size, k2.size)
    eq(k1.encrypted, k2.encrypted)

class RadosZone(Zone):
    def __init__(self, name, zonegroup = None, cluster = None, data = None, zone_id = None, gateways = None):
        super(RadosZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def  tier_type(self):
        return "rados"


    class Conn(ZoneConn):
        def __init__(self, zone, credentials):
            super(RadosZone.Conn, self).__init__(zone, credentials)

        def get_bucket(self, name):
            return self.conn.get_bucket(name)

        def create_bucket(self, name):
            return self.conn.create_bucket(name)

        def check_bucket_eq(self, zone_conn, bucket_name):
            log.info('comparing bucket=%s zones={%s, %s}', bucket_name, self.name, zone_conn.name)
            b1 = self.get_bucket(bucket_name)
            b2 = zone_conn.get_bucket(bucket_name)

            b1_versions = b1.list_versions()
            log.debug('bucket1 objects:')
            for o in b1_versions:
                log.debug('o=%s', o.name)

            b2_versions = b2.list_versions()
            log.debug('bucket2 objects:')
            for o in b2_versions:
                log.debug('o=%s', o.name)

            for k1, k2 in zip_longest(b1_versions, b2_versions):
                if k1 is None:
                    log.critical('key=%s is missing from zone=%s', k2.name, self.name)
                    assert False
                if k2 is None:
                    log.critical('key=%s is missing from zone=%s', k1.name, zone_conn.name)
                    assert False

                check_object_eq(k1, k2)

                if isinstance(k1, DeleteMarker):
                    # verify that HEAD sees a delete marker
                    assert b1.get_key(k1.name) is None
                    assert b2.get_key(k2.name) is None
                else:
                    # now get the keys through a HEAD operation, verify that the available data is the same
                    k1_head = b1.get_key(k1.name, version_id=k1.version_id)
                    k2_head = b2.get_key(k2.name, version_id=k2.version_id)
                    check_object_eq(k1_head, k2_head, False)

                    if k1.version_id:
                        # compare the olh to make sure they agree about the current version
                        k1_olh = b1.get_key(k1.name)
                        k2_olh = b2.get_key(k2.name)
                        # if there's a delete marker, HEAD will return None
                        if k1_olh or k2_olh:
                            check_object_eq(k1_olh, k2_olh, False)

            log.info('success, bucket identical: bucket=%s zones={%s, %s}', bucket_name, self.name, zone_conn.name)

            return True

    def get_conn(self, credentials):
        return self.Conn(self, credentials)

