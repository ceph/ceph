import logging

try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest

from nose.tools import eq_ as eq

from rgw_multi.multisite import *

log = logging.getLogger(__name__)

def check_object_eq(k1, k2, check_extra = True):
    assert k1
    assert k2
    log.debug('comparing key name=%s', k1.name)
    eq(k1.name, k2.name)
    eq(k1.get_contents_as_string(), k2.get_contents_as_string())
    eq(k1.metadata, k2.metadata)
    eq(k1.cache_control, k2.cache_control)
    eq(k1.content_type, k2.content_type)
    eq(k1.content_encoding, k2.content_encoding)
    eq(k1.content_disposition, k2.content_disposition)
    eq(k1.content_language, k2.content_language)
    eq(k1.etag, k2.etag)
    eq(k1.last_modified, k2.last_modified)
    if check_extra:
        eq(k1.owner.id, k2.owner.id)
        eq(k1.owner.display_name, k2.owner.display_name)
    eq(k1.storage_class, k2.storage_class)
    eq(k1.size, k2.size)
    eq(k1.version_id, k2.version_id)
    eq(k1.encrypted, k2.encrypted)


class RadosZone(Zone):
    def __init__(self, name, zonegroup = None, cluster = None, data = None, zone_id = None, gateways = []):
        super(RadosZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def  tier_type(self):
        return "rados"

    def get_bucket(self, name, credentials):
        conn = self.get_connection(credentials)
        return conn.get_bucket(name, credentials)

    def check_bucket_eq(self, zone, bucket_name, credentials):
        log.info('comparing bucket=%s zones={%s, %s}', bucket_name, self.name, zone.name)
        b1 = self.get_bucket(bucket_name, credentials)
        b2 = zone.get_bucket(bucket_name, credentials)

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
                log.critical('key=%s is missing from zone=%s', k1.name, zone.name)
                assert False

            check_object_eq(k1, k2)

            # now get the keys through a HEAD operation, verify that the available data is the same
            k1_head = b1.get_key(k1.name)
            k2_head = b2.get_key(k2.name)

            check_object_eq(k1_head, k2_head, False)

        log.info('success, bucket identical: bucket=%s zones={%s, %s}', bucket_name, self.name, zone.name)


