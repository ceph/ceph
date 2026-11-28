import logging

from .zone_rados import RadosZone


log = logging.getLogger('rgw_multi.tests')


class BZone(RadosZone):  # pylint: disable=too-many-ancestors
    """ backup zone class

    A backup zone never drops data: deletes on the source are replicated as
    delete markers instead of removals, and every version the zone has ever
    received is retained.  It is an ordinary rados zone in every other
    respect, so RadosZone's boto3 Conn is inherited as-is (RadosZone.get_conn
    returns self.Conn, which resolves through the subclass).
    """
    def __init__(self, name, zonegroup=None, cluster=None, data=None, zone_id=None, gateways=None):
        super(BZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def is_read_only(self):
        return False

    def tier_type(self):
        return "backup"

    def create(self, cluster, args=None, **kwargs):
        if args is None:
            args = []
        args += ['--tier-type', self.tier_type()]
        return self.json_command(cluster, 'create', args)

    def has_buckets(self):
        # RGWBackupBucketMetadataHandler::remove() renames buckets rather than
        # deleting them, so the generic "same bucket names in every zone"
        # helpers do not apply here.
        return False

    def has_roles(self):
        return True


class BZoneConfig:
    """ backup zone configuration """
    def __init__(self, cfg, section):
        pass
