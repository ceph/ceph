import logging

from .multisite import Zone


log = logging.getLogger('rgw_multi.tests')


class AZone(Zone):  # pylint: disable=too-many-ancestors
    """ archive zone class """
    def __init__(self, name, zonegroup=None, cluster=None, data=None, zone_id=None, gateways=None):
        super(AZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def is_read_only(self):
        return False

    def tier_type(self):
        return "archive"

    def create(self, cluster, args=None, **kwargs):
        if args is None:
            args = ''
        args += ['--tier-type', self.tier_type()]
        return self.json_command(cluster, 'create', args)

    def has_buckets(self):
        return False


class AZoneConfig:
    """ archive zone configuration """
    def __init__(self, cfg, section):
        pass


def print_connection_info(conn):
    """print info of connection"""
    print("Host: " + conn.host+':'+str(conn.port))
    print("AWS Secret Key: " + conn.aws_secret_access_key)
    print("AWS Access Key: " + conn.aws_access_key_id)
