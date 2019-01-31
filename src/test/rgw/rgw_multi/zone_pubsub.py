import json
import requests.compat
import logging

import boto
import boto.s3.connection

import dateutil.parser

from .multisite import *
from .tools import *

log = logging.getLogger(__name__)

class PSZone(Zone):
    def __init__(self, name, zonegroup = None, cluster = None, data = None, zone_id = None, gateways = None):
        super(PSZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def is_read_only(self):
        return True

    def tier_type(self):
        return "pubsub"

    def create(self, cluster, args = None, check_retcode = True):
        """ create the object with the given arguments """

        if args is None:
            args = ''

        args += [ '--tier-type', self.tier_type() ] 

        return self.json_command(cluster, 'create', args, check_retcode=check_retcode)

    def has_buckets(self):
        return False

    class Conn(ZoneConn):
        def __init__(self, zone, credentials):
            super(PSZone.Conn, self).__init__(zone, credentials)

    def get_conn(self, credentials):
        return self.Conn(self, credentials)

