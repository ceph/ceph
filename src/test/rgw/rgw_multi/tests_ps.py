import logging

import boto
import boto.s3.connection

from rgw_multi.multisite import *
from rgw_multi.tests import *
from rgw_multi.zone_pubsub import *

log = logging.getLogger(__name__)

# check if at least one pubsub zone exist
def check_ps_configured():
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    es_zones = zonegroup.zones_by_type.get("pubsub")
    if not es_zones:
        raise SkipTest("Requires at least one PS zone")

# check if a specific zone is pubsub zone
def is_ps_zone(zone_conn):
    if not zone_conn:
        return False
    return zone_conn.zone.tier_type() == "pubsub"

# initialize the environment
def init_env():
    check_ps_configured()

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zonegroup_meta_checkpoint(zonegroup)

    for conn in zonegroup_conns.zones:
        if is_ps_zone(conn):
            zone_meta_checkpoint(conn.zone)

# test basic sync 
def test_ps_sync():
    init_env()

