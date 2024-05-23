import logging

from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal

from boto.s3.deletemarker import DeleteMarker

from .tests import get_realm, \
    ZonegroupConns, \
    zonegroup_meta_checkpoint, \
    zone_meta_checkpoint, \
    zone_bucket_checkpoint, \
    zone_data_checkpoint, \
    zonegroup_bucket_checkpoint, \
    check_bucket_eq, \
    gen_bucket_name, \
    get_user, \
    get_tenant

from .zone_az import print_connection_info


# configure logging for the tests module
log = logging.getLogger(__name__)


##########################################
# utility functions for archive zone tests
##########################################

def check_az_configured():
    """check if at least one archive zone exist"""
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    az_zones = zonegroup.zones_by_type.get("archive")
    if az_zones is None or len(az_zones) != 1:
        raise SkipTest("Requires one archive zone")


def is_az_zone(zone_conn):
    """check if a specific zone is archive zone"""
    if not zone_conn:
        return False
    return zone_conn.zone.tier_type() == "archive"


def init_env():
    """initialize the environment"""
    check_az_configured()

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zonegroup_meta_checkpoint(zonegroup)

    az_zones = []
    zones = []
    for conn in zonegroup_conns.zones:
        if is_az_zone(conn):
            zone_meta_checkpoint(conn.zone)
            az_zones.append(conn)
        elif not conn.zone.is_read_only():
            zones.append(conn)

    assert_not_equal(len(zones), 0)
    assert_not_equal(len(az_zones), 0)
    return zones, az_zones


def zone_full_checkpoint(target_zone, source_zone):
    zone_meta_checkpoint(target_zone)
    zone_data_checkpoint(target_zone, source_zone)


def check_bucket_exists_on_zone(zone, bucket_name):
    try:
        zone.conn.get_bucket(bucket_name)
    except:
        return False
    return True


def check_key_exists(key):
    try:
        key.get_contents_as_string()
    except:
        return False
    return True


def get_versioning_status(bucket):
    res = bucket.get_versioning_status()
    key = 'Versioning'
    if not key in res:
        return None
    else:
        return res[key]


def get_versioned_objs(bucket):
    b = []
    for b_entry in bucket.list_versions():
        if isinstance(b_entry, DeleteMarker):
            continue
        d = {}
        d['version_id'] = b_entry.version_id
        d['size'] = b_entry.size
        d['etag'] = b_entry.etag
        d['is_latest'] = b_entry.is_latest
        b.append({b_entry.key:d})
    return b


def get_versioned_entries(bucket):
    dm = []
    ver = []
    for b_entry in bucket.list_versions():
        if isinstance(b_entry, DeleteMarker):
            d = {}
            d['version_id'] = b_entry.version_id
            d['is_latest'] = b_entry.is_latest
            dm.append({b_entry.name:d})
        else:
            d = {}
            d['version_id'] = b_entry.version_id
            d['size'] = b_entry.size
            d['etag'] = b_entry.etag
            d['is_latest'] = b_entry.is_latest
            ver.append({b_entry.key:d})
    return (dm, ver)


def get_number_buckets_by_zone(zone):
    return len(zone.conn.get_all_buckets())


def get_bucket_names_by_zone(zone):
    return [b.name for b in zone.conn.get_all_buckets()]


def get_full_bucket_name(partial_bucket_name, bucket_names_az):
    full_bucket_name = None
    for bucket_name in bucket_names_az:
        if bucket_name.startswith(partial_bucket_name):
            full_bucket_name = bucket_name
            break
    return full_bucket_name


####################
# archive zone tests
####################


def test_az_info():
    """ log information for manual testing """
    return SkipTest("only used in manual testing")
    zones, az_zones = init_env()
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # create objects in the bucket
    number_of_objects = 3
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    print('Zonegroup: ' + zonegroup.name)
    print('user: ' + get_user())
    print('tenant: ' + get_tenant())
    print('Master Zone')
    print_connection_info(zones[0].conn)
    print('Archive Zone')
    print_connection_info(az_zones[0].conn)
    print('Bucket: ' + bucket_name)


def test_az_create_empty_bucket():
     """ test empty bucket replication """
     zones, az_zones = init_env()
     bucket_name = gen_bucket_name()
     # create bucket on the non archive zone
     zones[0].create_bucket(bucket_name)
     # sync
     zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
     # bucket exist on the archive zone
     p = check_bucket_exists_on_zone(az_zones[0], bucket_name)
     assert_equal(p, True)


def test_az_check_empty_bucket_versioning():
     """ test bucket versioning with empty bucket """
     zones, az_zones = init_env()
     bucket_name = gen_bucket_name()
     # create bucket on the non archive zone
     bucket = zones[0].create_bucket(bucket_name)
     # sync
     zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
     # get bucket on archive zone
     bucket_az = az_zones[0].conn.get_bucket(bucket_name)
     # check for non bucket versioning
     p1 = get_versioning_status(bucket) is None
     assert_equal(p1, True)
     p2 = get_versioning_status(bucket_az) is None
     assert_equal(p2, True)


def test_az_object_replication():
    """ test object replication """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    key = bucket.new_key("foo")
    key.set_contents_from_string("bar")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check object on archive zone
    bucket_az = az_zones[0].conn.get_bucket(bucket_name)
    key_az = bucket_az.get_key("foo")
    p1 = key_az.get_contents_as_string(encoding='ascii') == "bar"
    assert_equal(p1, True)


def test_az_object_replication_versioning():
    """ test object replication versioning """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # create object on the non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    key = bucket.new_key("foo")
    key.set_contents_from_string("bar")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check object content on archive zone
    bucket_az = az_zones[0].conn.get_bucket(bucket_name)
    key_az = bucket_az.get_key("foo")
    p1 = key_az.get_contents_as_string(encoding='ascii') == "bar"
    assert_equal(p1, True)
    # grab object versioning and etag
    for b_version in bucket.list_versions():
        b_version_id = b_version.version_id
        b_version_etag = b_version.etag
    for b_az_version in bucket_az.list_versions():
        b_az_version_id = b_az_version.version_id
        b_az_version_etag = b_az_version.etag
    # check
    p2 = b_version_id == 'null'
    assert_equal(p2, True)
    p3 = b_az_version_id != 'null'
    assert_equal(p3, True)
    p4 = b_version_etag == b_az_version_etag
    assert_equal(p4, True)


def test_az_lazy_activation_of_versioned_bucket():
    """ test lazy activation of versioned bucket """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # create object on the non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # get bucket on archive zone
    bucket_az = az_zones[0].conn.get_bucket(bucket_name)
    # check for non bucket versioning
    p1 = get_versioning_status(bucket) is None
    assert_equal(p1, True)
    p2 = get_versioning_status(bucket_az) is None
    assert_equal(p2, True)
    # create object on non archive zone
    key = bucket.new_key("foo")
    key.set_contents_from_string("bar")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check lazy versioned buckets
    p3 = get_versioning_status(bucket) is None
    assert_equal(p3, True)
    p4 = get_versioning_status(bucket_az) == 'Enabled'
    assert_equal(p4, True)


def test_az_archive_zone_double_object_replication_versioning():
    """ test archive zone double object replication versioning """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # create object on the non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    key = bucket.new_key("foo")
    key.set_contents_from_string("bar")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # get bucket on archive zone
    bucket_az = az_zones[0].conn.get_bucket(bucket_name)
    # check for non bucket versioning
    p1 = get_versioning_status(bucket) is None
    assert_equal(p1, True)
    p2 = get_versioning_status(bucket_az) == 'Enabled'
    assert_equal(p2, True)
    # overwrite object on non archive zone
    key = bucket.new_key("foo")
    key.set_contents_from_string("ouch")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check lazy versioned buckets
    p3 = get_versioning_status(bucket) is None
    assert_equal(p3, True)
    p4 = get_versioning_status(bucket_az) == 'Enabled'
    assert_equal(p4, True)
    # get versioned objects
    objs = get_versioned_objs(bucket)
    objs_az = get_versioned_objs(bucket_az)
    # check version_id, size, and is_latest on non archive zone
    p5 = objs[0]['foo']['version_id'] == 'null'
    assert_equal(p5, True)
    p6 = objs[0]['foo']['size'] == 4
    assert_equal(p6, True)
    p7 = objs[0]['foo']['is_latest'] == True
    assert_equal(p7, True)
    # check version_id, size, is_latest on archive zone
    latest_obj_az_etag = None
    for obj_az  in objs_az:
        current_obj_az = obj_az['foo']
        if current_obj_az['is_latest'] == True:
            p8 = current_obj_az['size'] == 4
            assert_equal(p8, True)
            latest_obj_az_etag = current_obj_az['etag']
        else:
            p9 = current_obj_az['size'] == 3
            assert_equal(p9, True)
        assert_not_equal(current_obj_az['version_id'], 'null')
    # check last versions' etags
    p10 = objs[0]['foo']['etag'] == latest_obj_az_etag
    assert_equal(p10, True)


def test_az_deleted_object_replication():
    """ test zone deleted object replication """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # create object on the non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    key = bucket.new_key("foo")
    key.set_contents_from_string("bar")
    p1 = key.get_contents_as_string(encoding='ascii') == "bar"
    assert_equal(p1, True)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # update object on non archive zone
    key.set_contents_from_string("soup")
    p2 = key.get_contents_as_string(encoding='ascii') == "soup"
    assert_equal(p2, True)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # delete object on non archive zone
    key.delete()
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check object on non archive zone
    p3 = check_key_exists(key) == False
    assert_equal(p3, True)
    # check objects on archive zone
    bucket_az = az_zones[0].conn.get_bucket(bucket_name)
    key_az = bucket_az.get_key("foo")
    p4 = check_key_exists(key_az) == True
    assert_equal(p4, True)
    p5 = key_az.get_contents_as_string(encoding='ascii') == "soup"
    assert_equal(p5, True)
    b_ver_az = get_versioned_objs(bucket_az)
    p6 = len(b_ver_az) == 2
    assert_equal(p6, True)


def test_az_bucket_renaming_on_empty_bucket_deletion():
    """ test bucket renaming on empty bucket deletion """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # grab number of buckets on non archive zone
    num_buckets = get_number_buckets_by_zone(zones[0])
    # grab number of buckets on archive zone
    num_buckets_az = get_number_buckets_by_zone(az_zones[0])
    # create bucket on non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # delete bucket in non archive zone
    zones[0].delete_bucket(bucket_name)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check no new buckets on non archive zone
    p1 = get_number_buckets_by_zone(zones[0]) == num_buckets
    assert_equal(p1, True)
    # check non deletion on bucket on archive zone
    p2 = get_number_buckets_by_zone(az_zones[0]) == (num_buckets_az + 1)
    assert_equal(p2, True)
    # check bucket renaming
    bucket_names_az = get_bucket_names_by_zone(az_zones[0])
    new_bucket_name = bucket_name + '-deleted-'
    p3 = any(bucket_name.startswith(new_bucket_name) for bucket_name in bucket_names_az)
    assert_equal(p3, True)


def test_az_old_object_version_in_archive_zone():
    """ test old object version in archive zone """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # grab number of buckets on non archive zone
    num_buckets = get_number_buckets_by_zone(zones[0])
    # grab number of buckets on archive zone
    num_buckets_az = get_number_buckets_by_zone(az_zones[0])
    # create bucket on non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    # create object on non archive zone
    key = bucket.new_key("foo")
    key.set_contents_from_string("zero")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # save object version on archive zone
    bucket_az = az_zones[0].conn.get_bucket(bucket_name)
    b_ver_az = get_versioned_objs(bucket_az)
    obj_az_version_id = b_ver_az[0]['foo']['version_id']
    # update object on non archive zone
    key.set_contents_from_string("one")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # delete object on non archive zone
    key.delete()
    # delete bucket on non archive zone
    zones[0].delete_bucket(bucket_name)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check same buckets on non archive zone
    p1 = get_number_buckets_by_zone(zones[0]) == num_buckets
    assert_equal(p1, True)
    # check for new bucket on archive zone
    p2 = get_number_buckets_by_zone(az_zones[0]) == (num_buckets_az + 1)
    assert_equal(p2, True)
    # get new bucket name on archive zone
    bucket_names_az = get_bucket_names_by_zone(az_zones[0])
    new_bucket_name_az = get_full_bucket_name(bucket_name + '-deleted-', bucket_names_az)
    p3 = new_bucket_name_az is not None
    assert_equal(p3, True)
    # check number of objects on archive zone
    new_bucket_az = az_zones[0].conn.get_bucket(new_bucket_name_az)
    new_b_ver_az = get_versioned_objs(new_bucket_az)
    p4 = len(new_b_ver_az) == 2
    assert_equal(p4, True)
    # check versioned objects on archive zone
    new_key_az = new_bucket_az.get_key("foo", version_id=obj_az_version_id)
    p5 = new_key_az.get_contents_as_string(encoding='ascii') == "zero"
    assert_equal(p5, True)
    new_key_latest_az = new_bucket_az.get_key("foo")
    p6 = new_key_latest_az.get_contents_as_string(encoding='ascii') == "one"
    assert_equal(p6, True)


def test_az_force_bucket_renaming_if_same_bucket_name():
    """ test force bucket renaming if same bucket name """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # grab number of buckets on non archive zone
    num_buckets = get_number_buckets_by_zone(zones[0])
    # grab number of buckets on archive zone
    num_buckets_az = get_number_buckets_by_zone(az_zones[0])
    # create bucket on non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check same buckets on non archive zone
    p1 = get_number_buckets_by_zone(zones[0]) == (num_buckets + 1)
    assert_equal(p1, True)
    # check for new bucket on archive zone
    p2 = get_number_buckets_by_zone(az_zones[0]) == (num_buckets_az + 1)
    assert_equal(p2, True)
    # delete bucket on non archive zone
    zones[0].delete_bucket(bucket_name)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check number of buckets on non archive zone
    p3 = get_number_buckets_by_zone(zones[0]) == num_buckets
    assert_equal(p3, True)
    # check number of buckets on archive zone
    p4 = get_number_buckets_by_zone(az_zones[0]) == (num_buckets_az + 1)
    assert_equal(p4, True)
    # get new bucket name on archive zone
    bucket_names_az = get_bucket_names_by_zone(az_zones[0])
    new_bucket_name_az = get_full_bucket_name(bucket_name + '-deleted-', bucket_names_az)
    p5 = new_bucket_name_az is not None
    assert_equal(p5, True)
    # create bucket on non archive zone
    _ = zones[0].create_bucket(new_bucket_name_az)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check number of buckets on non archive zone
    p6 = get_number_buckets_by_zone(zones[0]) == (num_buckets + 1)
    assert_equal(p6, True)
    # check number of buckets on archive zone
    p7 = get_number_buckets_by_zone(az_zones[0]) == (num_buckets_az + 2)
    assert_equal(p7, True)


def test_az_versioning_support_in_zones():
    """ test versioning support on zones """
    zones, az_zones = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on non archive zone
    bucket = zones[0].create_bucket(bucket_name)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # get bucket on archive zone
    bucket_az = az_zones[0].conn.get_bucket(bucket_name)
    # check non versioned buckets
    p1 = get_versioning_status(bucket) is None
    assert_equal(p1, True)
    p2 = get_versioning_status(bucket_az) is None
    assert_equal(p2, True)
    # create object on non archive zone
    key = bucket.new_key("foo")
    key.set_contents_from_string("zero")
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check bucket versioning
    p3 = get_versioning_status(bucket) is None
    assert_equal(p3, True)
    p4 = get_versioning_status(bucket_az) == 'Enabled'
    assert_equal(p4, True)
    # enable bucket versioning on non archive zone
    bucket.configure_versioning(True)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check bucket versioning
    p5 = get_versioning_status(bucket) == 'Enabled'
    assert_equal(p5, True)
    p6 = get_versioning_status(bucket_az) == 'Enabled'
    assert_equal(p6, True)
    # delete object on non archive zone
    key.delete()
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check delete-markers and versions on non archive zone
    (b_dm, b_ver) = get_versioned_entries(bucket)
    p7 = len(b_dm) == 1
    assert_equal(p7, True)
    p8 = len(b_ver) == 1
    assert_equal(p8, True)
    # check delete-markers and versions on archive zone
    (b_dm_az, b_ver_az) = get_versioned_entries(bucket_az)
    p9 = len(b_dm_az) == 1
    assert_equal(p9, True)
    p10 = len(b_ver_az) == 1
    assert_equal(p10, True)
    # delete delete-marker on non archive zone
    dm_version_id = b_dm[0]['foo']['version_id']
    bucket.delete_key("foo", version_id=dm_version_id)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check delete-markers and versions on non archive zone
    (b_dm, b_ver) = get_versioned_entries(bucket)
    p11 = len(b_dm) == 0
    assert_equal(p11, True)
    p12 = len(b_ver) == 1
    assert_equal(p12, True)
    # check delete-markers and versions on archive zone
    (b_dm_az, b_ver_az) = get_versioned_entries(bucket_az)
    p13 = len(b_dm_az) == 1
    assert_equal(p13, True)
    p14 = len(b_ver_az) == 1
    assert_equal(p14, True)
    # delete delete-marker on archive zone
    dm_az_version_id = b_dm_az[0]['foo']['version_id']
    bucket_az.delete_key("foo", version_id=dm_az_version_id)
    # sync
    zone_full_checkpoint(az_zones[0].zone, zones[0].zone)
    # check delete-markers and versions on non archive zone
    (b_dm, b_ver) = get_versioned_entries(bucket)
    p15 = len(b_dm) == 0
    assert_equal(p15, True)
    p16 = len(b_ver) == 1
    assert_equal(p16, True)
    # check delete-markers and versions on archive zone
    (b_dm_az, b_ver_az) = get_versioned_entries(bucket_az)
    p17 = len(b_dm_az) == 0
    assert_equal(p17, True)
    p17 = len(b_ver_az) == 1
    assert_equal(p17, True)
    # check body in zones
    obj_version_id = b_ver[0]['foo']['version_id']
    key = bucket.get_key("foo", version_id=obj_version_id)
    p18 = key.get_contents_as_string(encoding='ascii') == "zero"
    assert_equal(p18, True)
    obj_az_version_id = b_ver_az[0]['foo']['version_id']
    key_az = bucket_az.get_key("foo", version_id=obj_az_version_id)
    p19 = key_az.get_contents_as_string(encoding='ascii') == "zero"
    assert_equal(p19, True)
