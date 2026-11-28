import base64
import hashlib
import logging
import time
import urllib.error
import urllib.request

from nose import SkipTest
from nose.tools import assert_equal, assert_not_equal, assert_false, assert_in
from nose.plugins.attrib import attr

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from .tests import get_realm, \
    ZonegroupConns, \
    zonegroup_meta_checkpoint, \
    zone_meta_checkpoint, \
    zone_data_checkpoint, \
    zone_bucket_checkpoint, \
    bucket_lc_process, \
    gen_bucket_name


# configure logging for the tests module
log = logging.getLogger(__name__)


#########################################
# utility functions for backup zone tests
#########################################

def check_bz_configured():
    """ skip unless exactly one backup zone exists """
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    bz_zones = zonegroup.zones_by_type.get("backup")
    if bz_zones is None or len(bz_zones) != 1:
        raise SkipTest("Requires one backup zone")


def is_bz_zone(zone_conn):
    """ check whether a specific zone is a backup zone """
    if not zone_conn:
        return False
    return zone_conn.zone.tier_type() == "backup"


def init_env():
    """ initialize the environment, returning (rw zones, backup zones) """
    check_bz_configured()

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zonegroup_meta_checkpoint(zonegroup)

    bz_zones = []
    zones = []
    for conn in zonegroup_conns.zones:
        if is_bz_zone(conn):
            zone_meta_checkpoint(conn.zone)
            bz_zones.append(conn)
        elif not conn.zone.is_read_only():
            zones.append(conn)

    assert_not_equal(len(zones), 0)
    assert_not_equal(len(bz_zones), 0)
    return zones, bz_zones


def zone_full_checkpoint(target_zone, source_zone, bucket_name=None):
    """ wait for metadata and data to catch up

    Pass bucket_name whenever one is available.  zone_data_checkpoint()
    samples the source's datalog position once, before it starts polling, so
    an entry that has not been flushed yet at that moment is never waited for
    -- which makes a put/delete pair issued back-to-back racy.  'bucket sync
    checkpoint' re-reads the source bilog on every retry and does not have
    that hole.

    Only pass bucket_name once the bucket has been written to.  Bucket sync
    is started by a datalog entry, so for an empty bucket the target never
    creates a sync status and 'bucket sync checkpoint' waits for incremental
    sync until it times out.  Use zone_meta_checkpoint() for bucket metadata
    changes such as enabling versioning.
    """
    zone_meta_checkpoint(target_zone)
    zone_data_checkpoint(target_zone, source_zone)
    if bucket_name is not None:
        zone_bucket_checkpoint(target_zone, source_zone, bucket_name)


def list_versions(zone_conn, bucket_name, key=None):
    """ return (versions, delete_markers) for a key, newest-first per S3 """
    kwargs = {'Bucket': bucket_name}
    if key is not None:
        kwargs['Prefix'] = key
    resp = zone_conn.s3_client.list_object_versions(**kwargs)
    versions = [v for v in resp.get('Versions', [])
                if key is None or v['Key'] == key]
    markers = [m for m in resp.get('DeleteMarkers', [])
               if key is None or m['Key'] == key]
    return versions, markers


def latest_entry(zone_conn, bucket_name, key):
    """ return ('version'|'marker', entry) for whichever entry has IsLatest """
    versions, markers = list_versions(zone_conn, bucket_name, key)
    for v in versions:
        if v['IsLatest']:
            return ('version', v)
    for m in markers:
        if m['IsLatest']:
            return ('marker', m)
    return (None, None)


def get_body(zone_conn, bucket_name, key, version_id=None):
    """ return the object body as a string, or None if it does not exist """
    kwargs = {'Bucket': bucket_name, 'Key': key}
    if version_id is not None:
        kwargs['VersionId'] = version_id
    try:
        resp = zone_conn.s3_client.get_object(**kwargs)
    except ClientError:
        return None
    return resp['Body'].read().decode('utf-8')


def put_object(zone_conn, bucket_name, key, body):
    """ put an object, returning its version id ('null' if unversioned) """
    resp = zone_conn.s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
    return resp.get('VersionId', 'null')


def set_versioning(zone_conn, bucket_name, status):
    zone_conn.s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={'Status': status})


def log_entries(zone_conn, bucket_name, key, label):
    versions, markers = list_versions(zone_conn, bucket_name, key)
    log.debug('%s: versions=%s', label,
              [(v['VersionId'], v['IsLatest'], v['Size']) for v in versions])
    log.debug('%s: markers=%s', label,
              [(m['VersionId'], m['IsLatest']) for m in markers])


###################
# backup zone tests
###################

@attr('backup')
def test_bz_object_replication():
    """ objects written to a source zone reach the backup zone """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    put_object(source, bucket_name, 'foo', 'bar')

    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    assert_equal(get_body(backup, bucket_name, 'foo'), 'bar')


@attr('backup')
def test_bz_unversioned_delete_creates_marker():
    """ deleting from an unversioned source leaves a delete marker, data kept

    Covers the stale-OLH-epoch fix: the marker must actually be promoted to
    current, not written as a non-current instance.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    put_object(source, bucket_name, 'foo', 'bar')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    assert_equal(get_body(backup, bucket_name, 'foo'), 'bar')

    source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after delete')

    # the head of the backup bucket is a delete marker
    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'marker')
    # a plain GET now 404s
    assert_equal(get_body(backup, bucket_name, 'foo'), None)

    # but the data version is still there and readable by version id
    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(versions), 1)
    assert_equal(len(markers), 1)
    assert_equal(get_body(backup, bucket_name, 'foo',
                          version_id=versions[0]['VersionId']), 'bar')


@attr('backup')
def test_bz_unversioned_reupload_after_delete():
    """ re-uploading after a delete must resume replicating

    Covers the fetch_remote_obj -ENOENT fix: once a delete marker is current
    on the backup zone, copy_if_newer used to abort the fetch permanently.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    put_object(source, bucket_name, 'foo', 'bar')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after delete')
    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'marker')

    # upload the same key again
    put_object(source, bucket_name, 'foo', 'baz')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after re-upload')

    # the new object is current again, and its content replicated
    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'version')
    assert_equal(get_body(backup, bucket_name, 'foo'), 'baz')

    # nothing was lost: both data versions plus the marker are retained
    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(versions), 2)
    assert_equal(len(markers), 1)


@attr('backup')
def test_bz_versioned_delete_marker_replication():
    """ a delete marker on a versioned source becomes the backup's head """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)

    put_object(source, bucket_name, 'foo', 'bar')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    # delete without a version id -> source creates a delete marker
    source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after delete marker')

    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'marker')
    assert_equal(get_body(backup, bucket_name, 'foo'), None)

    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(versions), 1)
    assert_equal(len(markers), 1)


@attr('backup')
def test_bz_delete_latest_version_promotes_marker():
    """ deleting the source's latest version moves the backup's head

    Covers the promote_if_head path: the removed version *is* the head, so
    the marker must become current.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)

    v1 = put_object(source, bucket_name, 'foo', 'one')
    v2 = put_object(source, bucket_name, 'foo', 'two')
    assert_not_equal(v1, v2)
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    # the backup reuses the source's version ids for versioned buckets
    assert_equal(get_body(backup, bucket_name, 'foo', version_id=v1), 'one')
    assert_equal(get_body(backup, bucket_name, 'foo', version_id=v2), 'two')

    # delete the latest version explicitly
    source.s3_client.delete_object(Bucket=bucket_name, Key='foo', VersionId=v2)
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after deleting latest version')

    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'marker')
    assert_equal(get_body(backup, bucket_name, 'foo'), None)

    # both data versions are retained
    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(sorted(v['VersionId'] for v in versions), sorted([v1, v2]))
    assert_equal(len(markers), 1)


@attr('backup')
def test_bz_delete_old_version_keeps_head():
    """ deleting a non-latest source version must not move the backup's head

    Covers both the del_if_older fix (the marker has to be created at all)
    and the promote_if_head path (it must not be promoted).
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)

    v1 = put_object(source, bucket_name, 'foo', 'one')
    v2 = put_object(source, bucket_name, 'foo', 'two')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    # delete the *oldest* version, which is not the head
    source.s3_client.delete_object(Bucket=bucket_name, Key='foo', VersionId=v1)
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after deleting old version')

    # the head is untouched: v2 is still current and still readable
    kind, entry = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'version')
    assert_equal(entry['VersionId'], v2)
    assert_equal(get_body(backup, bucket_name, 'foo'), 'two')

    # the deletion was still recorded, as a non-current marker
    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(markers), 1)
    assert_false(markers[0]['IsLatest'])

    # and the deleted version's data is retained on the backup
    assert_equal(sorted(v['VersionId'] for v in versions), sorted([v1, v2]))
    assert_equal(get_body(backup, bucket_name, 'foo', version_id=v1), 'one')


@attr('backup')
def test_bz_suspended_versioning_delete_marker():
    """ null-version delete markers from a suspended bucket must replicate

    Covers the has_olh_epoch() squash fix: a LINK_OLH_DM sharing the squash
    key (object, "") with the null version used to be dropped.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)
    put_object(source, bucket_name, 'foo', 'one')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    set_versioning(source, bucket_name, 'Suspended')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    # writes in a suspended bucket land on the null version
    put_object(source, bucket_name, 'foo', 'two')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    # and a delete creates a null delete marker
    source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after suspended delete')

    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'marker')
    assert_equal(get_body(backup, bucket_name, 'foo'), None)

    # re-upload must resume replicating
    put_object(source, bucket_name, 'foo', 'three')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'version')
    assert_equal(get_body(backup, bucket_name, 'foo'), 'three')


@attr('backup')
def test_bz_delete_marker_removal_restores_object():
    """ removing a delete marker on the source restores the object on the backup

    A delete marker carries no user data, so replicating its removal costs the
    backup nothing: dropping our copy lets the olh fall back to the previous
    version, exactly as it does on the source.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)

    v1 = put_object(source, bucket_name, 'foo', 'one')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    assert_equal(get_body(backup, bucket_name, 'foo'), 'one')

    # delete with no version id -> a delete marker hides the object
    resp = source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    dm_id = resp['VersionId']
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after delete marker')
    kind, entry = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'marker')
    assert_equal(entry['VersionId'], dm_id)
    assert_equal(get_body(backup, bucket_name, 'foo'), None)

    # remove the delete marker -> the object is current again on the source
    source.s3_client.delete_object(Bucket=bucket_name, Key='foo', VersionId=dm_id)
    assert_equal(get_body(source, bucket_name, 'foo'), 'one')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after delete marker removal')

    # ... and the backup follows it back to the object
    kind, entry = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'version')
    assert_equal(entry['VersionId'], v1)
    assert_equal(get_body(backup, bucket_name, 'foo'), 'one')

    # the tombstone is gone on the backup too, and no data was lost
    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(markers), 0)
    assert_equal([v['VersionId'] for v in versions], [v1])


@attr('backup')
def test_bz_versioned_reupload_after_delete():
    """ a re-upload right after a delete must become the backup's head

    Data versions of a versioned source sync with the source's olh epoch (the
    source's clock when it linked them), but the backup stamps its delete
    markers with its own clock when it syncs them. If the source writes the
    next version before the backup has synced the marker, that version
    carries an older epoch than the marker, loses start_modify() and is
    stored as noncurrent: the backup hides an object the source still has.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)

    put_object(source, bucket_name, 'foo', 'one')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    # back to back, so the backup syncs the marker after 'two' was written
    source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    v2 = put_object(source, bucket_name, 'foo', 'two')
    assert_equal(get_body(source, bucket_name, 'foo'), 'two')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    log_entries(source, bucket_name, 'foo', 'source after re-upload')
    log_entries(backup, bucket_name, 'foo', 'backup after re-upload')

    # the backup's head follows the source's
    kind, entry = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'version')
    assert_equal(entry['VersionId'], v2)
    assert_equal(get_body(backup, bucket_name, 'foo'), 'two')

    # and the deleted version and its marker are both retained
    assert_equal(version_bodies(backup, bucket_name, 'foo'), ['one', 'two'])
    _, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(markers), 1)


#############################################
# utility functions for backup zone lifecycle
#############################################

def lifecycle_xml(noncurrent_days, newer_noncurrent=None, backup_zone=True,
                  expiration='<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>'):
    """ a single-rule lifecycle configuration, optionally scoped to backup zones """
    rule_filter = '<BackupZone/>' if backup_zone else '<Prefix></Prefix>'
    newer = ''
    if newer_noncurrent is not None:
        newer = '<NewerNoncurrentVersions>%d</NewerNoncurrentVersions>' % newer_noncurrent
    return ('<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            '<Rule>'
            '<ID>backup-retention</ID>'
            '<Filter>%s</Filter>'
            '<Status>Enabled</Status>'
            '<Expiration>%s</Expiration>'
            '<NoncurrentVersionExpiration>%s<NoncurrentDays>%d</NoncurrentDays>'
            '</NoncurrentVersionExpiration>'
            '</Rule>'
            '</LifecycleConfiguration>') % (rule_filter, expiration, newer, noncurrent_days)


def put_lifecycle_xml(zone_conn, bucket_name, xml):
    """ PUT ?lifecycle with a raw XML body, returning (status, body)

    boto3 validates lifecycle configurations against the AWS model, which has
    no BackupZone filter element, so sign and send the request ourselves.
    """
    client = zone_conn.s3_client
    body = xml.encode('utf-8')
    headers = {'Content-Type': 'application/xml',
               'Content-MD5': base64.b64encode(hashlib.md5(body).digest()).decode()}
    request = AWSRequest(method='PUT',
                         url='%s/%s?lifecycle' % (client.meta.endpoint_url, bucket_name),
                         data=body, headers=headers)
    credentials = Credentials(zone_conn.credentials.access_key,
                              zone_conn.credentials.secret)
    S3SigV4Auth(credentials, 's3', client.meta.region_name).add_auth(request)
    prepared = request.prepare()

    raw = urllib.request.Request(prepared.url, data=body,
                                 headers=dict(prepared.headers.items()),
                                 method='PUT')
    try:
        with urllib.request.urlopen(raw) as resp:
            return resp.status, resp.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8')


def set_lifecycle(source, backup, bucket_name, xml):
    """ set a lifecycle configuration on the source and wait for the backup to see it

    The configuration is bucket metadata, so it replicates; the backup zone
    registers the bucket for lifecycle processing when it syncs it.
    """
    status, body = put_lifecycle_xml(source, bucket_name, xml)
    assert_equal(status, 200, body)
    zone_meta_checkpoint(backup.zone)


def lc_process(zone_conn, bucket_name):
    """ run lifecycle once on one zone's copy of a bucket; a 'day' is one second """
    bucket_lc_process(zone_conn.zone, bucket_name, ['--rgw-lc-debug-interval=1'])


def version_bodies(zone_conn, bucket_name, key):
    """ the bodies of all data versions of a key, sorted """
    versions, _ = list_versions(zone_conn, bucket_name, key)
    return sorted(get_body(zone_conn, bucket_name, key, version_id=v['VersionId'])
                  for v in versions)


def put_and_sync(source, backup, bucket_name, key, bodies):
    """ write each body in turn, waiting for the backup after every write

    On an unversioned source each put replaces the previous object, so a
    version the backup has not fetched before the next put is never fetched.
    """
    for body in bodies:
        put_object(source, bucket_name, key, body)
        zone_full_checkpoint(backup.zone, source.zone, bucket_name)


#############################
# backup zone lifecycle tests
#############################

@attr('backup')
def test_bz_lc_backup_rule_skipped_on_source():
    """ a BackupZone rule runs on the backup zone only

    The configuration replicates to every zone, so zone_check() must skip it
    on the source and apply it on the backup.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)
    for body in ['one', 'two', 'three']:
        put_object(source, bucket_name, 'foo', body)
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    set_lifecycle(source, backup, bucket_name,
                  lifecycle_xml(noncurrent_days=1, newer_noncurrent=1))

    lc_process(source, bucket_name)
    assert_equal(version_bodies(source, bucket_name, 'foo'), ['one', 'three', 'two'])

    lc_process(backup, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after backup lifecycle')
    assert_equal(version_bodies(backup, bucket_name, 'foo'), ['three', 'two'])


@attr('backup')
def test_bz_lc_plain_rule_skipped_on_backup():
    """ a rule without BackupZone never runs on the backup zone """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    set_versioning(source, bucket_name, 'Enabled')
    zone_meta_checkpoint(backup.zone)
    for body in ['one', 'two', 'three']:
        put_object(source, bucket_name, 'foo', body)
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)

    set_lifecycle(source, backup, bucket_name,
                  lifecycle_xml(noncurrent_days=1, backup_zone=False))
    # let the noncurrent versions age past one 'day'
    time.sleep(3)

    lc_process(backup, bucket_name)
    assert_equal(version_bodies(backup, bucket_name, 'foo'), ['one', 'three', 'two'])

    # the rule itself is effective: it does expire versions on the source.
    # The backup is not checked after this, the source's expirations replicate.
    lc_process(source, bucket_name)
    assert_equal(version_bodies(source, bucket_name, 'foo'), ['three'])


@attr('backup')
def test_bz_lc_cap_purges_oldest_versions():
    """ NewerNoncurrentVersions caps the versions of a live object, whatever their age

    The cap is the number of noncurrent versions kept next to the current one.
    """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    bodies = ['v%d' % i for i in range(6)]
    put_and_sync(source, backup, bucket_name, 'foo', bodies)
    assert_equal(version_bodies(backup, bucket_name, 'foo'), sorted(bodies))

    # NoncurrentDays is far away: only the cap can purge here
    set_lifecycle(source, backup, bucket_name,
                  lifecycle_xml(noncurrent_days=3600, newer_noncurrent=3))
    lc_process(backup, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after cap')

    # current v5 plus the three newest noncurrent versions
    assert_equal(version_bodies(backup, bucket_name, 'foo'), ['v2', 'v3', 'v4', 'v5'])
    kind, _ = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'version')
    assert_equal(get_body(backup, bucket_name, 'foo'), 'v5')


@attr('backup')
def test_bz_lc_live_object_not_purged_by_age():
    """ without a cap, the versions of a live object are kept however old """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    put_and_sync(source, backup, bucket_name, 'foo', ['one', 'two', 'three'])

    set_lifecycle(source, backup, bucket_name, lifecycle_xml(noncurrent_days=1))
    # well past NoncurrentDays for every noncurrent version
    time.sleep(3)

    lc_process(backup, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after lifecycle')
    assert_equal(version_bodies(backup, bucket_name, 'foo'), ['one', 'three', 'two'])


@attr('backup')
def test_bz_lc_removed_object_purged_after_window():
    """ a removed object is purged NoncurrentDays after its delete

    The window counts from the delete marker, not from when each version
    became noncurrent, and the marker is cleaned up by
    ExpiredObjectDeleteMarker once it is the only entry left.
    """
    window = 30  # seconds, with --rgw-lc-debug-interval=1

    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)
    put_and_sync(source, backup, bucket_name, 'foo', ['one', 'two', 'three'])
    set_lifecycle(source, backup, bucket_name, lifecycle_xml(noncurrent_days=window))

    source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    kind, marker = latest_entry(backup, bucket_name, 'foo')
    assert_equal(kind, 'marker')
    expires_at = marker['LastModified'].timestamp() + window

    # inside the window the removed object stays restorable
    if time.time() < expires_at - 5:
        lc_process(backup, bucket_name)
        assert_equal(version_bodies(backup, bucket_name, 'foo'), ['one', 'three', 'two'])
    else:
        log.warning('sync took too long, skipping the in-window check')

    time.sleep(max(0, expires_at + 2 - time.time()))

    # past the window every version goes; the marker still has versions
    # listed behind it in this pass, so it is kept
    lc_process(backup, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after window')
    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(versions), 0)
    assert_equal(len(markers), 1)

    # the next pass finds the marker alone and removes it
    lc_process(backup, bucket_name)
    versions, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(versions), 0)
    assert_equal(len(markers), 0)


@attr('backup')
def test_bz_lc_markers_not_counted_in_cap():
    """ delete markers neither use up nor get purged by the version cap """
    zones, bz_zones = init_env()
    source, backup = zones[0], bz_zones[0]
    bucket_name = gen_bucket_name()

    source.create_bucket(bucket_name)

    # one, then a delete marker, then two: the marker sits between them
    put_and_sync(source, backup, bucket_name, 'foo', ['one'])
    source.s3_client.delete_object(Bucket=bucket_name, Key='foo')
    zone_full_checkpoint(backup.zone, source.zone, bucket_name)
    put_and_sync(source, backup, bucket_name, 'foo', ['two'])

    set_lifecycle(source, backup, bucket_name,
                  lifecycle_xml(noncurrent_days=3600, newer_noncurrent=1))

    # 'one' is the only noncurrent version, the marker does not count
    lc_process(backup, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after first pass')
    assert_equal(version_bodies(backup, bucket_name, 'foo'), ['one', 'two'])
    _, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(markers), 1)

    # now 'two' and 'one' are noncurrent: 'one' is over the cap, the marker stays
    put_and_sync(source, backup, bucket_name, 'foo', ['three'])
    lc_process(backup, bucket_name)
    log_entries(backup, bucket_name, 'foo', 'after second pass')
    assert_equal(version_bodies(backup, bucket_name, 'foo'), ['three', 'two'])
    _, markers = list_versions(backup, bucket_name, 'foo')
    assert_equal(len(markers), 1)


@attr('backup')
def test_bz_lc_rejects_current_expiration():
    """ a BackupZone rule must not expire current versions """
    zones, _ = init_env()
    source = zones[0]
    bucket_name = gen_bucket_name()
    source.create_bucket(bucket_name)

    status, body = put_lifecycle_xml(source, bucket_name,
                                     lifecycle_xml(noncurrent_days=1,
                                                   expiration='<Days>1</Days>'))
    assert_equal(status, 400, body)
    assert_in('InvalidRequest', body)
