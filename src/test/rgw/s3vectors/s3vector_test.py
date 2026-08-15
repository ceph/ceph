import logging
import random
import time
import threading
import subprocess
import os
import string
from datetime import datetime, timedelta, timezone
import pytest
import boto3
import json

from . import(
    configfile,
    get_config_host,
    get_config_port,
    get_config_zonegroup,
    get_access_key,
    get_secret_key,
    get_config_host2,
    get_config_port2,
    get_config_cluster,
    get_config_cluster2,
    get_config_rgw_client,
    get_config_master_cluster,
    is_s3_backend,
    get_s3vector_backend,
    get_s3vector_local_path,
    get_s3vector_s3_endpoint,
    get_s3vector_s3_region,
    get_s3vector_s3_allow_http
    )


# configure logging for the tests module
log = logging.getLogger(__name__)

num_buckets = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/../'

# seconds to wait for the ceph cluster before giving up on a command
cluster_connect_timeout = 10

def bash(cmd, **kwargs):
    log.debug('running command: %s', ' '.join(cmd))
    kwargs['stdout'] = subprocess.PIPE
    # errors are part of the output, so that they are reported when a command fails
    kwargs.setdefault('stderr', subprocess.STDOUT)
    process = subprocess.Popen(cmd, **kwargs)
    s = process.communicate()[0].decode('utf-8')
    return (s, process.returncode)


def installed_cluster_conf(cluster):
    """ the configuration file of an installed cluster, if it exists.
    "mrun" is always looking for a "ceph.conf", so it cannot be used when the
    clusters are named (which is the case in a multisite teuthology run) """
    conf = f'/etc/ceph/{cluster}.conf'
    return conf if os.path.exists(conf) else None


def admin(args, cluster=None, **kwargs):
    """ radosgw-admin command """
    cluster = cluster or get_config_cluster()
    if installed_cluster_conf(cluster):
        cmd = ['radosgw-admin', '--cluster', cluster,
               '-n', get_config_rgw_client() or 'client.0'] + args
    else:
        cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', cluster] + args
    return bash(cmd, **kwargs)


def ceph_admin(args, cluster=None, **kwargs):
    """ ceph command.
    the connect timeout prevents the command from blocking for the default 300
    seconds of "client_mount_timeout" when the cluster cannot be reached """
    cluster = cluster or get_config_cluster()
    timeout = ['--connect-timeout', str(cluster_connect_timeout)]
    if installed_cluster_conf(cluster):
        cmd = ['ceph', '--cluster', cluster] + timeout + args
    else:
        cmd = [test_path + 'test-rgw-call.sh', 'call_ceph', cluster] + timeout + args
    return bash(cmd, **kwargs)


def rgw_client_name(port=None):
    """ the name of the RGW client, as it is known to the cluster. it is set in the
    configuration when the RGW is not started by "vstart" """
    return get_config_rgw_client() or f'client.rgw.{port or get_config_port()}'


def try_set_rgw_config_option(option, value, port=None, cluster=None, client=None):
    """ change a config option of an RGW. returns the result of the command """
    client = client or rgw_client_name(port)
    out, ret = ceph_admin(['config', 'set', client, option, str(value)], cluster=cluster)
    if ret != 0:
        log.warning("could not set '%s' on '%s': %s", option, client, out)
    return out, ret


def set_rgw_config_option(option, value, port=None, cluster=None):
    """ change a config option of an RGW. skips the calling test if the cluster
    cannot be reached, since the test would run with the wrong value """
    out, ret = try_set_rgw_config_option(option, value, port, cluster)
    if ret != 0:
        pytest.skip(f"could not set '{option}': {out}")
    return out, ret


def _configure_backend(host, port, cluster):
    """ set the s3vector backend options on an RGW, so that it does not have to
    be started with them """
    backend = get_s3vector_backend()
    options = {'rgw_s3vector_backend': backend}
    if backend == 'local':
        # each RGW must have its own directory, otherwise they would share the data.
        # "<cluster>" and "<port>" are replaced with the values of the tested zone
        local_path = get_s3vector_local_path() or '/tmp/rgw-s3vector-<cluster>-<port>'
        options['rgw_s3vector_local_path'] = local_path.replace(
            '<cluster>', cluster).replace('<port>', str(port))
    elif backend == 's3':
        allow_http = get_s3vector_s3_allow_http()
        # by default, the RGW is sending the S3 requests to itself
        scheme = 'http' if allow_http in ('true', 'True', '1') else 'https'
        options['rgw_s3vector_s3_endpoint'] = get_s3vector_s3_endpoint() or \
            f'{scheme}://{host}:{port}'
        options['rgw_s3vector_s3_region'] = get_s3vector_s3_region() or get_config_zonegroup()
        options['rgw_s3vector_s3_allow_http'] = allow_http

    # the tests must run against the backend they were configured with: if it
    # cannot be set, the run is failed, and not done against another backend
    client = rgw_client_name(port)
    for option, value in options.items():
        out, ret = try_set_rgw_config_option(option, value, port=port, cluster=cluster, client=client)
        assert ret == 0, \
            f"failed to set '{option}' of the '{backend}' backend on '{client}' of cluster '{cluster}': {out}"
    log.info("configured the '%s' backend on '%s' of cluster '%s'", backend, client, cluster)


@pytest.fixture(autouse=True, scope="module")
def configure_backend(configfile):
    """ configure the s3vector backend on the RGWs of all zones """
    _configure_backend(get_config_host(), get_config_port(), get_config_cluster())
    if get_config_host2():
        _configure_backend(get_config_host2(), get_config_port2(), get_config_cluster2())


# the group is created on the zonegroup, and applies to all of its buckets
data_sync_group_id = 's3vector-tests-no-data-sync'


@pytest.fixture(autouse=True, scope="module")
def forbid_data_sync(configfile):
    """ forbid data sync between the zones of the zonegroup.
    the buckets backing the vector buckets hold LanceDB files, and each zone must
    have its own copy of them. this is done at the zonegroup level, and not per
    bucket, so that there is no window in which a bucket is created but its data
    may still be synced. note that the metadata of the entities is still synced.
    """
    if not get_config_host2() or not is_s3_backend():
        # not a multisite configuration, or a backend that does not store the
        # vector data in S3 buckets, so there is nothing that may be synced
        yield
        return

    # the sync policy may be changed only on the master zone, and a change of the
    # zonegroup policy requires a period update
    cluster = get_config_master_cluster()
    out, ret = admin(['sync', 'group', 'create',
                      '--group-id', data_sync_group_id,
                      '--status', 'forbidden'], cluster=cluster)
    if ret != 0:
        log.warning("failed to forbid data sync: %s. the data of the zones may be synced", out)
        yield
        return
    admin(['sync', 'group', 'pipe', 'create',
           '--group-id', data_sync_group_id, '--pipe-id', 'all',
           '--source-zones', '*', '--source-bucket', '*',
           '--dest-zones', '*', '--dest-bucket', '*'], cluster=cluster)
    admin(['period', 'update', '--commit'], cluster=cluster)
    log.info("data sync is forbidden on the zonegroup")

    yield

    admin(['sync', 'group', 'remove', '--group-id', data_sync_group_id], cluster=cluster)
    admin(['period', 'update', '--commit'], cluster=cluster)
    log.info("data sync is allowed again on the zonegroup")


def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    return 'kaboom' + run_prefix + '-' + str(num_buckets)


def connection(service_name='s3vectors'):
    hostname = get_config_host()
    port_no = get_config_port()
    access_key = get_access_key()
    secret_key = get_secret_key()
    if port_no == 443 or port_no == 8443:
        scheme = 'https://'
    else:
        scheme = 'http://'

    client = boto3.client(service_name,
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=get_config_zonegroup())

    return client

def connection2(service_name='s3vectors'):
    hostname = get_config_host2()
    if not hostname:
        log.info("No second host configured")
        return None
    port_no = get_config_port2()
    if not port_no:
        log.info("No second port configured")
        return None
    access_key = get_access_key()
    secret_key = get_secret_key()
    if port_no == 443 or port_no == 8443:
        scheme = 'https://'
    else:
        scheme = 'http://'

    client = boto3.client(service_name,
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=get_config_zonegroup())

    return client


def _wait_for_user(uid, tenant=None, retries=12, delay=5):
    """ wait for a user of the master zone to be synced to the tested zone """
    if get_config_cluster() == (get_config_master_cluster() or get_config_cluster()):
        # the user was created on the tested zone
        return
    args = ['user', 'info', '--uid', uid]
    if tenant:
        args += ['--tenant', tenant]
    for _ in range(retries):
        _, result = admin(args)
        if result == 0:
            return
        time.sleep(delay)
    log.warning("user '%s' was not synced to cluster '%s'", uid, get_config_cluster())


def another_user(tenant=None):
    # users may be created only on the master zone, and are synced from there. so,
    # when the tests are running against another zone, no user can be created
    master_cluster = get_config_master_cluster()
    if master_cluster and master_cluster != get_config_cluster():
        pytest.skip(f"cannot create a user: the master zone is in cluster "
                    f"'{master_cluster}', and the tested zone is in '{get_config_cluster()}'")

    access_key = str(time.time())
    secret_key = str(time.time())
    uid = 'superman' + str(time.time())
    args = ['user', 'create', '--uid', uid,
            '--access-key', access_key, '--secret-key', secret_key,
            '--display-name', '"Super Man"']
    if tenant:
        args += ['--tenant', tenant]
    out, result = admin(args, cluster=master_cluster)
    assert result == 0, f"failed to create user '{uid}': {out}"
    _wait_for_user(uid, tenant)
    hostname = get_config_host()
    port_no = get_config_port()
    if port_no == 443 or port_no == 8443:
        scheme = 'https://'
    else:
        scheme = 'http://'

    client = boto3.client('s3vectors',
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=get_config_zonegroup())

    client.uid = uid
    return client


#################
# s3vectors tests
#################

def _ensure_s3_bucket_for_vector_bucket(bucket_name, s3conn=None, retries=12, delay=5):
    """
    When using S3/SAL backend, create a regular S3 bucket with the same name
    as the vector bucket. Required because these backends store LanceDB data
    directly in an S3 bucket with the vector bucket name.
    In a multisite configuration, the connection of the second zone should be
    used to create the bucket there as well, since a vector bucket cannot be
    used in a zone before its backing bucket exists in it. Note that a creation
    request in a zone that is not the master zone is forwarded to the master,
    and the bucket is created locally afterwards.
    """
    if not is_s3_backend():
        return
    if not s3conn:
        s3conn = connection('s3')
    for attempt in range(retries):
        try:
            s3conn.head_bucket(Bucket=bucket_name)
            log.info("S3 bucket '%s' already exists", bucket_name)
            return
        except s3conn.exceptions.ClientError as err:
            error_code = err.response['Error']['Code']
            if error_code in ('404', 'NoSuchBucket'):
                log.info("Creating S3 bucket '%s' for S3/RGW backend", bucket_name)
                _create_s3bucket(s3conn, bucket_name)
                return
            if error_code not in ('403', 'AccessDenied', 'Forbidden'):
                raise
            # the bucket, or the user owning it, was created on another zone and
            # was not synced to this one yet, so it is not accessible here. this
            # is not a permission error, and is expected to resolve itself
            log.info("S3 bucket '%s' is not accessible yet (%s), attempt %d/%d",
                     bucket_name, error_code, attempt + 1, retries)
            time.sleep(delay)
    raise AssertionError(
        f"S3 bucket '{bucket_name}' was still not accessible after "
        f"{retries * delay} seconds")


def _delete_s3_bucket_for_vector_bucket(bucket_name):
    """
    When using S3/SAL backend, delete the regular S3 bucket that was created
    for the vector bucket.
    """
    if not is_s3_backend():
        return
    s3conn = connection('s3')
    try:
        paginator = s3conn.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects:
                    s3conn.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})
        s3conn.delete_bucket(Bucket=bucket_name)
        log.info("Deleted S3 bucket '%s'", bucket_name)
    except s3conn.exceptions.ClientError as err:
        if err.response['Error']['Code'] in ('404', 'NoSuchBucket'):
            log.info("S3 bucket '%s' does not exist, nothing to delete", bucket_name)
        else:
            log.warning("Failed to delete S3 bucket '%s': %s", bucket_name, str(err))


def _purge_all_versions(s3conn, bucket_name):
    """
    Remove every version and delete marker from a versioned S3 bucket, so that
    it is actually empty and can be deleted.
    """
    paginator = s3conn.get_paginator('list_object_versions')
    for page in paginator.paginate(Bucket=bucket_name):
        objects = []
        for v in page.get('Versions', []):
            objects.append({'Key': v['Key'], 'VersionId': v['VersionId']})
        for dm in page.get('DeleteMarkers', []):
            objects.append({'Key': dm['Key'], 'VersionId': dm['VersionId']})
        if objects:
            s3conn.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})


def _delete_all_indexes(conn, bucket_name):
    """
    Delete all indexes of a vector bucket. must be done before the vector bucket
    itself is deleted, since deleting a vector bucket with indexes is not allowed
    """
    result = conn.list_indexes(vectorBucketName=bucket_name)
    for index in result['indexes']:
        _ = conn.delete_index(vectorBucketName=bucket_name, indexName=index['indexName'])


def _delete_vector_bucket(conn, bucket_name):
    """Delete a vector bucket together with all of its indexes"""
    _delete_all_indexes(conn, bucket_name)
    return conn.delete_vector_bucket(vectorBucketName=bucket_name)


def _delete_all_vector_buckets(conn, conn2=None):
    """
    Delete all vector buckets of a zone, and of the other zone when its
    connection is given. A backing bucket is shared by the zones, so it is
    deleted only after the vector buckets of both zones are gone.
    """
    bucket_names = []
    for c in filter(None, (conn, conn2)):
        result = c.list_vector_buckets()
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        for bucket in result['vectorBuckets']:
            bucket_name = bucket['vectorBucketName']
            if bucket_name not in bucket_names:
                bucket_names.append(bucket_name)
            try:
                _ = _delete_vector_bucket(c, bucket_name)
            except c.exceptions.ClientError as err:
                log.warning("Failed to delete vector bucket '%s': %s", bucket_name, str(err))

    for bucket_name in bucket_names:
        _delete_s3_bucket_for_vector_bucket(bucket_name)


@pytest.mark.vector_bucket_test
def test_create_vector_bucket():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    log.info('create_vector_bucket result: %s', result)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert result['vectorBucketArn'] == 'arn:aws:s3vectors:::bucket/{}'.format(bucket_name)
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.skip(reason="connection does not fail even with permission change")
@pytest.mark.vector_bucket_test
def test_create_vector_bucket_bad_path():
    conn = connection()
    bucket_name = gen_bucket_name()
    db_path = '/tmp/lancedb/'
    os.makedirs(db_path, exist_ok=True)
    original_mode = os.stat(db_path).st_mode
    os.chmod(db_path, 0o555)
    try:
        pytest.raises(conn.exceptions.ClientError, conn.create_vector_bucket, vectorBucketName=bucket_name)
    finally:
        os.chmod(db_path, original_mode)


@pytest.mark.vector_bucket_test
def test_get_vector_bucket():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    bucket_arn = result['vectorBucketArn']
    result = conn.get_vector_bucket(vectorBucketName=bucket_name)
    log.info("get_vector_buckets result: %s", result)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_vector_bucket(vectorBucketArn=bucket_arn)
    log.info("get_vector_buckets result: %s", result)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    invalid_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.get_vector_bucket, vectorBucketName=invalid_name)
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.vector_bucket_test
def test_delete_vector_bucket():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    _delete_s3_bucket_for_vector_bucket(bucket_name)
    pytest.raises(conn.exceptions.ClientError, conn.get_vector_bucket, vectorBucketName=bucket_name)
    pytest.raises(conn.exceptions.ClientError, conn.delete_vector_bucket, vectorBucketName=bucket_name)
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # note that vector buckets of other tests, or of previous runs, may still exist
    assert bucket_name not in [b['vectorBucketName'] for b in result['vectorBuckets']]
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.vector_bucket_test
def test_delete_vector_bucket_with_indexes():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # deleting a vector bucket which still has indexes should fail with a conflict
    with pytest.raises(conn.exceptions.ClientError) as exc_info:
        conn.delete_vector_bucket(vectorBucketName=bucket_name)
    assert exc_info.value.response['ResponseMetadata']['HTTPStatusCode'] == 409
    assert exc_info.value.response['Error']['Code'] == 'BucketNotEmpty'
    # the vector bucket and its index should still exist
    result = conn.get_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # once the index is deleted, the vector bucket could be deleted
    result = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    _delete_s3_bucket_for_vector_bucket(bucket_name)
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.vector_bucket_test
def test_list_vector_buckets():
    conn = connection()
    bucket_name1 = gen_bucket_name()
    bucket_name2 = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name1)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    _ensure_s3_bucket_for_vector_bucket(bucket_name2)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name1 in bucket_names
    assert bucket_name2 in bucket_names
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.multisite_test
def test_vector_buckets_not_synced():
    """Test that vector buckets are not synced between the zones of a zonegroup."""
    conn = connection()
    conn2 = connection2()
    if not conn2:
        log.info("Skipping test_vector_buckets_not_synced since second connection is not configured")
        return

    # the backing buckets are regular S3 buckets, and must exist in the zone in
    # which their vector bucket is used
    bucket_name1 = gen_bucket_name()
    bucket_name2 = gen_bucket_name()
    s3conn2 = connection2('s3')
    _ensure_s3_bucket_for_vector_bucket(bucket_name1)
    _ensure_s3_bucket_for_vector_bucket(bucket_name1, s3conn2)
    _ensure_s3_bucket_for_vector_bucket(bucket_name2, s3conn2)

    # create a vector bucket from each connection
    result = conn.create_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn2.create_vector_bucket(vectorBucketName=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    time.sleep(5)

    # each zone sees only the vector buckets that were created in it
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name1 in bucket_names
    assert bucket_name2 not in bucket_names

    result = conn2.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets from conn2 result: %s", result)
    bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name2 in bucket_names
    assert bucket_name1 not in bucket_names

    # a vector bucket of the other zone cannot be used
    pytest.raises(conn.exceptions.ClientError, conn.get_vector_bucket, vectorBucketName=bucket_name2)
    pytest.raises(conn2.exceptions.ClientError, conn2.get_vector_bucket, vectorBucketName=bucket_name1)

    # the same name may be used for a different vector bucket in the other zone
    result = conn2.create_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    time.sleep(5)

    # deleting it in one zone does not affect the other
    result = conn2.delete_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    time.sleep(5)
    result = conn.get_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # cleanup
    _delete_all_vector_buckets(conn, conn2)


@pytest.mark.multisite_test
def test_vector_data_not_synced():
    """Test that a vector bucket of the same name in another zone is a different
    bucket, holding its own vectors."""
    conn = connection()
    conn2 = connection2()
    if not conn2:
        log.info("Skipping test_vector_data_not_synced since second connection is not configured")
        return

    dimension = 4
    index_name = 'test-index'
    bucket_name = gen_bucket_name()
    # the backing bucket must exist in both zones. data sync is forbidden for the
    # entire zonegroup during the run, so each zone holds its own objects
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    _ensure_s3_bucket_for_vector_bucket(bucket_name, connection2('s3'))

    # the same vector bucket and index are created in both zones
    for c in (conn, conn2):
        result = c.create_vector_bucket(vectorBucketName=bucket_name)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        result = c.create_index(vectorBucketName=bucket_name, indexName=index_name,
                                dataType='float32', dimension=dimension,
                                distanceMetric='euclidean')
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # vectors are written only in the first zone
    num_vectors = 5
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    time.sleep(5)

    # the first zone has them
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == num_vectors

    # and the second zone has none
    result = conn2.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vectors from conn2 result: %s", result)
    assert len(result['vectors']) == 0
    result = conn2.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                               keys=[v['key'] for v in vectors])
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 0
    result = conn2.query_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                 queryVector=generate_data(dimension, 0), topK=num_vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 0

    # cleanup
    for c in (conn2, conn):
        _ = _delete_vector_bucket(c, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)


def _create_s3bucket(s3conn, bucket_name):
    try:
        result = s3conn.create_bucket(Bucket=bucket_name)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    except s3conn.exceptions.ClientError as err:
        log.warning("s3 bucket creation failed with: %s", str(err))
        assert err.response['ResponseMetadata']['HTTPStatusCode'] == 500


@pytest.mark.vector_bucket_test
def test_vector_buckets_creation_with_buckets():
    conn = connection()
    s3conn = connection('s3')
    bucket_name1 = gen_bucket_name()
    # create vector bucket
    _ensure_s3_bucket_for_vector_bucket(bucket_name1)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # create s3 bucket with the same name
    _create_s3bucket(s3conn, bucket_name1)
    # create another s3 bucket
    bucket_name2 = gen_bucket_name()
    _create_s3bucket(s3conn, bucket_name2)
    # list vector buckets and verify only one bucket there
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    vector_bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name1 in vector_bucket_names
    assert bucket_name2 not in vector_bucket_names
    # list s3 buckets and verify both bucket there
    result = s3conn.list_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_buckets result: %s", result)
    s3_bucket_names = [b['Name'] for b in result['Buckets']]
    assert bucket_name1 in s3_bucket_names
    assert bucket_name2 in s3_bucket_names
    # now try to create a vector bucket with a name that already exists as an s3 bucket
    _ensure_s3_bucket_for_vector_bucket(bucket_name2)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # list vector buckets and verify both bucket there
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    vector_bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name1 in vector_bucket_names
    assert bucket_name2 in vector_bucket_names
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.vector_bucket_test
def test_vector_buckets_deletion_with_buckets():
    conn = connection()
    s3conn = connection('s3')
    bucket_name1 = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name1)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # create s3 bucket with the same name
    _create_s3bucket(s3conn, bucket_name1)

    # verify vector bucket exists (via get and list)
    result = conn.get_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    vector_bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name1 in vector_bucket_names
    # verify s3 bucket exists (via get and list)
    result = s3conn.head_bucket(Bucket=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = s3conn.list_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_buckets result: %s", result)
    bucket_names = [b['Name'] for b in result['Buckets']]
    assert bucket_name1 in bucket_names


    # delete vector bucket
    result = conn.delete_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # verify vector bucket is not there (via get and list)
    pytest.raises(conn.exceptions.ClientError, conn.get_vector_bucket, vectorBucketName=bucket_name1)
    # verify s3 bucket still exists
    result = s3conn.head_bucket(Bucket=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = s3conn.list_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_buckets result: %s", result)
    s3_bucket_names = [b['Name'] for b in result['Buckets']]
    assert bucket_name1 in s3_bucket_names
    # create another vector bucket
    bucket_name2 = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name2)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # and an s3 bucket with the same name
    _create_s3bucket(s3conn, bucket_name2)
    # delete the s3 bucket: for S3/SAL backends, empty it first (LanceDB data files);
    # for local backend, the bucket is empty since LanceDB uses local filesystem.
    if is_s3_backend():
        paginator = s3conn.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name2):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects:
                    s3conn.delete_objects(Bucket=bucket_name2, Delete={'Objects': objects})
    s3conn.delete_bucket(Bucket=bucket_name2)
    # verify s3 bucket is not there
    pytest.raises(s3conn.exceptions.ClientError, s3conn.head_bucket, Bucket=bucket_name2)
    # verify vector bucket still exists (via get and list)
    result = conn.get_vector_bucket(vectorBucketName=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    vector_bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name2 in vector_bucket_names
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.index_test
def test_create_index():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert result['indexArn'] == 'arn:aws:s3vectors:::bucket/{}/index/{}'.format(bucket_name, index_name)
    # create with same name should fail with ConflictException
    with pytest.raises(conn.exceptions.ClientError) as exc_info:
        conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert exc_info.value.response['Error']['Code'] == 'BucketAlreadyExists'
    # create an index on bucket that does not exist
    invalid_bucket_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.create_index, vectorBucketName=invalid_bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_create_index_invalid_filterable_keys():
    """Test that invalid filterable metadata key names fail with ValidationException."""
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    common = dict(vectorBucketName=bucket_name, dataType='float32', dimension=4, distanceMetric='euclidean')

    # duplicate field names
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys[1].name',
        indexName='dup-fields',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'genre'}, {'name': 'genre'}
        ]}, **common)

    # duplicate field names, even when the declarations differ
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys[1].name',
        indexName='dup-fields-diff-type',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'year', 'type': 'Number'}, {'name': 'year', 'type': 'String'}
        ]}, **common)

    # duplicate non-filterable field names
    assert_create_index_validation_error(conn,
        'metadataConfiguration.nonFilterableMetadataKeys[2]',
        indexName='dup-nonfilterable',
        metadataConfiguration={'nonFilterableMetadataKeys': [
            'genre', 'year', 'genre'
        ]}, **common)

    # reserved column name: key
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys',
        indexName='reserved-key',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'key'}
        ]}, **common)

    # reserved column name: data
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys',
        indexName='reserved-data',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'data'}
        ]}, **common)

    # reserved column name: metadata
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys',
        indexName='reserved-metadata',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'metadata'}
        ]}, **common)

    # filterable key name starting with underscore
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys[0].name',
        indexName='underscore-key',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': '_internal'}
        ]}, **common)

    # overlap between filterable and non-filterable keys
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys[0].name',
        indexName='overlap-keys',
        metadataConfiguration={
            'nonFilterableMetadataKeys': ['genre', 'year'],
            'filterableMetadataKeys': [{'name': 'genre'}]
        }, **common)

    # up to 10 filterable keys are allowed
    result = conn.create_index(indexName='max-filterable',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': f'key-{i}'} for i in range(10)
        ]}, **common)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # more than 10 filterable keys are rejected
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys',
        indexName='too-many-filterable',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': f'key-{i}'} for i in range(11)
        ]}, **common)

    # up to 10 non-filterable keys are allowed
    result = conn.create_index(indexName='max-nonfilterable',
        metadataConfiguration={'nonFilterableMetadataKeys': [
            f'key-{i}' for i in range(10)
        ]}, **common)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # more than 10 non-filterable keys are rejected
    assert_create_index_validation_error(conn,
        'metadataConfiguration.nonFilterableMetadataKeys',
        indexName='too-many-nonfilterable',
        metadataConfiguration={'nonFilterableMetadataKeys': [
            f'key-{i}' for i in range(11)
        ]}, **common)

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_get_index():
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 128
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_arn = result['indexArn']
    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_index(indexArn=index_arn)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info('get_index result: %s', result)
    assert result["index"]["dimension"] == dimension
    assert result["index"]["indexName"] == index_name
    assert result["index"]['indexArn'] == 'arn:aws:s3vectors:::bucket/{}/index/{}'.format(bucket_name, index_name)
    assert result["index"]["creationTime"] > datetime.now(timezone.utc) - timedelta(days=1), "creationTime should be within the last day"
    assert result["index"]["distanceMetric"] == "euclidean"
    # get an index from bucket that does not exist
    invalid_bucket_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.get_index, vectorBucketName=invalid_bucket_name, indexName=index_name)
    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_non_filterable_metadata_keys():
    """Test that nonFilterableMetadataKeys is stored on CreateIndex and returned on GetIndex."""
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    nonfilterable_keys = ['key1', 'key2', 'key3']
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=128, distanceMetric='euclidean',
        metadataConfiguration={'nonFilterableMetadataKeys': nonfilterable_keys})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    returned_keys = result['index']['metadataConfiguration']['nonFilterableMetadataKeys']
    assert set(returned_keys) == set(nonfilterable_keys), \
        f"expected {nonfilterable_keys} but got {returned_keys}"

    # create index without nonFilterableMetadataKeys
    index_name2 = 'test-index2'
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name2,
        dataType='float32', dimension=64, distanceMetric='cosine')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    returned_keys = result['index']['metadataConfiguration']['nonFilterableMetadataKeys']
    assert returned_keys == [], f"expected empty list but got {returned_keys}"

    log.info('test_non_filterable_metadata_keys: verified nonFilterableMetadataKeys round-trip')

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_filterable_metadata_keys():
    """Test filterableMetadataKeys: store on CreateIndex, retrieve on GetIndex,
    and populate filterable columns via PutVectors."""
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    dimension = 8
    filterable_keys = [
        {'name': 'genre', 'type': 'String'},
        {'name': 'year', 'type': 'Number'},
        {'name': 'popular', 'type': 'Boolean'}
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={
            'filterableMetadataKeys': filterable_keys
        })
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # verify filterableMetadataKeys are returned on GetIndex
    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    returned_filterable = result['index']['metadataConfiguration'].get('filterableMetadataKeys', [])
    assert len(returned_filterable) == len(filterable_keys), \
        f"expected {len(filterable_keys)} filterable keys but got {len(returned_filterable)}"
    returned_names = {k['name'] for k in returned_filterable}
    expected_names = {k['name'] for k in filterable_keys}
    assert returned_names == expected_names, \
        f"expected names {expected_names} but got {returned_names}"

    # put vectors with metadata that includes filterable fields plus extra keys not in the list
    vectors = []
    genres = ['rock', 'jazz', 'pop', 'rock', 'jazz']
    for i in range(5):
        v = {
            'key': f'vec-{i}',
            'data': generate_data(dimension, i),
            'metadata': json.dumps({
                'genre': genres[i],
                'year': 2000 + i,
                'popular': i % 2 == 0,
                'artist': f'artist-{i}',
                'rating': 4.5 + i * 0.1
            })
        }
        vectors.append(v)

    # vectors with metadata containing only keys NOT in the filterable list
    for i in range(5, 8):
        v = {
            'key': f'vec-{i}',
            'data': generate_data(dimension, i),
            'metadata': json.dumps({
                'artist': f'artist-{i}',
                'rating': 3.0 + i * 0.1
            })
        }
        vectors.append(v)

    # vectors with no metadata at all
    for i in range(8, 10):
        v = {
            'key': f'vec-{i}',
            'data': generate_data(dimension, i),
        }
        vectors.append(v)

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # verify all vectors can be retrieved with correct metadata
    all_keys = [f'vec-{i}' for i in range(10)]
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=all_keys, returnMetadata=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 10

    for vector in result['vectors']:
        key = vector['key']
        idx = int(key.split('-')[1])
        if idx < 5:
            assert 'metadata' in vector, f"{key} should have metadata"
            md = json.loads(vector['metadata'])
            assert md['genre'] == genres[idx], f"{key} genre mismatch"
            assert md['year'] == 2000 + idx, f"{key} year mismatch"
            assert md['popular'] == (idx % 2 == 0), f"{key} popular mismatch"
            assert md['artist'] == f'artist-{idx}', f"{key} artist mismatch"
        elif idx < 8:
            assert 'metadata' in vector, f"{key} should have metadata"
            md = json.loads(vector['metadata'])
            assert 'genre' not in md, f"{key} should not have genre"
            assert md['artist'] == f'artist-{idx}', f"{key} artist mismatch"
        else:
            assert 'metadata' not in vector, f"{key} should not have metadata"

    log.info('test_filterable_metadata_keys: verified filterable metadata round-trip')

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_filterable_metadata_list_keys():
    """Test filterableMetadataKeys with list types: StringList, NumberList, BooleanList."""
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-list-index'
    dimension = 4
    filterable_keys = [
        {'name': 'genre', 'type': 'String'},
        {'name': 'tags', 'type': 'StringList'},
        {'name': 'scores', 'type': 'NumberList'},
        {'name': 'flags', 'type': 'BooleanList'}
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='cosine',
        metadataConfiguration={
            'filterableMetadataKeys': filterable_keys
        })
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # verify filterableMetadataKeys with list types are returned on GetIndex
    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    returned_filterable = result['index']['metadataConfiguration'].get('filterableMetadataKeys', [])
    assert len(returned_filterable) == len(filterable_keys), \
        f"expected {len(filterable_keys)} filterable keys but got {len(returned_filterable)}"
    for rk in returned_filterable:
        expected = next(fk for fk in filterable_keys if fk['name'] == rk['name'])
        assert rk['type'] == expected['type'], \
            f"expected type {expected['type']} for {rk['name']} but got {rk['type']}"

    # put vectors with list-type metadata
    vectors = [
        {
            'key': 'vec-0',
            'data': generate_data(dimension, 0),
            'metadata': json.dumps({
                'tags': ['rock', 'pop'],
                'scores': [1.5, 2.5, 3.5],
                'flags': [True, False, True]
            })
        },
        {
            'key': 'vec-1',
            'data': generate_data(dimension, 1),
            'metadata': json.dumps({
                'genre': 'jazz',
                'tags': ['jazz'],
                'flags': [False]
            })
        },
        {
            'key': 'vec-2',
            'data': generate_data(dimension, 2),
            'metadata': json.dumps({
                'description': 'no list keys here'
            })
        },
        {
            'key': 'vec-3',
            'data': generate_data(dimension, 3),
        },
    ]

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # verify vectors can be retrieved with correct metadata
    all_keys = [f'vec-{i}' for i in range(4)]
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=all_keys, returnMetadata=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 4

    for vector in result['vectors']:
        key = vector['key']
        idx = int(key.split('-')[1])
        if idx == 0:
            md = json.loads(vector['metadata'])
            assert md['tags'] == ['rock', 'pop'], f"{key} tags mismatch"
            assert md['scores'] == [1.5, 2.5, 3.5], f"{key} scores mismatch"
            assert md['flags'] == [True, False, True], f"{key} flags mismatch"
        elif idx == 1:
            md = json.loads(vector['metadata'])
            assert md['genre'] == 'jazz', f"{key} genre mismatch"
            assert md['tags'] == ['jazz'], f"{key} tags mismatch"
            assert 'scores' not in md, f"{key} should not have scores"
            assert md['flags'] == [False], f"{key} flags mismatch"
        elif idx == 2:
            md = json.loads(vector['metadata'])
            assert 'genre' not in md, f"{key} should not have genre"
            assert 'tags' not in md, f"{key} should not have tags"
            assert 'scores' not in md, f"{key} should not have scores"
            assert 'flags' not in md, f"{key} should not have flags"
            assert md['description'] == 'no list keys here'
        else:
            assert 'metadata' not in vector, f"{key} should not have metadata"

    log.info('test_filterable_metadata_list_keys: verified list-type filterable metadata round-trip')

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_metadata_dots_in_names_rejected():
    """Test that dots in metadata key names are rejected at index creation."""
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    dimension = 4

    # dots in filterable metadata key names
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys[0].name',
        vectorBucketName=bucket_name, indexName='test-dot-filterable',
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': [{'name': 'user.name'}]})

    # dots in non-filterable metadata key names
    assert_create_index_validation_error(conn,
        'metadataConfiguration.nonFilterableMetadataKeys[0]',
        vectorBucketName=bucket_name, indexName='test-dot-nonfilterable',
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'nonFilterableMetadataKeys': ['user.name']})

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_delete_index():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # delete an index from bucket that does not exist
    invalid_bucket_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.delete_index, vectorBucketName=invalid_bucket_name, indexName=index_name)
    # delete the index from the right bucket
    result = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # try to get the index
    with pytest.raises(conn.exceptions.ClientError):
        result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    # deleting an index that does not exist is not an error
    result = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.delete_index(vectorBucketName=bucket_name, indexName='no-such-index')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.index_test
def test_list_indexes():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name1 = 'test-index1'
    index_name2 = 'test-index2'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name1, dataType='float32', dimension=128, distanceMetric='cosine')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name2, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.list_indexes(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info('list_indexes result: %s', result)
    index_names = [i['indexName'] for i in result['indexes']]
    assert index_name1 in index_names
    assert index_name2 in index_names
    for idx in result['indexes']:
        assert idx['creationTime'] > datetime.now(timezone.utc) - timedelta(days=1), "creationTime should be within the last day"
    # list indexs from bucket that does not exist
    invalid_bucket_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.list_indexes, vectorBucketName=invalid_bucket_name)
    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)


def assert_put_vectors_validation_error(conn, expected_paths, **kwargs):
    """Call put_vectors expecting a ValidationException, verify the fieldList paths.
    expected_paths can be a single string or a list of strings."""
    if isinstance(expected_paths, str):
        expected_paths = [expected_paths]
    captured = {}
    def capture(**kw):
        captured['body'] = kw['http_response'].content
    event = 'after-call.s3vectors.PutVectors'
    conn.meta.events.register(event, capture)
    try:
        with pytest.raises(conn.exceptions.ClientError) as exc_info:
            conn.put_vectors(**kwargs)
        assert exc_info.value.response['Error']['Code'] == 'ValidationException'
        body = json.loads(captured['body'])
        assert 'fieldList' in body, f"response should contain fieldList"
        actual_paths = [entry['path'] for entry in body['fieldList']]
        assert actual_paths == expected_paths, \
            f"expected fieldList paths {expected_paths} but got {actual_paths}"
    finally:
        conn.meta.events.unregister(event, capture)


def assert_query_vectors_validation_error(conn, expected_paths, **kwargs):
    """Call query_vectors expecting a ValidationException, verify the fieldList paths.
    expected_paths can be a single string or a list of strings."""
    if isinstance(expected_paths, str):
        expected_paths = [expected_paths]
    captured = {}
    def capture(**kw):
        captured['body'] = kw['http_response'].content
    event = 'after-call.s3vectors.QueryVectors'
    conn.meta.events.register(event, capture)
    try:
        with pytest.raises(conn.exceptions.ClientError) as exc_info:
            conn.query_vectors(**kwargs)
        assert exc_info.value.response['Error']['Code'] == 'ValidationException'
        body = json.loads(captured['body'])
        assert 'fieldList' in body, f"response should contain fieldList"
        actual_paths = [entry['path'] for entry in body['fieldList']]
        assert actual_paths == expected_paths, \
            f"expected fieldList paths {expected_paths} but got {actual_paths}"
    finally:
        conn.meta.events.unregister(event, capture)


def assert_create_index_validation_error(conn, expected_paths, **kwargs):
    """Call create_index expecting a ValidationException, verify the fieldList paths.
    expected_paths can be a single string or a list of strings."""
    if isinstance(expected_paths, str):
        expected_paths = [expected_paths]
    captured = {}
    def capture(**kw):
        captured['body'] = kw['http_response'].content
    event = 'after-call.s3vectors.CreateIndex'
    conn.meta.events.register(event, capture)
    try:
        with pytest.raises(conn.exceptions.ClientError) as exc_info:
            conn.create_index(**kwargs)
        assert exc_info.value.response['Error']['Code'] == 'ValidationException'
        body = json.loads(captured['body'])
        assert 'fieldList' in body, f"response should contain fieldList"
        actual_paths = [entry['path'] for entry in body['fieldList']]
        assert actual_paths == expected_paths, \
            f"expected fieldList paths {expected_paths} but got {actual_paths}"
    finally:
        conn.meta.events.unregister(event, capture)


def generate_data(dimension, index=0):
  return {'float32': [random.gauss(float(index), 1.0) for _ in range(dimension)]}


def generate_vectors(num_vectors, dimension, with_metadata=False):
    vectors = []
    for i in range(num_vectors):
        v = {
            'key': 'vec-' + str(i),
            'data': generate_data(dimension, i)
        }
        if with_metadata:
            v['metadata'] = json.dumps({'genre': f'genre-{i}', 'year': 2000 + i})
        vectors.append(v)
    return vectors


def verify_get_vectors(conn, bucket_name, index_name, vector_ids, expected_dimension=None, allow_extra=False):
    return_data = expected_dimension is not None
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=vector_ids, returnData=return_data)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    assert 'vectors' in result
    returned_vectors = result['vectors']
    num_expected = len(vector_ids)
    if allow_extra:
        assert len(returned_vectors) >= num_expected, \
            f"expected at least {num_expected} vectors but got {len(returned_vectors)}"
    else:
        assert len(returned_vectors) == num_expected, \
            f"expected {num_expected} vectors but got {len(returned_vectors)}"

    for vector in returned_vectors:
        assert 'key' in vector, "vector should have a 'key' field"
        assert vector['key'] in vector_ids, f"unexpected vector key: {vector['key']}"

        if return_data:
            # verify data is present
            assert 'data' in vector, \
                f"vector {vector['key']} should have 'data' field when returnData=True"
            assert 'float32' in vector['data'], \
                f"vector {vector['key']} data should have 'float32' field"

            actual_dimension = len(vector['data']['float32'])
            assert actual_dimension == expected_dimension, \
                f"vector {vector['key']} has dimension {actual_dimension}, expected {expected_dimension}"
        else:
            # verify data is NOT present
            assert 'data' not in vector, \
                f"vector {vector['key']} should not have 'data' field when returnData=False"

    if not allow_extra:
      # verify all requested keys are present
      returned_keys = [v['key'] for v in returned_vectors]
      assert set(returned_keys) == set(vector_ids), \
          f"returned keys don't match requested keys. got {set(returned_keys)}, expected {set(vector_ids)}"

    log.info('get_vectors verification completed: %d vectors with returnData=%s',
             len(returned_vectors), return_data)

    return returned_vectors


def verify_list_vectors_pagination(conn, bucket_name, index_name, expected_vectors, max_results,
                                   expected_dimension=None):
    return_data = expected_dimension is not None
    total_vectors = len(expected_vectors)
    all_retrieved_vectors = []
    next_token = None
    page_count = 0
    expected_pages = (total_vectors + max_results - 1) // max_results  # ceiling division

    while True:
        page_count += 1
        if next_token:
            result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                      maxResults=max_results, nextToken=next_token, returnData=return_data)
        else:
            result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                      maxResults=max_results, returnData=return_data)

        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        page_vectors = result.get('vectors', [])

        for vector in page_vectors:
            assert 'key' in vector, "vector should have a 'key' field"

            if return_data:
                # verify data is present
                assert 'data' in vector, \
                    f"vector {vector['key']} should have 'data' field when returnData=True"
                assert 'float32' in vector['data'], \
                    f"vector {vector['key']} data should have 'float32' field"

                actual_dimension = len(vector['data']['float32'])
                assert actual_dimension == expected_dimension, \
                    f"vector {vector['key']} has dimension {actual_dimension}, expected {expected_dimension}"
            else:
                # verify data is NOT present
                assert 'data' not in vector, \
                    f"vector {vector['key']} should not have 'data' field when returnData=False"
        log.info('page %d returned %d vectors', page_count, len(page_vectors))
        all_retrieved_vectors.extend(page_vectors)

        if 'nextToken' in result and result['nextToken']:
            # if there's a next page, this page should have max_results items
            assert len(page_vectors) == max_results, \
                f"page {page_count} should have {max_results} vectors but has {len(page_vectors)}"
            next_token = result['nextToken']
        else:
            # last page - should have the remainder (or max_results if exact multiple)
            expected_last_page_size = total_vectors % max_results
            if expected_last_page_size == 0:
                expected_last_page_size = max_results
            assert len(page_vectors) == expected_last_page_size, \
                f"last page should have {expected_last_page_size} vectors but has {len(page_vectors)}"
            break

    # verify we got the expected number of pages
    assert page_count == expected_pages, \
        f"expected {expected_pages} pages but got {page_count}"

    assert len(all_retrieved_vectors) == len(expected_vectors)
    expected_by_key = {v['key']: v for v in expected_vectors}
    for retrieved in all_retrieved_vectors:
        assert retrieved['key'] in expected_by_key, f"unexpected key: {retrieved['key']}"
        if return_data:
            expected = expected_by_key[retrieved['key']]
            actual_data = retrieved['data']['float32']
            expected_data = expected['data']['float32']
            assert len(actual_data) == len(expected_data), \
                f"key {retrieved['key']}: got {len(actual_data)} values, " \
                f"expected {len(expected_data)}"
            # the vectors are stored as float32, so the values that come back are
            # not the exact ones that were sent
            mismatches = [(i, a, b, abs(a - b))
                          for i, (a, b) in enumerate(zip(actual_data, expected_data))
                          if abs(a - b) >= 1e-4]
            assert not mismatches, \
                f"returned data don't match expected data for key {retrieved['key']}: " + \
                ", ".join(f"[{i}] got {a!r} expected {b!r} (diff {d})"
                          for i, a, b, d in mismatches[:5]) + \
                (f" and {len(mismatches) - 5} more" if len(mismatches) > 5 else "")

    log.info('pagination verification completed: %d vectors across %d pages',
             len(all_retrieved_vectors), page_count)

    return all_retrieved_vectors, page_count


@pytest.mark.vector_test
def test_put_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    vectors = generate_vectors(10, 128)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)


def update_vectors_thread(conn, bucket_name, index_name, dimension, allow_extra=False):
    num_vectors = 10
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    time.sleep(0.5)
    num_vectors = 20
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    time.sleep(5)
    num_vectors = 30
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # Get vectors with returnData=True
    vector_ids = [f'vec-{i}' for i in range(num_vectors)]
    returned_vectors = verify_get_vectors(conn, bucket_name, index_name, vector_ids, expected_dimension=dimension, allow_extra=allow_extra)
    log.info('test_get_vectors: successfully verified %d vectors', len(returned_vectors))


@pytest.mark.vector_test
def test_update_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    num_indexes = 10
    dimension = 128
    threads = []
    for i in range(num_indexes):
        index_name = 'test-index-'+str(i)
        result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=dimension, distanceMetric='euclidean')
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        t = threading.Thread(target=update_vectors_thread, args=(conn, bucket_name, index_name, dimension))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)


@pytest.mark.vector_test
def test_update_vectors_single_index():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    num_threads = 10
    dimension = 128
    threads = []
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    for i in range(num_threads):
        t = threading.Thread(target=update_vectors_thread, args=(conn, bucket_name, index_name, dimension, True))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_dimension_mismatch():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    dimension = 128
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # generate vectors with wrong dimension
    wrong_dimension = 64
    vectors = generate_vectors(10, wrong_dimension)
    # all vectors have wrong dimension, bail on first error
    assert_put_vectors_validation_error(conn,
        'vectors[0].data',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    # verify no vectors were inserted
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result.get('vectors', [])) == 0
    # mix of correct and wrong dimension vectors - bail on first wrong one
    correct_vectors = generate_vectors(5, dimension)
    wrong_vectors = generate_vectors(5, wrong_dimension)
    for i, v in enumerate(wrong_vectors):
        v['key'] = f'wrong-{i}'
    mixed_vectors = correct_vectors + wrong_vectors
    assert_put_vectors_validation_error(conn,
        'vectors[5].data',
        vectorBucketName=bucket_name, indexName=index_name, vectors=mixed_vectors)
    # verify no vectors were inserted (all-or-nothing)
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result.get('vectors', [])) == 0
    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_get_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 128
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    num_vectors = 10
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # Get vectors with returnData=True
    vector_ids = [f'vec-{i}' for i in range(num_vectors)]
    returned_vectors = verify_get_vectors(conn, bucket_name, index_name, vector_ids, expected_dimension=dimension)

    log.info('test_get_vectors: successfully verified %d vectors with data', len(returned_vectors))

    # keys that do not exist in the index are omitted from the response
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                              keys=['vec-0', 'no-such-key'])
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert [v['key'] for v in result['vectors']] == ['vec-0']

    # a request in which no key matches is still a success, with an empty list
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                              keys=['no-such-key', 'no-such-key-either'])
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert result['vectors'] == []

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_get_vectors_without_data():
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 128
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    num_vectors = 10
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vector_ids = [f'vec-{i}' for i in range(num_vectors)]
    returned_vectors = verify_get_vectors(conn, bucket_name, index_name, vector_ids)

    log.info('test_get_vectors_without_data: successfully verified %d vectors without data',
             len(returned_vectors))

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_list_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    dimension = 8
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    total_vectors = 5
    max_results = 100
    vectors = generate_vectors(total_vectors, dimension)

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    all_retrieved_vectors, page_count = verify_list_vectors_pagination(
        conn, bucket_name, index_name, vectors, max_results)

    assert page_count == 1, f"expected 1 pages but got {page_count}"
    log.info('test_list_vectors: successfully verified %d vectors across %d pages',
             len(all_retrieved_vectors), page_count)

    # segmented listing is not implemented: a single segment lists the entire index
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name,
                               maxResults=max_results, segmentCount=1, segmentIndex=0)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == total_vectors

    # and any other segment is rejected
    for segment_count, segment_index in ((2, 0), (2, 1), (16, 15)):
        with pytest.raises(conn.exceptions.ClientError) as exc_info:
            conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name,
                              maxResults=max_results,
                              segmentCount=segment_count, segmentIndex=segment_index)
        assert exc_info.value.response['ResponseMetadata']['HTTPStatusCode'] == 400

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_list_vectors_with_data():
    """Test list_vectors with returnData=True to verify data is returned."""
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    dimension = 128
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    num_vectors = 15
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    all_retrieved_vectors, page_count = verify_list_vectors_pagination(
        conn, bucket_name, index_name, vectors, max_results=100, expected_dimension=dimension)

    assert page_count == 1, f"expected 1 page but got {page_count}"
    log.info('test_list_vectors_with_data: successfully verified %d vectors with data',
             len(all_retrieved_vectors))

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_list_vectors_without_data():
    """Test list_vectors with returnData=False to verify data is not returned."""
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    dimension = 128
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    num_vectors = 15
    vectors = generate_vectors(num_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    all_retrieved_vectors, page_count = verify_list_vectors_pagination(
        conn, bucket_name, index_name, vectors, max_results=100)

    assert page_count == 1, f"expected 1 page but got {page_count}"
    log.info('test_list_vectors_without_data: successfully verified %d vectors without data',
             len(all_retrieved_vectors))

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_list_vectors_pagination():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    dimension = 128
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # 27 vectors with page size 10 = 3 pages (10, 10, 7)
    total_vectors = 27
    max_results = 10
    vectors = generate_vectors(total_vectors, dimension)

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    all_retrieved_vectors, page_count = verify_list_vectors_pagination(
        conn, bucket_name, index_name, vectors, max_results, expected_dimension=dimension)

    assert page_count == 3, f"expected 3 pages but got {page_count}"
    log.info('test_list_vectors: successfully verified %d vectors across %d pages',
             len(all_retrieved_vectors), page_count)

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_list_vectors_exact_pagination():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    dimension = 128
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # 30 vectors with page size 10 = 3 pages (10, 10, 10) - exact fit
    total_vectors = 30
    max_results = 10
    vectors = generate_vectors(total_vectors, dimension)

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    all_retrieved_vectors, page_count = verify_list_vectors_pagination(
        conn, bucket_name, index_name, vectors, max_results, expected_dimension=dimension)

    assert page_count == 3, f"expected 3 pages but got {page_count}"
    log.info('test_list_vectors_exact_pagination: successfully verified %d vectors across %d pages',
             len(all_retrieved_vectors), page_count)

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_delete_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    dimension = 128
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    total_vectors = 20
    vectors = generate_vectors(total_vectors, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # known vectors to delete: vec-2, vec-5, vec-7, vec-10, vec-12, vec-15, vec-17, vec-19
    # unknown vectors: vec-100, vec-999, nonexistent-key
    vectors_to_delete = ['vec-2', 'vec-5', 'vec-7', 'vec-10', 'vec-12', 'vec-15', 'vec-17', 'vec-19',
                         'vec-100', 'vec-999', 'nonexistent-key']
    result = conn.delete_vectors(vectorBucketName=bucket_name, indexName=index_name, keys=vectors_to_delete)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # list all vectors to verify deletion
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name,
                              maxResults=100, returnData=False)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    remaining_vectors = result.get('vectors', [])
    remaining_keys = [v['key'] for v in remaining_vectors]
    expected_remaining_keys = [f'vec-{i}' for i in range(total_vectors) if f'vec-{i}' not in vectors_to_delete]
    assert set(remaining_keys) == set(expected_remaining_keys), \
        f"remaining vector keys don't match expected. got {set(remaining_keys)}, expected {set(expected_remaining_keys)}"

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors():
    dimension = 8
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    vectors = generate_vectors(100, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    top_k = 5
    for expected_index in [8, 17, 42, 99]:
        query_vector = generate_data(dimension, expected_index)
        result = conn.query_vectors(vectorBucketName=bucket_name, indexName=index_name, queryVector=query_vector, topK=top_k)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        expected_key = 'vec-'+str(expected_index)
        assert expected_key in [v['key'] for v in result['vectors']]
        assert 'distance' not in [v for v in result['vectors']]
        assert len(result['vectors']) == top_k
        assert result['distanceMetric'] == 'euclidean'
        log.info(result['vectors'])

    # topK must be between 1 and 10000. note that a topK below 1 is rejected by the
    # client itself, so only the upper limit can be verified here
    query_vector = generate_data(dimension, 0)
    query_args = dict(vectorBucketName=bucket_name, indexName=index_name, queryVector=query_vector)
    result = conn.query_vectors(topK=10000, **query_args)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    with pytest.raises(conn.exceptions.ClientError) as exc_info:
        conn.query_vectors(topK=10001, **query_args)
    assert exc_info.value.response['ResponseMetadata']['HTTPStatusCode'] == 400

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_with_distance():
    dimension = 8
    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    vectors = generate_vectors(100, dimension)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    top_k = 5
    for expected_index in [8, 17, 42, 99]:
        query_vector = generate_data(dimension, expected_index)
        result = conn.query_vectors(vectorBucketName=bucket_name, indexName=index_name, queryVector=query_vector, topK=top_k, returnDistance=True)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        expected_key = 'vec-'+str(expected_index)
        assert expected_key in [v['key'] for v in result['vectors']]
        for v in result['vectors']:
          assert 'distance' in v
        assert len(result['vectors']) == top_k
        assert result['distanceMetric'] == 'euclidean'
        log.info(result['vectors'])

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_and_get_vectors_metadata():
    """Test storing and retrieving a mix of vectors with and without metadata."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # create a mix: some vectors with metadata, some without
    vectors_with_md = generate_vectors(3, dimension, with_metadata=True)
    vectors_without_md = generate_vectors(3, dimension, with_metadata=False)
    # rename keys to avoid collisions
    for i, v in enumerate(vectors_without_md):
        v['key'] = f'no-md-{i}'
    all_vectors = vectors_with_md + vectors_without_md
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=all_vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    all_keys = [v['key'] for v in all_vectors]

    # get all vectors with returnMetadata=True
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=all_keys, returnMetadata=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == len(all_vectors)
    for vector in result['vectors']:
        if vector['key'].startswith('vec-'):
            assert 'metadata' in vector, f"vector {vector['key']} should have 'metadata' field"
            md = json.loads(vector['metadata'])
            assert 'genre' in md, f"metadata should have 'genre' key: {vector['metadata']}"
            assert 'year' in md, f"metadata should have 'year' key: {vector['metadata']}"
        else:
            assert 'metadata' not in vector, \
                f"vector {vector['key']} should not have metadata when none was stored"

    # get all vectors with returnMetadata=False - no metadata should be returned
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=all_keys, returnMetadata=False)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    for vector in result['vectors']:
        assert 'metadata' not in vector, \
            f"vector {vector['key']} should not have 'metadata' field when returnMetadata=False"

    # get with both returnData and returnMetadata
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=all_keys, returnData=True, returnMetadata=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    for vector in result['vectors']:
        assert 'data' in vector, f"vector {vector['key']} should have 'data' field"
        assert 'float32' in vector['data']
        assert len(vector['data']['float32']) == dimension

    log.info('test_put_and_get_vectors_metadata: verified metadata for %d vectors', len(all_vectors))

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_list_vectors_with_metadata():
    """Test that list_vectors returns metadata when returnMetadata=True."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    num_vectors = 5
    vectors = generate_vectors(num_vectors, dimension, with_metadata=True)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # list with returnMetadata=True
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name,
                               maxResults=100, returnMetadata=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == num_vectors
    for vector in result['vectors']:
        assert 'metadata' in vector, f"vector {vector['key']} should have 'metadata' field"
        md = json.loads(vector['metadata'])
        assert 'genre' in md, f"metadata should have 'genre' key: {vector['metadata']}"
        assert 'year' in md, f"metadata should have 'year' key: {vector['metadata']}"

    # list with returnMetadata=False
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name,
                               maxResults=100, returnMetadata=False)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    for vector in result['vectors']:
        assert 'metadata' not in vector, \
            f"vector {vector['key']} should not have 'metadata' field when returnMetadata=False"

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_with_metadata():
    """Test that query_vectors returns metadata when returnMetadata=True."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    num_vectors = 20
    vectors = generate_vectors(num_vectors, dimension, with_metadata=True)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    top_k = 5
    query_vector = generate_data(dimension, 7)

    # query with returnMetadata=True
    result = conn.query_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                queryVector=query_vector, topK=top_k, returnMetadata=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == top_k
    for vector in result['vectors']:
        assert 'metadata' in vector, f"vector {vector['key']} should have 'metadata' field"
        md = json.loads(vector['metadata'])
        assert 'genre' in md, f"metadata should have 'genre' key: {vector['metadata']}"
        assert 'year' in md, f"metadata should have 'year' key: {vector['metadata']}"

    # query with returnMetadata=False
    result = conn.query_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                queryVector=query_vector, topK=top_k, returnMetadata=False)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    for vector in result['vectors']:
        assert 'metadata' not in vector, \
            f"vector {vector['key']} should not have 'metadata' field when returnMetadata=False"

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_malformed_metadata():
    """Test that vectors with malformed JSON metadata are skipped."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # all vectors have malformed metadata - bail on first error
    bad_vectors = [
        {
            'key': f'bad-{i}',
            'data': generate_data(dimension, i),
            'metadata': '{"info": {"value": "missing end quote}}'
        }
        for i in range(3)
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata',
        vectorBucketName=bucket_name, indexName=index_name, vectors=bad_vectors)

    # verify no vectors were inserted
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result.get('vectors', [])) == 0

    # mix of good and bad metadata - bail on first bad one
    good_vectors = [
        {
            'key': f'good-{i}',
            'data': generate_data(dimension, i),
            'metadata': json.dumps({'info': f'value-{i}'})
        }
        for i in range(3)
    ]
    mixed_vectors = good_vectors + bad_vectors
    assert_put_vectors_validation_error(conn,
        'vectors[3].metadata',
        vectorBucketName=bucket_name, indexName=index_name, vectors=mixed_vectors)

    # verify no vectors were inserted (all-or-nothing)
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result.get('vectors', [])) == 0

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_null_metadata_value():
    """Test that vectors with null metadata values are rejected."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # null value in metadata field
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': '{"color": null}'},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata.color',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # null among valid fields
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': '{"genre": "rock", "year": null}'},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata.year',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # valid vector followed by null vector - all-or-nothing
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'color': 'red'})},
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': '{"color": null}'},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[1].metadata.color',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # verify no vectors were inserted
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result.get('vectors', [])) == 0

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_dots_in_metadata_field_names():
    """Test that vectors with dots in metadata field names are rejected."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'user.name': 'alice'})},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata.user.name',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_duplicate_metadata_field_names():
    """Test that vectors holding the same metadata field more than once are rejected."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean',
                               metadataConfiguration={'filterableMetadataKeys': [{'name': 'genre'}]})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # note that the metadata is not built with a JSON encoder here, since it would
    # keep only one of the values of a duplicated field
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': '{"color": "red", "color": "blue"}'},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata.color',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # the same, for a filterable field
    vectors = [
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': '{"genre": "rock", "genre": "jazz"}'},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata.genre',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # none of the vectors was added
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result.get('vectors', [])) == 0

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_metadata_limits():
    """Test the limits on the number of metadata fields and on the metadata size."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # 50 metadata fields are allowed
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({f'key-{i}': i for i in range(50)})},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # more than 50 metadata fields are rejected
    vectors = [
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': json.dumps({f'key-{i}': i for i in range(51)})},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # nested fields are not counted, only the top level ones
    nested = {f'key-{i}': i for i in range(49)}
    nested['info'] = {f'nested-{i}': i for i in range(50)}
    vectors = [
        {'key': 'v2', 'data': generate_data(dimension, 2), 'metadata': json.dumps(nested)},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # metadata of more than 40KB is rejected
    vectors = [
        {'key': 'v3', 'data': generate_data(dimension, 3),
         'metadata': json.dumps({'big': 'x'*(40*1024)})},
    ]
    assert_put_vectors_validation_error(conn,
        'vectors[0].metadata',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)

    # cleanup
    _ = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_missing_filterable_fields():
    """Test that vectors with missing filterable metadata fields are inserted with nulls."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    filterable_keys = [
        {'name': 'genre', 'type': 'String'},
        {'name': 'year', 'type': 'Number'},
        {'name': 'popular', 'type': 'Boolean'}
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {
            'key': 'all-fields',
            'data': generate_data(dimension, 0),
            'metadata': json.dumps({'genre': 'rock', 'year': 2000, 'popular': True})
        },
        {
            'key': 'some-fields',
            'data': generate_data(dimension, 1),
            'metadata': json.dumps({'genre': 'jazz'})
        },
        {
            'key': 'no-filterable-fields',
            'data': generate_data(dimension, 2),
            'metadata': json.dumps({'artist': 'someone'})
        },
        {
            'key': 'no-metadata',
            'data': generate_data(dimension, 3),
        },
        {
            'key': 'nested-field',
            'data': generate_data(dimension, 4),
            'metadata': json.dumps({'info': {'genre': 'blues', 'year': 1990}})
        },
    ]

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    all_keys = [v['key'] for v in vectors]
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=all_keys, returnMetadata=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 5

    by_key = {v['key']: v for v in result['vectors']}

    md = json.loads(by_key['all-fields']['metadata'])
    assert md['genre'] == 'rock'
    assert md['year'] == 2000
    assert md['popular'] is True

    md = json.loads(by_key['some-fields']['metadata'])
    assert md['genre'] == 'jazz'
    assert 'year' not in md
    assert 'popular' not in md

    md = json.loads(by_key['no-filterable-fields']['metadata'])
    assert 'genre' not in md
    assert md['artist'] == 'someone'
    assert 'year' not in md
    assert 'popular' not in md

    assert 'metadata' not in by_key['no-metadata']

    # nested fields with filterable key names should not be found at top level
    md = json.loads(by_key['nested-field']['metadata'])
    assert 'genre' not in md
    assert 'year' not in md
    assert 'popular' not in md
    assert md['info']['genre'] == 'blues'
    assert md['info']['year'] == 1990

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_invalid_filterable_types():
    """Test that vectors with wrong types for filterable fields fail with ValidationException.
    Each invalid type mismatch is tested individually since PutVectors is all-or-nothing."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    filterable_keys = [
        {'name': 'genre'},
        {'name': 'year', 'type': 'Number'},
        {'name': 'popular', 'type': 'Boolean'},
        {'name': 'tags', 'type': 'StringList'},
        {'name': 'scores', 'type': 'NumberList'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # correct types should succeed
    correct_vector = [{
        'key': 'correct-types',
        'data': generate_data(dimension, 0),
        'metadata': json.dumps({
            'genre': 'rock', 'year': 2000, 'popular': True,
            'tags': ['a', 'b'], 'scores': [1.0, 2.0]
        })
    }]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=correct_vector)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # string for number field
    assert_put_vectors_validation_error(conn, 'vectors[0].metadata.year',
        vectorBucketName=bucket_name, indexName=index_name, vectors=[{
            'key': 'string-for-number',
            'data': generate_data(dimension, 1),
            'metadata': json.dumps({'year': 'not-a-number'})
        }])

    # number for string field - should succeed (number is coerced to string)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=[{
        'key': 'number-for-string',
        'data': generate_data(dimension, 2),
        'metadata': json.dumps({'genre': 12345})
    }])
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # string for boolean field
    assert_put_vectors_validation_error(conn, 'vectors[0].metadata.popular',
        vectorBucketName=bucket_name, indexName=index_name, vectors=[{
            'key': 'string-for-boolean',
            'data': generate_data(dimension, 3),
            'metadata': json.dumps({'popular': 'yes'})
        }])

    # list for scalar field
    assert_put_vectors_validation_error(conn, 'vectors[0].metadata.genre',
        vectorBucketName=bucket_name, indexName=index_name, vectors=[{
            'key': 'list-for-scalar',
            'data': generate_data(dimension, 4),
            'metadata': json.dumps({'genre': ['rock', 'pop']})
        }])

    # scalar for list field
    assert_put_vectors_validation_error(conn, 'vectors[0].metadata.tags',
        vectorBucketName=bucket_name, indexName=index_name, vectors=[{
            'key': 'scalar-for-list',
            'data': generate_data(dimension, 5),
            'metadata': json.dumps({'tags': 'single-tag'})
        }])

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_put_vectors_must_exist():
    """Test the mustExist flag on filterable metadata keys."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    filterable_keys = [
        {'name': 'genre', 'mustExist': True},
        {'name': 'year', 'type': 'Number', 'mustExist': True},
        {'name': 'popular', 'type': 'Boolean'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # all required fields present - should succeed
    vectors = [
        {
            'key': f'v{i}',
            'data': generate_data(dimension, i),
            'metadata': json.dumps({'genre': f'genre-{i}', 'year': 2000 + i})
        }
        for i in range(3)
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # verify vectors were inserted
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 3

    # missing required field - should fail
    vectors_with_missing = [
        {
            'key': 'ok-row',
            'data': generate_data(dimension, 10),
            'metadata': json.dumps({'genre': 'rock', 'year': 2020})
        },
        {
            'key': 'ok-row-2',
            'data': generate_data(dimension, 11),
            'metadata': json.dumps({'genre': 'jazz', 'year': 2021})
        },
        {
            'key': 'bad-row-nested-ok',
            'data': generate_data(dimension, 12),
            'metadata': json.dumps({'popular': True, 'info': {'genre': 'blues', 'year': 1990}})
        },
    ]
    assert_put_vectors_validation_error(conn, 'vectors[2].metadata.genre',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors_with_missing)

    # same scenario but with mustExist=false - should succeed
    _ = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)

    filterable_keys_nullable = [
        {'name': 'genre', 'mustExist': False},
        {'name': 'year', 'type': 'Number', 'mustExist': False},
        {'name': 'popular', 'type': 'Boolean'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys_nullable})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors_with_missing)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # verify all vectors were inserted
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 3

    # vector without metadata and mustExist=true - should fail
    _ = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)

    filterable_keys_not_null = [
        {'name': 'genre', 'mustExist': True},
        {'name': 'popular', 'type': 'Boolean'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys_not_null})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors_no_metadata = [
        {
            'key': 'with-metadata',
            'data': generate_data(dimension, 20),
            'metadata': json.dumps({'genre': 'rock'})
        },
        {
            'key': 'no-metadata',
            'data': generate_data(dimension, 21),
        },
    ]
    assert_put_vectors_validation_error(conn, 'vectors[1].metadata.genre',
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors_no_metadata)

    # same but with mustExist=false - should succeed
    _ = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)

    filterable_keys_all_null = [
        {'name': 'genre', 'mustExist': False},
        {'name': 'popular', 'type': 'Boolean'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys_all_null})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors_no_metadata)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 2

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_filter():
    """Test metadata filtering during vector queries."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    filterable_keys = [
        {'name': 'genre'},
        {'name': 'year', 'type': 'Number'},
        {'name': 'popular', 'type': 'Boolean'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'genre': 'rock', 'year': 2020, 'popular': True, 'color': 'red'})},
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': json.dumps({'genre': 'jazz', 'year': 2019, 'popular': False, 'color': 'blue'})},
        {'key': 'v2', 'data': generate_data(dimension, 2),
         'metadata': json.dumps({'genre': 'rock', 'year': 2018, 'popular': True, 'color': 'red'})},
        {'key': 'v3', 'data': generate_data(dimension, 3),
         'metadata': json.dumps({'genre': 'pop', 'year': 2021, 'popular': False, 'color': 'green'})},
        {'key': 'v4', 'data': generate_data(dimension, 4),
         'metadata': json.dumps({'genre': 'jazz', 'year': 2020, 'popular': True, 'color': 'red'})},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    query_vector = generate_data(dimension, 0)
    top_k = 10

    def query_keys(filter_expr):
        result = conn.query_vectors(
            vectorBucketName=bucket_name, indexName=index_name,
            queryVector=query_vector, topK=top_k, filter=filter_expr)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        return sorted([v['key'] for v in result['vectors']])

    # implicit equality
    assert query_keys({'genre': 'rock'}) == ['v0', 'v2']

    # explicit $eq
    assert query_keys({'genre': {'$eq': 'rock'}}) == ['v0', 'v2']

    # $ne
    assert query_keys({'genre': {'$ne': 'rock'}}) == ['v1', 'v3', 'v4']

    # $gt
    assert query_keys({'year': {'$gt': 2019}}) == ['v0', 'v3', 'v4']

    # range: $gte + $lte
    assert query_keys({'year': {'$gte': 2019, '$lte': 2020}}) == ['v0', 'v1', 'v4']

    # $in
    assert query_keys({'genre': {'$in': ['rock', 'jazz']}}) == ['v0', 'v1', 'v2', 'v4']

    # $nin
    assert query_keys({'genre': {'$nin': ['rock']}}) == ['v1', 'v3', 'v4']

    # $exists
    assert query_keys({'genre': {'$exists': True}}) == ['v0', 'v1', 'v2', 'v3', 'v4']

    # boolean filter
    assert query_keys({'popular': True}) == ['v0', 'v2', 'v4']

    # $and
    assert query_keys({'$and': [{'genre': 'rock'}, {'year': {'$gt': 2019}}]}) == ['v0']

    # $or
    assert query_keys({'$or': [{'genre': 'rock'}, {'genre': 'jazz'}]}) == ['v0', 'v1', 'v2', 'v4']

    # implicit AND (multiple top-level fields)
    assert query_keys({'genre': 'jazz', 'popular': True}) == ['v4']

    # mixed $and: column filter (genre) + JSON metadata filter (color)
    assert query_keys({'$and': [{'genre': 'rock'}, {'color': 'red'}]}) == ['v0', 'v2']
    assert query_keys({'$and': [{'genre': 'jazz'}, {'color': 'red'}]}) == ['v4']

    # implicit AND with mixed column + JSON fields
    assert query_keys({'genre': 'rock', 'color': 'red'}) == ['v0', 'v2']

    # nested $and: both inner $ands mix column and JSON fields
    assert query_keys({'$and': [
        {'$and': [{'genre': 'rock'}, {'color': 'red'}]},
        {'$and': [{'year': {'$gt': 2019}}, {'color': 'red'}]}
    ]}) == ['v0']

    # nested $or inside $and: each $or is homogeneous (column-only or JSON-only)
    assert query_keys({'$and': [
        {'$or': [{'genre': 'rock'}, {'genre': 'jazz'}]},
        {'color': 'red'}
    ]}) == ['v0', 'v2', 'v4']

    # mixed $or: column + JSON fields should be rejected
    assert_query_vectors_validation_error(
        conn, 'filter',
        vectorBucketName=bucket_name, indexName=index_name,
        queryVector=query_vector, topK=top_k,
        filter={'$or': [{'genre': 'rock'}, {'color': 'red'}]})

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_post_filtering():
    """Test that postFiltering forces all filtering through JSON post-filtering."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    filterable_keys = [
        {'name': 'genre'},
        {'name': 'year', 'type': 'Number'},
        {'name': 'popular', 'type': 'Boolean'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # v0 (index=0) and v3 (index=10) are rock; v1 and v2 are jazz.
    # query is centered at index 0, so v0 is nearest and v3 is far away.
    # with topK=2: pre-filtering on genre=rock searches only rock vectors
    # and returns both (v0, v3). post-filtering fetches the 2 nearest
    # overall (v0, v1), then filters to rock, returning only v0.
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'genre': 'rock', 'year': 2020, 'popular': True, 'color': 'red'})},
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': json.dumps({'genre': 'jazz', 'year': 2019, 'popular': False, 'color': 'blue'})},
        {'key': 'v2', 'data': generate_data(dimension, 2),
         'metadata': json.dumps({'genre': 'jazz', 'year': 2018, 'popular': True, 'color': 'green'})},
        {'key': 'v3', 'data': generate_data(dimension, 10),
         'metadata': json.dumps({'genre': 'rock', 'year': 2021, 'popular': False, 'color': 'red'})},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    query_vector = generate_data(dimension, 0)

    def query_keys(filter_expr, top_k, post_filtering=False):
        kwargs = dict(vectorBucketName=bucket_name, indexName=index_name,
                      queryVector=query_vector, topK=top_k, filter=filter_expr)
        if post_filtering:
            kwargs['postFiltering'] = True
        result = conn.query_vectors(**kwargs)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        return sorted([v['key'] for v in result['vectors']])

    # pre-filtering on genre=rock with topK=2 returns both rock vectors
    assert query_keys({'genre': 'rock'}, top_k=2) == ['v0', 'v3']
    # post-filtering with topK=3 only sees the 3 nearest (v0, v1, v2), so v3 is excluded
    assert query_keys({'genre': 'rock'}, top_k=3, post_filtering=True) == ['v0']

    # post-filtering allows mixed $or (column + JSON fields)
    assert query_keys({'$or': [{'genre': 'rock'}, {'color': 'blue'}]}, top_k=10, post_filtering=True) == ['v0', 'v1', 'v3']

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_filter_nonfilterable():
    """Test that filtering on non-filterable keys is rejected."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'nonFilterableMetadataKeys': ['secret']})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'secret': 'hidden'})},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    query_vector = generate_data(dimension, 0)
    with pytest.raises(conn.exceptions.ClientError) as exc_info:
        conn.query_vectors(
            vectorBucketName=bucket_name, indexName=index_name,
            queryVector=query_vector, topK=5, filter={'secret': 'hidden'})
    assert exc_info.value.response['ResponseMetadata']['HTTPStatusCode'] == 400

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_filter_json_metadata():
    """Test filtering on undeclared metadata fields using JSON extraction."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'color': 'red', 'priority': 10, 'active': True})},
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': json.dumps({'color': 'blue', 'priority': 3, 'active': False})},
        {'key': 'v2', 'data': generate_data(dimension, 2),
         'metadata': json.dumps({'color': 'red', 'priority': 7, 'active': True})},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    query_vector = generate_data(dimension, 0)
    top_k = 10

    def query_keys(filter_expr):
        result = conn.query_vectors(
            vectorBucketName=bucket_name, indexName=index_name,
            queryVector=query_vector, topK=top_k, filter=filter_expr)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        return sorted([v['key'] for v in result['vectors']])

    # string field
    assert query_keys({'color': 'red'}) == ['v0', 'v2']

    # number field comparison
    assert query_keys({'priority': {'$gt': 5}}) == ['v0', 'v2']

    # boolean field
    assert query_keys({'active': True}) == ['v0', 'v2']

    # the JSON type of the value in the filter decides how the metadata field is
    # read, so a value of the wrong type never matches
    assert query_keys({'priority': '10'}) == []
    assert query_keys({'color': {'$eq': 42}}) == []
    assert query_keys({'active': {'$eq': 'true'}}) == []

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_missing_json_metadata():
    """Test filtering on JSON metadata fields that some of the vectors do not have."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # v2 has metadata without a "color" field, and v3 has no metadata at all
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'color': 'red'})},
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': json.dumps({'color': 'blue'})},
        {'key': 'v2', 'data': generate_data(dimension, 2),
         'metadata': json.dumps({'shape': 'round'})},
        {'key': 'v3', 'data': generate_data(dimension, 3)},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    query_vector = generate_data(dimension, 0)
    top_k = 10

    def query_keys(filter_expr):
        result = conn.query_vectors(
            vectorBucketName=bucket_name, indexName=index_name,
            queryVector=query_vector, topK=top_k, filter=filter_expr)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        return sorted([v['key'] for v in result['vectors']])

    assert query_keys({'color': 'red'}) == ['v0']

    # a condition on a field that the vector does not have never matches,
    # including the negative operators
    assert query_keys({'color': {'$ne': 'red'}}) == ['v1']
    assert query_keys({'color': {'$nin': ['red']}}) == ['v1']

    # $exists tells the vectors without the field apart. a vector with no
    # metadata at all has no fields, so it matches $exists=false as well
    assert query_keys({'color': {'$exists': True}}) == ['v0', 'v1']
    assert query_keys({'color': {'$exists': False}}) == ['v2', 'v3']

    # to include the vectors without the field in a negative condition, it
    # should be combined with $exists
    assert query_keys({'$or': [{'color': {'$ne': 'red'}},
                               {'color': {'$exists': False}}]}) == ['v1', 'v2', 'v3']

    # cleanup
    _ = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_missing_filterable_fields():
    """Test filtering on filterable metadata keys that some of the vectors do not have."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # "genre" is nullable, so it is null for vectors whose metadata did not have it
    index_name = 'test-index'
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'genre', 'type': 'String'},
            {'name': 'year', 'type': 'Number'},
        ]})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # v2 has metadata without a "genre" field, and v3 has no metadata at all
    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'genre': 'rock', 'year': 2020})},
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': json.dumps({'genre': 'jazz', 'year': 2019})},
        {'key': 'v2', 'data': generate_data(dimension, 2),
         'metadata': json.dumps({'year': 2018})},
        {'key': 'v3', 'data': generate_data(dimension, 3)},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    query_vector = generate_data(dimension, 0)
    top_k = 10

    def query_keys(filter_expr, index=index_name):
        result = conn.query_vectors(
            vectorBucketName=bucket_name, indexName=index,
            queryVector=query_vector, topK=top_k, filter=filter_expr)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        return sorted([v['key'] for v in result['vectors']])

    assert query_keys({'genre': 'rock'}) == ['v0']

    # a condition on a column that is null for the vector never matches,
    # including the negative operators
    assert query_keys({'genre': {'$ne': 'rock'}}) == ['v1']
    assert query_keys({'genre': {'$nin': ['rock']}}) == ['v1']

    # $exists on a nullable column tells the vectors without the field apart
    assert query_keys({'genre': {'$exists': True}}) == ['v0', 'v1']
    assert query_keys({'genre': {'$exists': False}}) == ['v2', 'v3']

    # to include the vectors without the field in a negative condition, it
    # should be combined with $exists
    assert query_keys({'$or': [{'genre': {'$ne': 'rock'}},
                               {'genre': {'$exists': False}}]}) == ['v1', 'v2', 'v3']

    # a column declared with mustExist is never null, so $exists=true matches
    # every vector of the index, and $exists=false matches none
    must_exist_index = 'test-index-must-exist'
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=must_exist_index,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'genre', 'type': 'String', 'mustExist': True},
        ]})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'genre': 'rock'})},
        {'key': 'v1', 'data': generate_data(dimension, 1),
         'metadata': json.dumps({'genre': 'jazz'})},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=must_exist_index, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    assert query_keys({'genre': {'$exists': True}}, index=must_exist_index) == ['v0', 'v1']
    assert query_keys({'genre': {'$exists': False}}, index=must_exist_index) == []

    # cleanup
    _ = conn.delete_index(vectorBucketName=bucket_name, indexName=must_exist_index)
    _ = conn.delete_index(vectorBucketName=bucket_name, indexName=index_name)
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_filter_errors():
    """Test that invalid filter expressions are rejected with 400."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    filterable_keys = [
        {'name': 'genre'},
        {'name': 'year', 'type': 'Number'},
        {'name': 'popular', 'type': 'Boolean'},
    ]
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean',
        metadataConfiguration={'filterableMetadataKeys': filterable_keys})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {'key': 'v0', 'data': generate_data(dimension, 0),
         'metadata': json.dumps({'genre': 'rock', 'year': 2020, 'popular': True})},
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    query_vector = generate_data(dimension, 0)
    query_args = dict(vectorBucketName=bucket_name, indexName=index_name,
                      queryVector=query_vector, topK=5)

    def expect_error(filter_expr):
        assert_query_vectors_validation_error(
            conn, 'filter', filter=filter_expr, **query_args)

    # unknown operator
    expect_error({'genre': {'$regex': 'r.*'}})

    # invalid boolean value for boolean column
    expect_error({'popular': {'$eq': 'yes'}})

    # invalid number value for number column
    expect_error({'year': {'$eq': 'not_a_number'}})

    # empty $in list
    expect_error({'genre': {'$in': []}})

    # mixed types in $in list (JSON field)
    expect_error({'color': {'$in': ['red', 42]}})

    # mixed $or: column + JSON fields
    expect_error({'$or': [{'genre': 'rock'}, {'color': 'red'}]})

    # nested mixed $or via $and
    expect_error({'$or': [{'genre': 'rock'}, {'$and': [{'genre': 'jazz'}, {'color': 'blue'}]}]})

    # mixed $or nested inside $and
    expect_error({'$and': [{'$or': [{'genre': 'rock'}, {'color': 'red'}]}, {'popular': True}]})

    # object value in $eq (JSON field)
    expect_error({'color': {'$eq': {'nested': 'value'}}})

    # implicit $eq with an array value (JSON field)
    expect_error({'color': ['red', 'blue']})

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)

@pytest.mark.vector_test
def test_query_vectors_post_filter_topk():
    """ Test topK oversampling with JSON post-filtering """

    set_rgw_config_option('rgw_s3vector_topk_post_filter_factor', 1.5)
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-index'
    result = conn.create_index(
        vectorBucketName=bucket_name, indexName=index_name,
        dataType='float32', dimension=dimension, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = []
    for i in range(7):
        vectors.append({
            'key': f'red-{i}',
            'data': generate_data(dimension, i),
            'metadata': json.dumps({'color': 'red'}),
        })
    for i in range(13):
        vectors.append({
            'key': f'blue-{i}',
            'data': generate_data(dimension, 100 + i),
            'metadata': json.dumps({'color': 'blue'}),
        })
    result = conn.put_vectors(
        vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    top_k = 9

    # test 1: fewer than k matches after filtering — return all matches
    # query near red vectors (index 0), so all 7 red are in the top results
    result = conn.query_vectors(
        vectorBucketName=bucket_name, indexName=index_name,
        queryVector=generate_data(dimension, 0), topK=top_k, filter={'color': 'red'})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 7

    # test 2: more than k matches after filtering — return exactly k
    # query near blue vectors (index 106), so all 13 blue are in the top results
    # do not request returnDistance
    result = conn.query_vectors(
        vectorBucketName=bucket_name, indexName=index_name,
        queryVector=generate_data(dimension, 106), topK=top_k, filter={'color': 'blue'})
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == top_k
    # verify distance is not in the response
    for v in result['vectors']:
        log.info(v)
        assert 'distance' not in v, "distance should not be in response when not requested"

    # cleanup
    _ = _delete_vector_bucket(conn, bucket_name)
    set_rgw_config_option('rgw_s3vector_topk_post_filter_factor', 1)


@pytest.mark.vector_test
def test_sal_error_propagation():
    """Verify that SAL errors propagate through LanceDB back to the S3Vector API.

    For S3/SAL backends: create a vector bucket and index, delete the backing
    S3 bucket, then attempt put_vectors. The operation should fail because the
    underlying storage is gone. This validates the error chain:
    rgw_sal_wrapper -> store.rs -> LanceDB -> lancedb-c -> rgw_s3vector.cc
    """
    if not is_s3_backend():
        pytest.skip("error propagation test only applies to S3/SAL backends")

    conn = connection()
    bucket_name = gen_bucket_name()
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'test-error-prop'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=4, distanceMetric='cosine')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # put some initial vectors to ensure the index is working
    vectors = generate_vectors(2, 4)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # delete the backing S3 bucket to trigger SAL errors
    _delete_s3_bucket_for_vector_bucket(bucket_name)

    # put_vectors should now fail since the backing store is gone
    new_vectors = generate_vectors(2, 4)
    try:
        result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=new_vectors)
        # if we get here, the operation unexpectedly succeeded
        log.warning("put_vectors succeeded after backing bucket deletion (status=%s)",
                    result['ResponseMetadata']['HTTPStatusCode'])
    except conn.exceptions.ClientError as err:
        log.info("put_vectors correctly failed after backing bucket deletion: %s", err)
        assert err.response['ResponseMetadata']['HTTPStatusCode'] >= 400

    # cleanup - recreate backing bucket so delete_vector_bucket can clean up
    _ensure_s3_bucket_for_vector_bucket(bucket_name)
    _ = _delete_vector_bucket(conn, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)


@pytest.mark.vector_bucket_test
def test_cross_owner_vector_bucket():
    """When the S3 bucket owner differs from the vector bucket owner,
    operations should fail without a bucket policy and succeed with one."""
    if not is_s3_backend():
        pytest.skip("cross-owner test only applies to S3/SAL backends")

    # user A (config user) creates the S3 bucket
    s3conn = connection('s3')
    bucket_name = gen_bucket_name()
    _create_s3bucket(s3conn, bucket_name)

    # user B creates a vector bucket with the same name
    other = another_user()
    result = other.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # without a bucket policy, user B should fail to create an index
    index_name = 'cross-owner-idx'
    try:
        other.create_index(vectorBucketName=bucket_name, indexName=index_name,
                           dataType='float32', dimension=4, distanceMetric='cosine')
        assert False, "create_index should have failed without bucket policy"
    except other.exceptions.ClientError as err:
        log.info("create_index correctly failed without bucket policy: %s", err)
        assert err.response['ResponseMetadata']['HTTPStatusCode'] >= 400

    # user A sets a bucket policy granting user B full access
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": [f"arn:aws:iam:::user/{other.uid}"]},
            "Action": "s3:*",
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ]
        }]
    })
    s3conn.put_bucket_policy(Bucket=bucket_name, Policy=policy)

    # now user B should be able to create an index and write/query vectors
    result = other.create_index(vectorBucketName=bucket_name, indexName=index_name,
                                dataType='float32', dimension=4, distanceMetric='cosine')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = generate_vectors(2, 4)
    result = other.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    result = other.query_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                 queryVector=vectors[0]['data'], topK=2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 2

    # cleanup
    _ = _delete_vector_bucket(other, bucket_name)
    _delete_s3_bucket_for_vector_bucket(bucket_name)


@pytest.mark.vector_bucket_test
def test_versioned_s3_bucket():
    """Vector bucket backed by an S3 bucket with versioning enabled."""
    if not is_s3_backend():
        pytest.skip("versioned bucket test only applies to S3/SAL backends")
    if get_s3vector_backend() == 'rgw':
        pytest.skip("versioned buckets not supported with RGW backend (null version IDs)")

    conn = connection()
    s3conn = connection('s3')
    bucket_name = gen_bucket_name()

    # create an S3 bucket and enable versioning
    _create_s3bucket(s3conn, bucket_name)
    s3conn.put_bucket_versioning(Bucket=bucket_name,
                                 VersioningConfiguration={'Status': 'Enabled'})

    # create a vector bucket, index, and write/query vectors
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    index_name = 'versioned-idx'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name,
                               dataType='float32', dimension=4, distanceMetric='cosine')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = generate_vectors(3, 4)
    # upsert the same vectors multiple times to exercise overwrites with versioning
    for i in range(3):
        result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # get vectors
    vector_keys = [v['key'] for v in vectors]
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                              keys=vector_keys, returnData=True)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 3

    # query vectors
    query_vector = generate_data(4, 0)
    result = conn.query_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                queryVector=query_vector, topK=3)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 3

    # delete vectors
    result = conn.delete_vectors(vectorBucketName=bucket_name, indexName=index_name,
                                 keys=vector_keys)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # verify they are gone
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                              keys=vector_keys)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectors']) == 0

    # cleanup - delete the indexes, and then purge all versions and delete markers.
    # the files that LanceDB removes are kept as noncurrent versions of the objects,
    # so the vector bucket would still look like it holds indexes if the versions
    # are not purged before it is deleted
    _delete_all_indexes(conn, bucket_name)
    _purge_all_versions(s3conn, bucket_name)
    result = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # deleting the vector bucket removes the LanceDB files again, which leaves
    # a new set of delete markers behind, so purge once more before the bucket
    # itself can be deleted
    _purge_all_versions(s3conn, bucket_name)
    s3conn.delete_bucket(Bucket=bucket_name)

