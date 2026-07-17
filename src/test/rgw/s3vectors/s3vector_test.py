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
from botocore.config import Config

from . import(
    configfile,
    get_config_host,
    get_config_port,
    get_config_zonegroup,
    get_access_key,
    get_secret_key,
    get_config_host2,
    get_config_port2
    )


# configure logging for the tests module
log = logging.getLogger(__name__)

num_buckets = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/../'

def bash(cmd, **kwargs):
    log.debug('running command: %s', ' '.join(cmd))
    kwargs['stdout'] = subprocess.PIPE
    process = subprocess.Popen(cmd, **kwargs)
    s = process.communicate()[0].decode('utf-8')
    return (s, process.returncode)


def admin(args, **kwargs):
    """ radosgw-admin command """
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', 'noname'] + args
    return bash(cmd, **kwargs)


def ceph_admin(args, **kwargs):
    """ ceph command """
    cmd = [test_path + 'test-rgw-call.sh', 'call_ceph', 'noname'] + args
    return bash(cmd, **kwargs)


def set_rgw_config_option(option, value):
    """ change a config option """
    client = f'client.rgw.{get_config_port()}'
    return ceph_admin(['config', 'set', client, option, str(value)])


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


def another_user(tenant=None):
    access_key = str(time.time())
    secret_key = str(time.time())
    uid = 'superman' + str(time.time())
    if tenant:
        _, result = admin(['user', 'create', '--uid', uid, '--tenant', tenant, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])
    else:
        _, result = admin(['user', 'create', '--uid', uid, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])

    assert result == 0
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

    return client


#################
# s3vectors tests
#################

def _delete_all_vector_buckets(conn):
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    for bucket in result['vectorBuckets']:
        _ = conn.delete_vector_bucket(vectorBucketName=bucket['vectorBucketName'])


@pytest.mark.vector_bucket_test
def test_create_vector_bucket():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    os.chmod(db_path, 0o555)
    try:
        pytest.raises(conn.exceptions.ClientError, conn.create_vector_bucket, vectorBucketName=bucket_name)
    finally:
        os.chmod(db_path, 0o755)


@pytest.mark.vector_bucket_test
def test_get_vector_bucket():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    pytest.raises(conn.exceptions.ClientError, conn.get_vector_bucket, vectorBucketName=bucket_name)
    pytest.raises(conn.exceptions.ClientError, conn.delete_vector_bucket, vectorBucketName=bucket_name)
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result['vectorBuckets']) == 0
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.vector_bucket_test
def test_list_vector_buckets():
    conn = connection()
    bucket_name1 = gen_bucket_name()
    bucket_name2 = gen_bucket_name()
    result = conn.create_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
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


@pytest.mark.vector_bucket_test
def test_vector_buckets_sync():
    conn = connection()
    conn2 = connection2()
    if not conn2:
        log.info("Skipping test_vector_buckets_sync since second connection is not configured")
        return

    # create buckets from the first connection
    bucket_name1 = gen_bucket_name()
    bucket_name2 = gen_bucket_name()
    result = conn.create_vector_bucket(vectorBucketName=bucket_name1)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.create_vector_bucket(vectorBucketName=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name1 in bucket_names
    assert bucket_name2 in bucket_names
    time.sleep(5)

    # now check from the second connection
    result = conn2.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets from conn2 result: %s", result)
    bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name1 in bucket_names
    assert bucket_name2 in bucket_names

    # create buckets from the 2nd connection
    bucket_name3 = gen_bucket_name()
    bucket_name4 = gen_bucket_name()
    result = conn2.create_vector_bucket(vectorBucketName=bucket_name3)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn2.create_vector_bucket(vectorBucketName=bucket_name4)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn2.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets from conn2 result: %s", result)
    bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name3 in bucket_names
    assert bucket_name4 in bucket_names
    time.sleep(5)

    # now check from the first connection
    result = conn.list_vector_buckets()
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("list_vector_buckets result: %s", result)
    bucket_names = [b['vectorBucketName'] for b in result['vectorBuckets']]
    assert bucket_name3 in bucket_names
    assert bucket_name4 in bucket_names

    # cleanup
    _delete_all_vector_buckets(conn)


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
    result = conn.create_vector_bucket(vectorBucketName=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # and an s3 bucket with the same name
    _create_s3bucket(s3conn, bucket_name2)
    # delete the s3 bucket
    result = s3conn.delete_bucket(Bucket=bucket_name2)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 204
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_create_index_invalid_filterable_keys():
    """Test that invalid filterable metadata key names fail with ValidationException."""
    conn = connection()
    bucket_name = gen_bucket_name()
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    common = dict(vectorBucketName=bucket_name, dataType='float32', dimension=4, distanceMetric='euclidean')

    # duplicate field names
    assert_create_index_validation_error(conn,
        'metadataConfiguration.filterableMetadataKeys',
        indexName='dup-fields',
        metadataConfiguration={'filterableMetadataKeys': [
            {'name': 'genre'}, {'name': 'genre'}
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

    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_get_index():
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 128
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_non_filterable_metadata_keys():
    """Test that nonFilterableMetadataKeys is stored on CreateIndex and returned on GetIndex."""
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_filterable_metadata_keys():
    """Test filterableMetadataKeys: store on CreateIndex, retrieve on GetIndex,
    and populate filterable columns via PutVectors."""
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_filterable_metadata_list_keys():
    """Test filterableMetadataKeys with list types: StringList, NumberList, BooleanList."""
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_metadata_dots_in_names_rejected():
    """Test that dots in metadata key names are rejected at index creation."""
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_delete_index():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    # not implemented yet
    #with pytest.raises(conn.exceptions.ClientError):
    #    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_list_indexes():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


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


def verify_get_vectors(conn, bucket_name, index_name, vector_ids, expected_dimension=None):
    return_data = expected_dimension is not None
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name,
                             keys=vector_ids, returnData=return_data)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    assert 'vectors' in result
    returned_vectors = result['vectors']
    num_expected = len(vector_ids)
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
            vector_pairs = zip(retrieved['data']['float32'], expected['data']['float32'])
            assert all(abs(a - b) < 1e-6 for a, b in vector_pairs), \
                f"returned data don't match expected data for key {retrieved['key']}"

    log.info('pagination verification completed: %d vectors across %d pages',
             len(all_retrieved_vectors), page_count)

    return all_retrieved_vectors, page_count


@pytest.mark.vector_test
def test_put_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    vectors = generate_vectors(10, 128)
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


def update_vectors_thread(conn, bucket_name, thread_id):
    index_name = 'test-index-'+str(thread_id)
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    dimension = 128
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
    returned_vectors = verify_get_vectors(conn, bucket_name, index_name, vector_ids, expected_dimension=dimension)


@pytest.mark.vector_test
def test_update_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    num_indexes = 10
    threads = []
    for i in range(num_indexes):
        t = threading.Thread(target=update_vectors_thread, args=(conn, bucket_name, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_vectors_dimension_mismatch():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_get_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 128
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

    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_get_vectors_without_data():
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 128
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_list_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
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

    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_list_vectors_with_data():
    """Test list_vectors with returnData=True to verify data is returned."""
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_list_vectors_without_data():
    """Test list_vectors with returnData=False to verify data is not returned."""
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_list_vectors_pagination():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_list_vectors_exact_pagination():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_delete_vectors():
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors():
    dimension = 8
    conn = connection()
    bucket_name = gen_bucket_name()
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

    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors_with_distance():
    dimension = 8
    conn = connection()
    bucket_name = gen_bucket_name()
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_and_get_vectors_metadata():
    """Test storing and retrieving a mix of vectors with and without metadata."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_list_vectors_with_metadata():
    """Test that list_vectors returns metadata when returnMetadata=True."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors_with_metadata():
    """Test that query_vectors returns metadata when returnMetadata=True."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_vectors_malformed_metadata():
    """Test that vectors with malformed JSON metadata are skipped."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 8
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_vectors_null_metadata_value():
    """Test that vectors with null metadata values are rejected."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_vectors_dots_in_metadata_field_names():
    """Test that vectors with dots in metadata field names are rejected."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_vectors_missing_filterable_fields():
    """Test that vectors with missing filterable metadata fields are inserted with nulls."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_vectors_invalid_filterable_types():
    """Test that vectors with wrong types for filterable fields fail with ValidationException.
    Each invalid type mismatch is tested individually since PutVectors is all-or-nothing."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_put_vectors_must_exist():
    """Test the mustExist flag on filterable metadata keys."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors_filter():
    """Test metadata filtering during vector queries."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors_post_filtering():
    """Test that postFiltering forces all filtering through JSON post-filtering."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    # post-filtering with topK=2 only sees the 2 nearest (v0, v1), so v3 is excluded
    assert query_keys({'genre': 'rock'}, top_k=2, post_filtering=True) == ['v0']

    # post-filtering allows mixed $or (column + JSON fields)
    assert query_keys({'$or': [{'genre': 'rock'}, {'color': 'blue'}]}, top_k=10, post_filtering=True) == ['v0', 'v1', 'v3']

    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


    result = conn.create_index(
        vectorBucketName=bucket_name,
        indexName=index_name,
        dataType='float32',
        dimension=dimension,
        distanceMetric='euclidean'
    )
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    vectors = [
        {'key':'v0', 'data':generate_data(dimension, 0)},
        {'key':'v1', 'data':generate_data(dimension, 1)}
    ]
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.vector_test
def test_vector_bucket_policy_baseline():
    owner = connection()
    other = another_user()
    bucket_name = gen_bucket_name()
    index_name = 'test-index'

    _setup_vector_bucket_with_index(owner, bucket_name, index_name)
    
    result = other.get_vectors(
            vectorBucketName=bucket_name, 
            indexName=index_name,
            keys=['v0'])

    status = result['ResponseMetadata']['HTTPStatusCode']
    print(result['ResponseMetadata'])
    assert status in (200, 403, 404)

    #cleanup
    _ = owner.delete_vector_bucket(vectorBucketName=bucket_name)

@pytest.mark.vector_test
def test_put_get_delete_vector_bucket_policy():
    owner = connection()
    bucket_name = gen_bucket_name()
    index_name = "test-index"

    _setup_vector_bucket_with_index(owner, bucket_name, index_name)

    bucket_arn = result['vectorBucketArn']
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": "*",
                       "Action": "s3vectors:GetVectors",
                       "Resource": bucket_arn}]
    })

    result = owner.put_vector_bucket_policy(
        vectorBucketName=bucket_name, policy=policy)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    
    #get
    result = owner.get_vector_bucket_policy(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    returned_policy = json.loads(result['policy'])
    assert returned_policy['version'] == '2012-10-17'

    #delete
    result = owner.delete_vector_bucket_policy(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    # post delete get must fail
    with pytest.raises(owner.exceptions.ClientError) as err_info:
        owner.get_vector_bucket_policy(vectorBucketName=bucket_name)
    assert err_info.value.response['Error']['Code'] == 'NoSuchBucketPolicy'

    #cleanup
    owner.delete_vector_bucket(vectorBucketName=bucket_name)
@pytest.mark.vector_test
def test_query_vectors_filter_nonfilterable():
    """Test that filtering on non-filterable keys is rejected."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors_filter_json_metadata():
    """Test filtering on undeclared metadata fields using JSON extraction."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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

    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors_filter_errors():
    """Test that invalid filter expressions are rejected with 400."""
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors_post_filter_topk():
    """ Test topK oversampling with JSON post-filtering """

    set_rgw_config_option('rgw_s3vector_topk_post_filter_factor', 1.5)
    conn = connection()
    bucket_name = gen_bucket_name()
    dimension = 4
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
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)
    set_rgw_config_option('rgw_s3vector_topk_post_filter_factor', 1)

