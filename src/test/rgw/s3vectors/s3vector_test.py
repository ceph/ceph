import logging
import json
import tempfile
import random
import socket
import time
import threading
import subprocess
import os
import stat
import string
from datetime import datetime, timedelta, timezone
from datetime import datetime, timezone
import pytest
import boto3
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

    if service_name == 's3vectors':
        config = Config(signature_version='s3v4')
    else:
        config = None

    client = boto3.client(service_name,
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=get_config_zonegroup(),
            config=config)

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

    if service_name == 's3vectors':
        config = Config(signature_version='s3v4')
    else:
        config = None

    client = boto3.client(service_name,
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=get_config_zonegroup(),
            config=config)

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
            region_name=get_config_zonegroup(),
            config=Config(signature_version='s3v4'))

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
    # idempotent create with same definition should succeed
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # create with different dimension should fail
    pytest.raises(conn.exceptions.ClientError, conn.create_index,
                  vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=64, distanceMetric='euclidean')
    # create an index on bucket that does not exist
    invalid_bucket_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.create_index, vectorBucketName=invalid_bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
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


def generate_data(dimension, index=0):
  return {'float32': [random.gauss(float(index), 1.0) for _ in range(dimension)]}


def generate_vectors(num_vectors, dimension):
    vectors = []
    for i in range(num_vectors):
        vectors.append({
            'key': 'vec-' + str(i),
            'data': generate_data(dimension, i)
            })
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
    # all vectors have wrong dimension, so put should fail
    pytest.raises(conn.exceptions.ClientError, conn.put_vectors,
                  vectorBucketName=bucket_name, indexName=index_name, vectors=vectors)
    # verify no vectors were inserted
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(result.get('vectors', [])) == 0
    # mix of correct and wrong dimension vectors - only correct ones should be inserted
    correct_vectors = generate_vectors(5, dimension)
    wrong_vectors = generate_vectors(5, wrong_dimension)
    # rename wrong vectors keys to avoid collisions
    for i, v in enumerate(wrong_vectors):
        v['key'] = f'wrong-{i}'
    mixed_vectors = correct_vectors + wrong_vectors
    result = conn.put_vectors(vectorBucketName=bucket_name, indexName=index_name, vectors=mixed_vectors)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # verify only the correct dimension vectors were inserted
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name, maxResults=100)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    inserted_keys = [v['key'] for v in result.get('vectors', [])]
    assert len(inserted_keys) == 5, f"expected 5 vectors but got {len(inserted_keys)}"
    for i in range(5):
        assert f'vec-{i}' in inserted_keys
    for i in range(5):
        assert f'wrong-{i}' not in inserted_keys
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

