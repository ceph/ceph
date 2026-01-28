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
import pytest
import boto3
from botocore.config import Config

from . import(
    configfile,
    get_config_host,
    get_config_port,
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
        config = Config(signature_version='s3')
    else:
        config = None

    client = boto3.client(service_name,
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
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
        config = Config(signature_version='s3')
    else:
        config = None

    client = boto3.client(service_name,
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
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
            config=Config(signature_version='s3'))

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
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # cleanup
    _delete_all_vector_buckets(conn)


@pytest.mark.vector_bucket_test
def test_get_vector_bucket():
    conn = connection()
    bucket_name = gen_bucket_name()
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    log.info("get_vector_buckets result: %s", result)
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
    # create an index on bucket that does not exist
    invalid_bucket_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.create_index, vectorBucketName=invalid_bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.index_test
def test_get_index():
    conn = connection()
    bucket_name = gen_bucket_name()
    result = conn.create_vector_bucket(vectorBucketName=bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    index_name = 'test-index'
    result = conn.create_index(vectorBucketName=bucket_name, indexName=index_name, dataType='float32', dimension=128, distanceMetric='euclidean')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.get_index(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
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
    #with pytest.raises(conn.exceptions.NoSuchIndex):
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
    # list indexs from bucket that does not exist
    invalid_bucket_name = bucket_name + '-invalid'
    pytest.raises(conn.exceptions.ClientError, conn.list_indexes, vectorBucketName=invalid_bucket_name)
    # not implemented yet
    #index_names = [i['IndexName'] for i in result['Indexes']]
    #assert index_name1 in index_names
    #assert index_name2 in index_names
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


def generate_data(dimension):
    return {'float32': [float(j) for j in range(dimension)]}


def generate_vectors(num_vectors, dimension):
    vectors = []
    for i in range(num_vectors):
        vectors.append({
            'key': 'vec-' + str(i),
            'data': generate_data(dimension)
            })
    return vectors


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
def test_get_vectors():
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
    vector_ids = ['vec-' + str(i) for i in range(10)]
    result = conn.get_vectors(vectorBucketName=bucket_name, indexName=index_name, keys=vector_ids)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # not implemented yet
    #assert len(result['Vectors']) == 10
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_list_vectors():
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
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # not implemented yet
    #assert len(result['Vectors']) == 10
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_delete_vectors():
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
    vector_ids = ['vec-' + str(i) for i in range(10)]
    result = conn.delete_vectors(vectorBucketName=bucket_name, indexName=index_name, keys=vector_ids)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    result = conn.list_vectors(vectorBucketName=bucket_name, indexName=index_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # not implemented yet
    #assert len(result['Vectors']) == 0
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)


@pytest.mark.vector_test
def test_query_vectors():
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
    query_vector = generate_data(128)
    result = conn.query_vectors(vectorBucketName=bucket_name, indexName=index_name, queryVector=query_vector, topK=5)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    # not implemented yet
    #assert len(result['Results']) == 5
    # cleanup
    _ = conn.delete_vector_bucket(vectorBucketName=bucket_name)

