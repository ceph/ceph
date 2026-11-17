import hashlib
import json
import time

import pytest
from botocore.exceptions import ClientError

from .utils import _get_status_and_error_code
from . import (
    configfile,
    setup_teardown,
    get_client,
    get_alt_client,
    get_new_bucket,
    get_lc_debug_interval,
    )


def _arn(bucket):
    return 'arn:aws:s3:::' + bucket


def _set_destination_policy(client, dest, sources):
    policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'Service': 's3.amazonaws.com'},
            'Action': 's3:PutObject',
            'Resource': _arn(dest) + '/*',
            'Condition': {
                'ArnLike': {'aws:SourceArn': [_arn(s) for s in sources]},
            },
        }],
    }
    client.put_bucket_policy(Bucket=dest, Policy=json.dumps(policy))


def _inventory_config(dest, inv_id='inv', prefix='reports', enabled=True,
                      versions='Current', frequency='Daily', fmt='Parquet',
                      fields=None):
    config = {
        'Id': inv_id,
        'IsEnabled': enabled,
        'IncludedObjectVersions': versions,
        'Schedule': {'Frequency': frequency},
        'Destination': {
            'S3BucketDestination': {
                'Bucket': _arn(dest),
                'Format': fmt,
            },
        },
    }
    if prefix:
        config['Destination']['S3BucketDestination']['Prefix'] = prefix
    if fields is not None:
        config['OptionalFields'] = fields
    return config


def _setup_buckets(client):
    src = get_new_bucket(client)
    dest = get_new_bucket(client)
    _set_destination_policy(client, dest, [src])
    return src, dest


def _put_inventory(client, src, config):
    client.put_bucket_inventory_configuration(
        Bucket=src, Id=config['Id'], InventoryConfiguration=config)


def _assert_put_fails(client, src, config, status, code):
    e = pytest.raises(ClientError, _put_inventory, client, src, config)
    assert _get_status_and_error_code(e.value.response) == (status, code)


@pytest.mark.inventory
def test_inventory_put_get_delete():
    client = get_client()
    src, dest = _setup_buckets(client)
    fields = ['Size', 'LastModifiedDate', 'ETag', 'StorageClass']
    config = _inventory_config(dest, versions='All', frequency='Weekly',
                               fields=fields)
    _put_inventory(client, src, config)

    got = client.get_bucket_inventory_configuration(
        Bucket=src, Id='inv')['InventoryConfiguration']
    assert got['Id'] == 'inv'
    assert got['IsEnabled'] is True
    assert got['IncludedObjectVersions'] == 'All'
    assert got['Schedule'] == {'Frequency': 'Weekly'}
    assert got['Destination']['S3BucketDestination'] == {
        'Bucket': _arn(dest), 'Format': 'Parquet', 'Prefix': 'reports'}
    assert sorted(got['OptionalFields']) == sorted(fields)

    response = client.delete_bucket_inventory_configuration(Bucket=src, Id='inv')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    e = pytest.raises(ClientError, client.get_bucket_inventory_configuration,
                      Bucket=src, Id='inv')
    assert _get_status_and_error_code(e.value.response) == \
        (404, 'NoSuchConfiguration')


@pytest.mark.inventory
def test_inventory_put_replaces_config():
    client = get_client()
    src, dest = _setup_buckets(client)
    _put_inventory(client, src, _inventory_config(dest, frequency='Daily'))
    _put_inventory(client, src, _inventory_config(dest, frequency='Weekly',
                                                  enabled=False))

    got = client.get_bucket_inventory_configuration(
        Bucket=src, Id='inv')['InventoryConfiguration']
    assert got['Schedule'] == {'Frequency': 'Weekly'}
    assert got['IsEnabled'] is False
    response = client.list_bucket_inventory_configurations(Bucket=src)
    assert len(response['InventoryConfigurationList']) == 1


@pytest.mark.inventory
def test_inventory_list():
    client = get_client()
    src, dest = _setup_buckets(client)

    response = client.list_bucket_inventory_configurations(Bucket=src)
    assert response.get('InventoryConfigurationList', []) == []
    assert response['IsTruncated'] is False

    ids = ['a', 'b', 'c']
    for inv_id in ids:
        _put_inventory(client, src, _inventory_config(dest, inv_id=inv_id))
    response = client.list_bucket_inventory_configurations(Bucket=src)
    assert sorted(c['Id'] for c in response['InventoryConfigurationList']) == ids
    assert response['IsTruncated'] is False

    client.delete_bucket_inventory_configuration(Bucket=src, Id='b')
    response = client.list_bucket_inventory_configurations(Bucket=src)
    assert sorted(c['Id'] for c in response['InventoryConfigurationList']) == \
        ['a', 'c']


@pytest.mark.inventory
def test_inventory_get_missing():
    client = get_client()
    src, dest = _setup_buckets(client)

    e = pytest.raises(ClientError, client.get_bucket_inventory_configuration,
                      Bucket=src, Id='nope')
    assert _get_status_and_error_code(e.value.response) == \
        (404, 'NoSuchConfiguration')

    _put_inventory(client, src, _inventory_config(dest))
    e = pytest.raises(ClientError, client.get_bucket_inventory_configuration,
                      Bucket=src, Id='nope')
    assert _get_status_and_error_code(e.value.response) == \
        (404, 'NoSuchConfiguration')


@pytest.mark.inventory
def test_inventory_delete_missing():
    client = get_client()
    src, dest = _setup_buckets(client)
    response = client.delete_bucket_inventory_configuration(Bucket=src, Id='nope')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    _put_inventory(client, src, _inventory_config(dest))
    response = client.delete_bucket_inventory_configuration(Bucket=src, Id='nope')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = client.list_bucket_inventory_configurations(Bucket=src)
    assert len(response['InventoryConfigurationList']) == 1


@pytest.mark.inventory
def test_inventory_invalid_config():
    client = get_client()
    src, dest = _setup_buckets(client)

    _assert_put_fails(client, src, _inventory_config(dest, fmt='CSV'),
                      400, 'InvalidArgument')
    _assert_put_fails(client, src, _inventory_config(dest, fields=['Foo']),
                      400, 'InvalidArgument')
    _assert_put_fails(client, src, _inventory_config(dest, inv_id='a/b'),
                      400, 'InvalidArgument')
    _assert_put_fails(client, src, _inventory_config(dest, frequency='Hourly'),
                      400, 'MalformedXML')
    _assert_put_fails(client, src, _inventory_config(dest, versions='Some'),
                      400, 'MalformedXML')

    config = _inventory_config(dest)
    config['Destination']['S3BucketDestination']['Bucket'] = dest
    _assert_put_fails(client, src, config, 400, 'InvalidArgument')

    config = _inventory_config(dest)
    config['Filter'] = {'Prefix': 'photos/'}
    _assert_put_fails(client, src, config, 501, 'NotImplemented')

    response = client.list_bucket_inventory_configurations(Bucket=src)
    assert response.get('InventoryConfigurationList', []) == []


@pytest.mark.inventory
def test_inventory_invalid_destination():
    client = get_client()
    src = get_new_bucket(client)

    _assert_put_fails(client, src, _inventory_config(src + '-missing'),
                      404, 'NoSuchBucket')
    _set_destination_policy(client, src, [src])
    _assert_put_fails(client, src, _inventory_config(src),
                      400, 'InvalidArgument')


@pytest.mark.inventory
def test_inventory_destination_policy():
    client = get_client()
    src = get_new_bucket(client)
    dest = get_new_bucket(client)

    # no policy
    _assert_put_fails(client, src, _inventory_config(dest), 403, 'AccessDenied')

    # policy for another source bucket
    other = get_new_bucket(client)
    _set_destination_policy(client, dest, [other])
    _assert_put_fails(client, src, _inventory_config(dest), 403, 'AccessDenied')

    _set_destination_policy(client, dest, [other, src])
    _put_inventory(client, src, _inventory_config(dest))


@pytest.mark.inventory
def test_inventory_permissions():
    client = get_client()
    alt_client = get_alt_client()
    src, dest = _setup_buckets(client)
    _put_inventory(client, src, _inventory_config(dest))

    _assert_put_fails(alt_client, src, _inventory_config(dest, inv_id='alt'),
                      403, 'AccessDenied')
    for call in (alt_client.get_bucket_inventory_configuration,
                 alt_client.delete_bucket_inventory_configuration):
        e = pytest.raises(ClientError, call, Bucket=src, Id='inv')
        assert _get_status_and_error_code(e.value.response) == \
            (403, 'AccessDenied')
    e = pytest.raises(ClientError, alt_client.list_bucket_inventory_configurations,
                      Bucket=src)
    assert _get_status_and_error_code(e.value.response) == (403, 'AccessDenied')


def _list_keys(client, bucket, prefix=''):
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o['Key'] for o in response.get('Contents', [])]


def _wait_for_manifest(client, dest, prefix, cycles=6):
    interval = get_lc_debug_interval()
    for _ in range(cycles):
        time.sleep(interval)
        manifests = [k for k in _list_keys(client, dest, prefix)
                     if k.endswith('/manifest.json')]
        if manifests:
            return manifests
    return []


def _get_body(client, bucket, key):
    return client.get_object(Bucket=bucket, Key=key)['Body'].read()


def _check_report(client, src, dest, manifest_key):
    raw = _get_body(client, dest, manifest_key)
    checksum_key = manifest_key[:-len('manifest.json')] + 'manifest.checksum'
    assert _get_body(client, dest, checksum_key).decode() == \
        hashlib.md5(raw).hexdigest()

    manifest = json.loads(raw)
    assert manifest['sourceBucket'] == src
    assert manifest['destinationBucket'] == _arn(dest)
    assert manifest['fileFormat'] == 'Parquet'
    assert manifest['fileSchema'].startswith('message s3.inventory {')
    assert len(manifest['files']) == 1

    report = manifest['files'][0]
    data = _get_body(client, dest, report['key'])
    assert len(data) == report['size']
    assert hashlib.md5(data).hexdigest() == report['MD5checksum']
    assert data[:4] == b'PAR1' and data[-4:] == b'PAR1'
    return manifest, data


@pytest.mark.inventory
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_inventory_delivery():
    client = get_client()
    src, dest = _setup_buckets(client)
    keys = ['a', 'dir/b', 'dir/c']
    for key in keys:
        client.put_object(Bucket=src, Key=key, Body=key)
    _put_inventory(client, src, _inventory_config(
        dest, inv_id='daily', fields=['Size', 'ETag', 'StorageClass']))

    prefix = 'reports/{}/daily/'.format(src)
    manifests = _wait_for_manifest(client, dest, prefix)
    assert len(manifests) == 1
    manifest, data = _check_report(client, src, dest, manifests[0])
    assert 'required binary key (STRING);' in manifest['fileSchema']
    assert 'optional int64 size;' in manifest['fileSchema']
    assert 'version_id' not in manifest['fileSchema']

    # one report per day: the next lifecycle cycles don't add another one
    time.sleep(2 * get_lc_debug_interval())
    data_keys = [k for k in _list_keys(client, dest, prefix)
                 if k.endswith('.parquet')]
    assert len(data_keys) == 1

    pq = pytest.importorskip('pyarrow.parquet')
    import pyarrow as pa
    table = pq.read_table(pa.BufferReader(data)).to_pydict()
    assert table['key'] == keys
    assert table['bucket'] == [src] * len(keys)
    assert table['size'] == [len(k) for k in keys]
    assert table['storage_class'] == ['STANDARD'] * len(keys)


@pytest.mark.inventory
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_inventory_delivery_versioned():
    client = get_client()
    src, dest = _setup_buckets(client)
    client.put_object(Bucket=src, Key='obj', Body='before versioning')
    client.put_bucket_versioning(Bucket=src,
                                 VersioningConfiguration={'Status': 'Enabled'})
    client.put_object(Bucket=src, Key='obj', Body='after versioning')
    client.delete_object(Bucket=src, Key='obj')
    _put_inventory(client, src, _inventory_config(dest, prefix='',
                                                  versions='All'))

    manifests = _wait_for_manifest(client, dest, '{}/inv/'.format(src))
    assert len(manifests) == 1
    manifest, data = _check_report(client, src, dest, manifests[0])
    for column in ('version_id', 'is_latest', 'is_delete_marker'):
        assert column in manifest['fileSchema']

    pq = pytest.importorskip('pyarrow.parquet')
    import pyarrow as pa
    rows = pq.read_table(pa.BufferReader(data)).to_pylist()
    assert len(rows) == 3
    delete_marker, current, legacy = rows
    assert delete_marker['is_delete_marker'] and delete_marker['is_latest']
    assert not current['is_delete_marker'] and not current['is_latest']
    assert legacy['version_id'] == 'null'
    assert not legacy['is_latest']


@pytest.mark.inventory
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_inventory_disabled_not_delivered():
    client = get_client()
    src, dest = _setup_buckets(client)
    client.put_object(Bucket=src, Key='obj', Body='data')
    _put_inventory(client, src, _inventory_config(dest, enabled=False))

    assert _wait_for_manifest(client, dest, '', cycles=3) == []
    assert _list_keys(client, dest) == []
