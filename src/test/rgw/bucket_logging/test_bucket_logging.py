import logging
import re
import subprocess
import os
import json
import time
import random
import string
import pytest
import boto3
from botocore.exceptions import ClientError

from . import (
    get_config_host,
    get_config_port,
    get_access_key,
    get_secret_key,
    get_user_id
)

log = logging.getLogger(__name__)

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/../'

num_buckets = 0
run_prefix = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))


def make_logging_policy(log_bucket, user_id, source_bucket=None):
    statement = {
        "Sid": "AllowLogging",
        "Effect": "Allow",
        "Principal": {"Service": "logging.s3.amazonaws.com"},
        "Action": "s3:PutObject",
        "Resource": f"arn:aws:s3:::{log_bucket}/*",
        "Condition": {
            "StringEquals": {"aws:SourceAccount": user_id}
        }
    }

    if source_bucket:
        statement["Condition"]["ArnLike"] = {
            "aws:SourceArn": f"arn:aws:s3:::{source_bucket}"
        }

    return {
        "Version": "2012-10-17",
        "Statement": [statement]
    }


def bash(cmd, **kwargs):
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
    process = subprocess.Popen(cmd, **kwargs)
    stdout, stderr = process.communicate()
    return (stdout.decode('utf-8'), process.returncode)


def admin(args, **kwargs):
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', 'noname'] + args
    return bash(cmd, **kwargs)


def rados(args, **kwargs):
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_rados', 'noname'] + args
    return bash(cmd, **kwargs)


def gen_bucket_name(prefix="bucket"):
    global num_buckets
    num_buckets += 1
    return f"{run_prefix}-{prefix}-{num_buckets}"


def get_s3_client():
    hostname = get_config_host()
    port = get_config_port()
    access_key = get_access_key()
    secret_key = get_secret_key()

    if port in (443, 8443):
        endpoint_url = f'https://{hostname}:{port}'
    else:
        endpoint_url = f'http://{hostname}:{port}'

    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )


@pytest.fixture
def s3_client():
    return get_s3_client()


def get_bucket_id(bucket_name):
    """Get the bucket ID using radosgw-admin bucket stats."""
    output, ret = admin(['bucket', 'stats', '--bucket', bucket_name])
    if ret != 0:
        log.error(f"Failed to get bucket stats for {bucket_name}")
        return None
    try:
        stats = json.loads(output)
        return stats.get('id')
    except json.JSONDecodeError:
        log.error(f"Failed to parse bucket stats JSON: {output}")
        return None


def find_temp_log_objects(bucket_id, pool='default.rgw.buckets.data'):
    """Returns (list of temp object names, success bool)."""
    output, ret = rados(['ls', '--pool', pool])

    if ret != 0:
        log.error(f"rados ls failed with code {ret}: {output}")
        return [], False

    if not output or not bucket_id:
        return [], True

    temp_objects = []
    for line in output.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        if bucket_id in line and '__shadow_' in line:
            temp_objects.append(line)
            log.debug(f"Found temp object: {line}")

    return temp_objects, True


def parse_logging_sources(output):
    """Parse bucket logging info output and return list of source bucket names.
    The output of 'bucket logging info --bucket <log_bucket>' is a bare JSON array
    of objects, each with a 'name' key."""
    if not output or not output.strip():
        return []
    info = json.loads(output)
    return [s['name'] for s in info]


def parse_logging_config(output):
    """Parse bucket logging info output and return the loggingEnabled dict.
    The output of 'bucket logging info --bucket <source_bucket>' is:
    {"bucketLoggingStatus": {"loggingEnabled": {...}}}"""
    info = json.loads(output)
    return info['bucketLoggingStatus']['loggingEnabled']


def parse_flush_output(output):
    """Parse the flushed object name from radosgw-admin flush output."""
    match = re.search(r"flushed pending logging object '([^']+)'", output)
    assert match, f"Failed to parse flushed object name from flush output: {output}"
    return match.group(1)


def verify_log_object_content(s3_client, bucket, key, source_bucket):
    """Download a log object and assert it has non-empty content referencing source_bucket."""
    body = s3_client.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
    assert len(body) > 0, "Flushed log object should not be empty"
    assert source_bucket in body, f"Log records should reference source bucket {source_bucket}"


def upload_test_objects(s3_client, bucket, count=3):
    for i in range(count):
        s3_client.put_object(
            Bucket=bucket,
            Key=f'object-{i}.txt',
            Body=f'content {i}'.encode()
        )


def setup_logging_target(s3_client, log_bucket, source_bucket=None):
    """Create log bucket and set logging policy. Returns success status."""
    try:
        s3_client.create_bucket(Bucket=log_bucket)
        log.debug(f"Created log bucket: {log_bucket}")

        user_id = get_user_id()
        policy = json.dumps(make_logging_policy(log_bucket, user_id, source_bucket))
        s3_client.put_bucket_policy(Bucket=log_bucket, Policy=policy)
        log.debug(f"Set logging policy on log bucket: {log_bucket}")
        return True
    except ClientError as e:
        log.error(f"Error setting up logging target: {e}")
        return False


def enable_bucket_logging(s3_client, source_bucket, log_bucket, prefix=None, logging_type='Standard'):
    """Enable logging on source bucket pointing to log bucket. Returns success status."""
    if prefix is None:
        prefix = f'{source_bucket}/'
    try:
        logging_enabled = {
            'TargetBucket': log_bucket,
            'TargetPrefix': prefix
                        }
        if logging_type == 'Journal':
            logging_enabled['LoggingType'] = 'Journal'
        s3_client.put_bucket_logging(Bucket=source_bucket, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled
        })
        log.debug(f"Enabled {logging_type} logging on {source_bucket} -> {log_bucket} with prefix '{prefix}'")
        return True
    except ClientError as e:
        log.error(f"Error enabling bucket logging: {e}")
        return False


def create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type='Standard'):
    """Create source/log buckets and enable logging. Returns success status."""
    try:
        s3_client.create_bucket(Bucket=source_bucket)
        log.debug(f"Created source bucket: {source_bucket}")
    except ClientError as e:
        log.error(f"Error creating source bucket: {e}")
        return False

    if not setup_logging_target(s3_client, log_bucket, source_bucket):
        return False
    return enable_bucket_logging(s3_client, source_bucket, log_bucket, logging_type=logging_type)


def setup_multi_source_logging(s3_client, source_bucket_1, source_bucket_2, log_bucket, logging_type='Standard'):
    """Create two source buckets logging to the same log bucket. Returns success status."""
    try:
        s3_client.create_bucket(Bucket=log_bucket)

        user_id = get_user_id()
        policy = json.dumps(make_logging_policy(log_bucket, user_id))
        s3_client.put_bucket_policy(Bucket=log_bucket, Policy=policy)

        s3_client.create_bucket(Bucket=source_bucket_1)
        s3_client.create_bucket(Bucket=source_bucket_2)

        for src in [source_bucket_1, source_bucket_2]:
            logging_enabled = {
                'TargetBucket': log_bucket,
                'TargetPrefix': f'{src}/'
            }
            if logging_type == 'Journal':
                logging_enabled['LoggingType'] = 'Journal'
            s3_client.put_bucket_logging(
                Bucket=src,
                BucketLoggingStatus={
                    'LoggingEnabled': logging_enabled
                }
            )

        return True
    except ClientError as e:
        log.error(f"Error setting up multi-source logging: {e}")
        return False


def cleanup_bucket(s3_client, bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                log.debug(f"Deleted object: {obj['Key']}")

        s3_client.delete_bucket(Bucket=bucket_name)
        log.debug(f"Deleted bucket: {bucket_name}")
    except ClientError as e:
        log.warning(f"Error cleaning up bucket {bucket_name}: {e}")


#####################
# bucket logging tests
#####################

@pytest.mark.basic_test
def test_bucket_logging_list(s3_client, logging_type):
    """Test radosgw-admin bucket logging list command.

    Note: the 'list' command returns pending commit objects — log objects that have
    been rolled over but not yet delivered to the log bucket. Rollover only happens
    after obj_roll_time (default 300s) expires, and even then the background
    BucketLoggingManager delivers them within ~10s. The 'flush' command bypasses
    the commit list entirely (synchronous delivery). This makes it impractical to
    verify actual list contents in a test, so we only validate the command succeeds
    and returns a well-formed JSON array.
    """
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        upload_test_objects(s3_client, source_bucket)

        output, ret = admin(['bucket', 'logging', 'list', '--bucket', source_bucket])

        assert ret == 0, f"bucket logging list failed with return code {ret}"
        assert output.strip(), "bucket logging list returned no output"
        pending = json.loads(output)
        assert isinstance(pending, list), f"Expected JSON array, got: {type(pending)}"

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_bucket_logging_info_source(s3_client, logging_type):
    """Test radosgw-admin bucket logging info on source bucket."""
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])

        assert ret == 0, f"bucket logging info failed with return code {ret}"
        assert output.strip(), "bucket logging info returned no output for source bucket"

        config = parse_logging_config(output)
        assert config['targetBucket'] == log_bucket
        assert config['targetPrefix'] == f'{source_bucket}/'

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_bucket_logging_info_log(s3_client, logging_type):
    """Test radosgw-admin bucket logging info on log bucket."""
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', log_bucket])

        assert ret == 0, f"bucket logging info failed with return code {ret}"
        assert output.strip(), "bucket logging info returned empty output for log bucket"
        source_names = parse_logging_sources(output)
        assert source_bucket in source_names, f"Source bucket {source_bucket} not in logging_sources: {source_names}"

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_bucket_logging_flush(s3_client, logging_type):
    """Test radosgw-admin bucket logging flush command."""
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        upload_test_objects(s3_client, source_bucket)

        output, ret = admin(['bucket', 'logging', 'flush', '--bucket', source_bucket])

        assert ret == 0, f"bucket logging flush failed with return code {ret}"
        assert output.strip(), "bucket logging flush returned empty output"
        flushed_obj_name = parse_flush_output(output)
        time.sleep(2)

        response = s3_client.list_objects_v2(Bucket=log_bucket)
        assert 'Contents' in response, "Log bucket should have contents after flush"
        log_object_keys = [obj['Key'] for obj in response['Contents']]
        assert flushed_obj_name in log_object_keys, f"Flushed object '{flushed_obj_name}' not in log bucket: {log_object_keys}"
        verify_log_object_content(s3_client, log_bucket, flushed_obj_name, source_bucket)

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_cleanup_on_log_bucket_delete(s3_client, logging_type):
    """Test that temp log objects are deleted when log bucket is deleted."""
    source_bucket = gen_bucket_name("cleanup-source")
    log_bucket = gen_bucket_name("cleanup-log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        log_bucket_id = get_bucket_id(log_bucket)
        assert log_bucket_id is not None, f"Failed to get bucket ID for {log_bucket}"

        upload_test_objects(s3_client, source_bucket)

        temp_objects_before, success = find_temp_log_objects(log_bucket_id)
        assert success, "Failed to list rados objects"
        assert len(temp_objects_before) > 0, "Expected temp objects to exist before cleanup"

        cleanup_bucket(s3_client, log_bucket)
        time.sleep(2)

        temp_objects_after, success = find_temp_log_objects(log_bucket_id)
        assert success, "Failed to list rados objects after cleanup"
        assert len(temp_objects_after) == 0, f"Temp objects still exist after deletion: {temp_objects_after}"

    except:
        cleanup_bucket(s3_client, log_bucket)
    finally:
        cleanup_bucket(s3_client, source_bucket)


@pytest.mark.basic_test
def test_cleanup_on_logging_disable(s3_client, logging_type):
    """Test that disabling logging flushes pending logs to the log bucket."""
    source_bucket = gen_bucket_name("disable-source")
    log_bucket = gen_bucket_name("disable-log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        upload_test_objects(s3_client, source_bucket)

        response = s3_client.list_objects_v2(Bucket=log_bucket)
        assert 'Contents' not in response, "Log bucket should be empty before disable"

        s3_client.put_bucket_logging(Bucket=source_bucket, BucketLoggingStatus={})
        time.sleep(2)

        response = s3_client.list_objects_v2(Bucket=log_bucket)
        assert 'Contents' in response, "Expected flushed log objects after disable"
        log_objects = response['Contents']
        assert len(log_objects) > 0, "Expected at least one flushed log object"
        assert log_objects[0]['Key'].startswith(f'{source_bucket}/')
        verify_log_object_content(s3_client, log_bucket, log_objects[0]['Key'], source_bucket)

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.skip(reason="https://tracker.ceph.com/issues/75295")
@pytest.mark.basic_test
def test_cleanup_on_logging_config_change(s3_client, logging_type):
    """Test that changing logging target bucket implicitly flushes pending records to the old bucket."""
    source_bucket = gen_bucket_name("config-change-source")
    log_bucket_1 = gen_bucket_name("config-change-log1")
    log_bucket_2 = gen_bucket_name("config-change-log2")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket_1, logging_type)

        upload_test_objects(s3_client, source_bucket)

        assert setup_logging_target(s3_client, log_bucket_2, source_bucket)
        assert enable_bucket_logging(s3_client, source_bucket, log_bucket_2, logging_type=logging_type)
        time.sleep(2)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret == 0, f"bucket logging info failed with return code {ret}"
        config = parse_logging_config(output)
        assert config['targetBucket'] == log_bucket_2

        response = s3_client.list_objects_v2(Bucket=log_bucket_1)
        assert 'Contents' in response, "Old log bucket should have implicitly flushed records"
        log_objects = response['Contents']
        assert len(log_objects) > 0, "Expected at least one flushed log object in old bucket"
        verify_log_object_content(s3_client, log_bucket_1, log_objects[0]['Key'], source_bucket)

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket_1)
        cleanup_bucket(s3_client, log_bucket_2)


@pytest.mark.basic_test
def test_cleanup_on_source_bucket_delete(s3_client, logging_type):
    """Test that deleting source bucket flushes pending logs."""
    source_bucket = gen_bucket_name("src-delete-source")
    log_bucket = gen_bucket_name("src-delete-log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        upload_test_objects(s3_client, source_bucket)

        response = s3_client.list_objects_v2(Bucket=source_bucket)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=source_bucket, Key=obj['Key'])
        s3_client.delete_bucket(Bucket=source_bucket)
        time.sleep(2)

        response = s3_client.list_objects_v2(Bucket=log_bucket)
        assert 'Contents' in response, "Expected log objects after source bucket deletion"
        log_objects = response['Contents']
        assert len(log_objects) > 0
        assert log_objects[0]['Key'].startswith(f'{source_bucket}/')
        verify_log_object_content(s3_client, log_bucket, log_objects[0]['Key'], source_bucket)

    except:
        cleanup_bucket(s3_client, source_bucket)
    finally:
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_bucket_logging_info_log_multiple_sources(s3_client, logging_type):
    """Test that multiple source buckets can log to the same log bucket."""
    source_bucket_1 = gen_bucket_name("multi-source1")
    source_bucket_2 = gen_bucket_name("multi-source2")
    log_bucket = gen_bucket_name("multi-log")

    try:
        assert setup_multi_source_logging(s3_client, source_bucket_1, source_bucket_2, log_bucket, logging_type)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', log_bucket])

        assert ret == 0, f"bucket logging info failed with return code {ret}"
        assert output.strip(), "bucket logging info returned empty output for log bucket"
        source_names = parse_logging_sources(output)
        assert source_bucket_1 in source_names, f"{source_bucket_1} not in logging_sources: {source_names}"
        assert source_bucket_2 in source_names, f"{source_bucket_2} not in logging_sources: {source_names}"

    finally:
        cleanup_bucket(s3_client, source_bucket_1)
        cleanup_bucket(s3_client, source_bucket_2)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_multiple_sources_disable_one(s3_client, logging_type):
    """Test that disabling one source does not affect the other source's logging."""
    source_bucket_1 = gen_bucket_name("disable-one-src1")
    source_bucket_2 = gen_bucket_name("disable-one-src2")
    log_bucket = gen_bucket_name("disable-one-log")

    try:
        assert setup_multi_source_logging(s3_client, source_bucket_1, source_bucket_2, log_bucket, logging_type)

        s3_client.put_bucket_logging(Bucket=source_bucket_1, BucketLoggingStatus={})

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket_1])
        assert ret == 0
        assert 'targetBucket' not in output, f"Disabled source should not have logging config: {output}"

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket_2])
        assert ret == 0
        assert output.strip(), "Active source should still have logging config"
        config = parse_logging_config(output)
        assert config['targetBucket'] == log_bucket

        upload_test_objects(s3_client, source_bucket_2)
        output, ret = admin(['bucket', 'logging', 'flush', '--bucket', source_bucket_2])
        assert ret == 0, f"bucket logging flush failed for source 2"
        assert output.strip(), "Flush of active source should produce output"
        flushed_obj = parse_flush_output(output)
        time.sleep(2)

        response = s3_client.list_objects_v2(Bucket=log_bucket)
        assert 'Contents' in response, "Expected log objects in log bucket after flush"
        log_object_keys = [obj['Key'] for obj in response['Contents']]
        assert flushed_obj in log_object_keys, f"Flushed object '{flushed_obj}' not in log bucket: {log_object_keys}"
        verify_log_object_content(s3_client, log_bucket, flushed_obj, source_bucket_2)

    finally:
        cleanup_bucket(s3_client, source_bucket_1)
        cleanup_bucket(s3_client, source_bucket_2)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_logging_info_after_disable(s3_client, logging_type):
    """Verify that bucket logging info returns empty for source after logging is disabled."""
    source_bucket = gen_bucket_name("info-disable-src")
    log_bucket = gen_bucket_name("info-disable-log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret == 0
        assert output.strip(), "bucket logging info returned empty output before disable"
        config = parse_logging_config(output)
        assert config['targetBucket'] == log_bucket

        s3_client.put_bucket_logging(Bucket=source_bucket, BucketLoggingStatus={})

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret == 0
        assert not output.strip(), f"Should not have logging config after disable: {output}"

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_logging_info_after_source_delete(s3_client, logging_type):
    """Verify that bucket logging info on deleted source returns error."""
    source_bucket = gen_bucket_name("info-delete-src")
    log_bucket = gen_bucket_name("info-delete-log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret == 0
        assert output.strip(), "bucket logging info returned empty before delete"
        config = parse_logging_config(output)
        assert config['targetBucket'] == log_bucket

        s3_client.delete_bucket(Bucket=source_bucket)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret != 0 or not output.strip(), f"Expected failure or empty for deleted bucket: ret={ret}, output={output}"

    except:
        cleanup_bucket(s3_client, source_bucket)
    finally:
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_flush_empty_creates_empty_object(s3_client, logging_type):
    """Test that flushing with no pending data creates a size-zero committed log object."""
    source_bucket = gen_bucket_name("empty-flush-src")
    log_bucket = gen_bucket_name("empty-flush-log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        output, ret = admin(['bucket', 'logging', 'flush', '--bucket', source_bucket])
        assert ret == 0, f"Flush failed with return code {ret}"
        assert output.strip()
        flushed_obj_name = parse_flush_output(output)
        time.sleep(2)

        response = s3_client.list_objects_v2(Bucket=log_bucket)
        assert 'Contents' in response, "Expected log object after flush"
        all_objects = {obj['Key']: obj['Size'] for obj in response['Contents']}
        assert flushed_obj_name in all_objects, f"'{flushed_obj_name}' not in log bucket: {list(all_objects.keys())}"
        assert all_objects[flushed_obj_name] == 0, f"Expected size 0, got {all_objects[flushed_obj_name]}"

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_logging_config_update_prefix(s3_client, logging_type):
    """Test that updating logging prefix is reflected in config and log objects."""
    source_bucket = gen_bucket_name("update-prefix-src")
    log_bucket = gen_bucket_name("update-prefix-log")

    old_prefix = f'{source_bucket}/'
    new_prefix = "new-prefix/"

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret == 0
        assert output.strip()
        assert parse_logging_config(output)['targetPrefix'] == old_prefix

        assert enable_bucket_logging(s3_client, source_bucket, log_bucket, prefix=new_prefix, logging_type=logging_type)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret == 0
        assert output.strip()
        assert parse_logging_config(output)['targetPrefix'] == new_prefix

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_logging_config_change_key_format(s3_client, logging_type):
    """Test that changing obj_key_format implicitly flushes pending records to the same log bucket."""
    source_bucket = gen_bucket_name("format-change-src")
    log_bucket = gen_bucket_name("format-change-log")

    try:
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket, logging_type)

        upload_test_objects(s3_client, source_bucket)

        logging_enabled = {
            'TargetBucket': log_bucket,
            'TargetPrefix': f'{source_bucket}/',
            'TargetObjectKeyFormat': {
                'PartitionedPrefix': {
                    'PartitionDateSource': 'DeliveryTime'
                }
            }
        }
        if logging_type == 'Journal':
            logging_enabled['LoggingType'] = 'Journal'
        s3_client.put_bucket_logging(Bucket=source_bucket, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled
        })
        time.sleep(2)

        output, ret = admin(['bucket', 'logging', 'info', '--bucket', source_bucket])
        assert ret == 0
        config = parse_logging_config(output)
        assert config['targetBucket'] == log_bucket

        response = s3_client.list_objects_v2(Bucket=log_bucket)
        assert 'Contents' in response, "Log bucket should have implicitly flushed records after format change"
        log_objects = response['Contents']
        assert len(log_objects) > 0, "Expected at least one flushed log object"
        verify_log_object_content(s3_client, log_bucket, log_objects[0]['Key'], source_bucket)

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


@pytest.mark.basic_test
def test_logging_commands_unconfigured_bucket(s3_client):
    """Test logging commands on a bucket without logging configured."""
    bucket = gen_bucket_name("unconfigured")

    try:
        s3_client.create_bucket(Bucket=bucket)

        for cmd in ['list', 'info', 'flush']:
            output, ret = admin(['bucket', 'logging', cmd, '--bucket', bucket])
            assert ret == 0, f"{cmd} failed on unconfigured bucket: ret={ret}"
            assert not output.strip(), f"{cmd} on unconfigured bucket should produce empty stdout: {output}"

    finally:
        cleanup_bucket(s3_client, bucket)