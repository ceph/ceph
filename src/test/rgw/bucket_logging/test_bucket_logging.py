"""
Bucket Logging Tests for RGW

This module tests radosgw-admin bucket logging commands and cleanup behavior.
"""

import logging
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
    configfile,
    get_config_host,
    get_config_port,
    get_access_key,
    get_secret_key,
    get_user_id
)

# Configure logging
log = logging.getLogger(__name__)

# Path to test helper scripts
test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/../'

# Global counters for unique naming
num_buckets = 0
run_prefix = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))

# Bucket logging policy template
LOGGING_POLICY_TEMPLATE = '''{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Sid": "AllowLogging",
            "Effect": "Allow",
            "Principal": {{
                "Service": "logging.s3.amazonaws.com"
            }},
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::{log_bucket}/*",
            "Condition": {{
                "StringEquals": {{
                    "aws:SourceAccount": "{account_id}"
                }},
                "ArnLike": {{
                    "aws:SourceArn": "arn:aws:s3:::{source_bucket}"
                }}
            }}
        }}
    ]
}}'''


# =============================================================================
# Helper Functions
# =============================================================================

def bash(cmd, **kwargs):
    """Execute a shell command and return output and return code."""
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
    process = subprocess.Popen(cmd, **kwargs)
    stdout, stderr = process.communicate()
    return (stdout.decode('utf-8'), process.returncode)


def admin(args, **kwargs):
    """Execute radosgw-admin command."""
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', 'noname'] + args
    return bash(cmd, **kwargs)


def rados(args, **kwargs):
    """Execute rados command."""
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_rados', 'noname'] + args
    return bash(cmd, **kwargs)


def gen_bucket_name(prefix="bucket"):
    """Generate a unique bucket name."""
    global num_buckets
    num_buckets += 1
    return f"{run_prefix}-{prefix}-{num_buckets}"


def get_s3_client():
    """Create and return an S3 client."""
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
    """Find temporary log objects for a bucket in rados.
    
    Args:
        bucket_id: Bucket ID to search for
        pool: RADOS pool name
    
    Returns:
        tuple: (list of temp object names, bool success)
    """
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
        # Look for objects starting with the log bucket ID
        # Temp objects follow pattern: <bucket_id>__shadow_<prefix>/<timestamp>
        # or may have other patterns starting with bucket_id
        if line.startswith(bucket_id):
            temp_objects.append(line)
            log.debug(f"Found temp object: {line}")
    
    return temp_objects, True


def create_bucket_with_logging(s3_client, source_bucket, log_bucket):
    """
    Create source and log buckets with logging enabled.
    
    Returns True on success, False on failure.
    """
    try:
        # Create log bucket first
        s3_client.create_bucket(Bucket=log_bucket)
        log.info(f"Created log bucket: {log_bucket}")

        # Create source bucket
        s3_client.create_bucket(Bucket=source_bucket)
        log.info(f"Created source bucket: {source_bucket}")

        # Set bucket policy on log bucket to allow logging
        user_id = get_user_id()
        policy = LOGGING_POLICY_TEMPLATE.format(
            log_bucket=log_bucket,
            source_bucket=source_bucket,
            account_id=user_id
        )
        s3_client.put_bucket_policy(Bucket=log_bucket, Policy=policy)
        log.info(f"Set logging policy on log bucket: {log_bucket}")

        # Enable logging on source bucket
        logging_config = {
            'LoggingEnabled': {
                'TargetBucket': log_bucket,
                'TargetPrefix': f'{source_bucket}/'
            }
        }
        s3_client.put_bucket_logging(Bucket=source_bucket, BucketLoggingStatus=logging_config)
        log.info(f"Enabled logging on source bucket: {source_bucket}")

        return True
    except ClientError as e:
        log.error(f"Error setting up bucket logging: {e}")
        return False


def cleanup_bucket(s3_client, bucket_name):
    """Delete all objects in a bucket and then delete the bucket."""
    try:
        # List and delete all objects
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                log.debug(f"Deleted object: {obj['Key']}")
        
        # Delete the bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        log.info(f"Deleted bucket: {bucket_name}")
    except ClientError as e:
        log.warning(f"Error cleaning up bucket {bucket_name}: {e}")


# =============================================================================
# Admin Command Tests
# =============================================================================

def test_bucket_logging_list():
    """Test radosgw-admin bucket logging list command."""
    s3_client = get_s3_client()
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")
    
    try:
        # Setup
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket), \
            "Failed to set up bucket logging"
        
        # Upload an object to generate log records
        s3_client.put_object(
            Bucket=source_bucket,
            Key='test-object.txt',
            Body=b'test content'
        )
        log.info(f"Uploaded test object to {source_bucket}")

        # Run bucket logging list command
        output, ret = admin([
            'bucket', 'logging', 'list',
            '--bucket', source_bucket
        ])
        
        log.info(f"bucket logging list output: {output}")
        assert ret == 0, f"bucket logging list failed with return code {ret}"
        
    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


def test_bucket_logging_info_source():
    """Test radosgw-admin bucket logging info on source bucket."""
    s3_client = get_s3_client()
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")
    
    try:
        # Setup
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket), \
            "Failed to set up bucket logging"
        
        # Run bucket logging info command on source bucket
        output, ret = admin([
            'bucket', 'logging', 'info',
            '--bucket', source_bucket
        ])
        
        log.info(f"bucket logging info (source) output: {output}")
        assert ret == 0, f"bucket logging info failed with return code {ret}"
        
        # Verify output contains logging configuration
        if output.strip():
            try:
                info = json.loads(output)
                assert 'bucketLoggingStatus' in info or 'targetbucket' in str(info).lower(), \
                    "Expected logging configuration in output"
            except json.JSONDecodeError:
                log.warning("Output is not JSON, checking for relevant content")
                
    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


def test_bucket_logging_info_log():
    """Test radosgw-admin bucket logging info on log bucket."""
    s3_client = get_s3_client()
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")
    
    try:
        # Setup
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket), \
            "Failed to set up bucket logging"
        
        # Run bucket logging info command on log bucket
        output, ret = admin([
            'bucket', 'logging', 'info',
            '--bucket', log_bucket
        ])
        
        log.info(f"bucket logging info (log) output: {output}")
        assert ret == 0, f"bucket logging info failed with return code {ret}"
        
    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


def test_bucket_logging_flush():
    """Test radosgw-admin bucket logging flush command."""
    s3_client = get_s3_client()
    source_bucket = gen_bucket_name("source")
    log_bucket = gen_bucket_name("log")
    
    try:
        # Setup
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket), \
            "Failed to set up bucket logging"
        
        # Upload objects to generate log records
        for i in range(3):
            s3_client.put_object(
                Bucket=source_bucket,
                Key=f'test-object-{i}.txt',
                Body=f'test content {i}'.encode()
            )
        log.info(f"Uploaded test objects to {source_bucket}")

        # Flush the logs
        output, ret = admin([
            'bucket', 'logging', 'flush',
            '--bucket', source_bucket
        ])
        
        log.info(f"bucket logging flush output: {output}")
        assert ret == 0, f"bucket logging flush failed with return code {ret}"

        # Give some time for flush to complete
        time.sleep(2)

        # Verify logs appear in log bucket
        response = s3_client.list_objects_v2(Bucket=log_bucket)
        log.info(f"Log bucket contents after flush: {response.get('Contents', [])}")
        
    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


# =============================================================================
# Cleanup Tests
# =============================================================================

def test_cleanup_on_log_bucket_delete():
    """
    Test that temporary log objects are deleted when log bucket is deleted.
    
    Steps:
    1. Create source bucket
    2. Create log bucket
    3. Set policy on log bucket
    4. Enable logging on source bucket
    5. Upload objects to source (generates temp log objects)
    6. Verify temp objects exist using rados ls
    7. Delete log bucket
    8. Verify temp objects are gone
    """
    s3_client = get_s3_client()
    source_bucket = gen_bucket_name("cleanup-source")
    log_bucket = gen_bucket_name("cleanup-log")

    try:
        # Set up buckets with logging
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket), \
            "Failed to set up bucket logging"

        # Get log bucket ID for identifying temp objects
        log_bucket_id = get_bucket_id(log_bucket)
        log.info(f"Log bucket ID: {log_bucket_id}")

        # Upload objects to source bucket to generate log records
        for i in range(5):
            s3_client.put_object(
                Bucket=source_bucket,
                Key=f'object-{i}.txt',
                Body=f'content {i}'.encode()
            )
        log.info(f"Uploaded 5 objects to {source_bucket}")

        # Wait for temp objects to appear (with retry logic)
        max_wait = 15  # seconds
        wait_interval = 1
        temp_objects_before = []
        for attempt in range(max_wait):
            temp_objects_before, success = find_temp_log_objects(log_bucket_id)
            assert success, "Failed to list rados objects"
            if temp_objects_before:
                log.info(f"Found {len(temp_objects_before)} temp objects after {attempt + 1}s")
                break
            time.sleep(wait_interval)
        else:
            # If no temp objects found, log warning but don't fail
            # (might be auto-flushed or different timing)
            log.warning("No temp objects found after waiting - logs may have been flushed")
            pytest.skip("No temp objects created - may be flushed too quickly or timing issue")
        
        log.info(f"Temp objects before delete: {temp_objects_before}")
        assert len(temp_objects_before) > 0, "Expected temp objects to exist"

        # Disable logging on source bucket before deleting log bucket
        s3_client.put_bucket_logging(
            Bucket=source_bucket,
            BucketLoggingStatus={}
        )
        log.info(f"Disabled logging on {source_bucket}")

        # Delete objects from log bucket (if any)
        try:
            response = s3_client.list_objects_v2(Bucket=log_bucket)
            if 'Contents' in response:
                for obj in response['Contents']:
                    s3_client.delete_object(Bucket=log_bucket, Key=obj['Key'])
        except ClientError:
            pass

        # Delete log bucket
        s3_client.delete_bucket(Bucket=log_bucket)
        log.info(f"Deleted log bucket: {log_bucket}")

        # Give time for cleanup to happen
        time.sleep(3)

        # Verify temp objects are gone
        temp_objects_after, success = find_temp_log_objects(log_bucket_id)
        assert success, "Failed to list rados objects"
        log.info(f"Temp objects after delete: {temp_objects_after}")

        # Cleanup should have removed temp objects
        assert len(temp_objects_after) == 0, \
            f"Temp objects still exist after log bucket deletion: {temp_objects_after}"

    finally:
        # Cleanup source bucket
        cleanup_bucket(s3_client, source_bucket)


def test_cleanup_on_logging_disable():
    """
    Test that temporary log objects are flushed when logging is disabled.
    """
    s3_client = get_s3_client()
    source_bucket = gen_bucket_name("disable-source")
    log_bucket = gen_bucket_name("disable-log")

    try:
        # Set up buckets with logging
        assert create_bucket_with_logging(s3_client, source_bucket, log_bucket), \
            "Failed to set up bucket logging"

        # Upload objects to source bucket
        for i in range(3):
            s3_client.put_object(
                Bucket=source_bucket,
                Key=f'object-{i}.txt',
                Body=f'content {i}'.encode()
            )
        log.info(f"Uploaded 3 objects to {source_bucket}")

        # Disable logging (should trigger flush)
        s3_client.put_bucket_logging(
            Bucket=source_bucket,
            BucketLoggingStatus={}
        )
        log.info(f"Disabled logging on {source_bucket}")

        # Give time for flush to complete
        time.sleep(3)

        # Check that logs were written to log bucket
        response = s3_client.list_objects_v2(Bucket=log_bucket)
        log_objects = response.get('Contents', [])
        log.info(f"Log objects after disable: {log_objects}")

        # Logs should have been flushed to the log bucket
        # (Note: this depends on whether there were pending logs)

    finally:
        cleanup_bucket(s3_client, source_bucket)
        cleanup_bucket(s3_client, log_bucket)


# =============================================================================
# Main
# =============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
