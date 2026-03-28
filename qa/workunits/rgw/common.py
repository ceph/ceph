#!/usr/bin/env python3

import errno
import subprocess
import logging as log
import boto3
import botocore.exceptions
import random
import json
import os
import tempfile
from time import sleep

log.basicConfig(format = '%(message)s', level=log.DEBUG)
log.getLogger('botocore').setLevel(log.CRITICAL)
log.getLogger('boto3').setLevel(log.CRITICAL)
log.getLogger('urllib3').setLevel(log.CRITICAL)

def exec_cmd(cmd, wait = True, **kwargs):
    check_retcode = kwargs.pop('check_retcode', True)
    kwargs['shell'] = True
    kwargs['stdout'] = subprocess.PIPE
    proc = subprocess.Popen(cmd, **kwargs)
    log.info(proc.args)
    if wait:
        out, _ = proc.communicate()
        if check_retcode:
            assert(proc.returncode == 0)
            return out
        return (out, proc.returncode)
    return ''
    
def create_user(uid, display_name, access_key, secret_key):
    _, ret = exec_cmd(f'radosgw-admin user create --uid {uid} --display-name "{display_name}" --access-key {access_key} --secret {secret_key}', check_retcode=False)
    assert(ret == 0 or errno.EEXIST)
    
def boto_connect(access_key, secret_key, config=None):
    def try_connect(portnum, ssl, proto):
        endpoint = proto + '://localhost:' + portnum
        conn = boto3.resource('s3',
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              use_ssl=ssl,
                              endpoint_url=endpoint,
                              verify=False,
                              config=config,
                              )
        try:
            list(conn.buckets.limit(1)) # just verify we can list buckets
        except botocore.exceptions.ConnectionError as e:
            print(e)
            raise
        print('connected to', endpoint)
        return conn
    try:
        return try_connect('80', False, 'http')
    except botocore.exceptions.ConnectionError:
        try: # retry on non-privileged http port
            return try_connect('8000', False, 'http')
        except botocore.exceptions.ConnectionError:
            # retry with ssl
            return try_connect('443', True, 'https')

def put_objects(bucket, key_list):
    objs = []
    for key in key_list:
        o = bucket.put_object(Key=key, Body=b"some_data")
        objs.append((o.key, o.version_id))
    return objs

def create_unlinked_objects(conn, bucket, key_list):
    # creates an unlinked/unlistable object for each key in key_list
    
    object_versions = []
    try:
        exec_cmd('ceph config set client rgw_debug_inject_set_olh_err 2')
        exec_cmd('ceph config set client rgw_debug_inject_olh_cancel_modification_err true')
        sleep(1)
        for key in key_list:
            tag = str(random.randint(0, 1_000_000))
            try:
                bucket.put_object(Key=key, Body=b"some_data", Metadata = {
                    'tag': tag,
                })
            except Exception as e:
                log.debug(e)
            out = exec_cmd(f'radosgw-admin bi list --bucket {bucket.name} --object {key}')
            instance_entries = filter(
                lambda x: x['type'] == 'instance',
                json.loads(out.replace(b'\x80', b'0x80')))
            found = False
            for ie in instance_entries:
                instance_id = ie['entry']['instance']
                ov = conn.ObjectVersion(bucket.name, key, instance_id).head()
                if ov['Metadata'] and ov['Metadata']['tag'] == tag:
                    object_versions.append((key, instance_id))
                    found = True
                    break
            if not found:
                raise Exception(f'failed to create unlinked object for key={key}')
    finally:
        exec_cmd('ceph config rm client rgw_debug_inject_set_olh_err')
        exec_cmd('ceph config rm client rgw_debug_inject_olh_cancel_modification_err')
    return object_versions


def get_bucket_index_info(bucket_name):
    """Get the bucket index pool, bucket ID, and number of shards."""
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {bucket_name}')
    stats = json.loads(out)
    bucket_id = stats['id']
    num_shards = stats.get('num_shards', 1)
    index_pool = 'default.rgw.buckets.index'
    return index_pool, bucket_id, num_shards


def create_orphaned_list_entries(conn, bucket, key_list):
    """
    Creates orphaned list entries for each key in key_list.
    An orphaned list entry is a plain (list) entry WITHOUT a corresponding
    instance entry. This simulates the corruption scenario where LC fails
    with ENOENT because bi_get_instance() cannot find the instance entry.

    Steps:
    1. Create a normal object (creates list + instance + olh entries)
    2. Delete the object via S3 (creates delete marker)
    3. Use rados to surgically remove the instance and olh entries directly

    Returns list of (key, version_id) tuples representing orphaned entries.
    """
    orphaned_entries = []
    index_pool, bucket_id, num_shards = get_bucket_index_info(bucket.name)

    for key in key_list:
        # Create object and then delete to create a delete marker
        bucket.put_object(Key=key, Body=b"orphan_test_data")
        bucket.Object(key).delete()

        # Get the bi list to find the delete marker's instance entry
        out = exec_cmd(f'radosgw-admin bi list --bucket {bucket.name} --object {key}')
        entries = json.loads(out.replace(b'\x80', b'0x80'))

        # Find ALL instance entries for this key
        # We need to remove all of them to create orphaned list entry
        FLAG_DELETE_MARKER = 0x4
        all_instances = []
        delete_marker_instance = None
        for entry in entries:
            if entry['type'] == 'instance':
                ent = entry['entry']
                instance_id = ent['instance']
                all_instances.append(instance_id)
                flags = ent.get('flags', 0)
                if (flags & FLAG_DELETE_MARKER) != 0:
                    delete_marker_instance = instance_id
                    log.debug(f'Found delete marker: {ent["name"]}:{instance_id} flags={flags}')

        if not delete_marker_instance:
            log.error(f'Could not identify delete marker instance for key={key}, skipping')
            continue

        orphaned_entries.append((key, delete_marker_instance))

        # Now, we need to remove these instance entries and the OLH entry
        # to create an orphaned list entry state
        olh_key_bytes = b'\x801001_' + key.encode()

        # Try all shards; object could be in any shard
        for shard_num in range(num_shards):
            shard_oid = f'.dir.{bucket_id}.{shard_num}'

            # Remove ALL instance entries for this key
            for instance_id in all_instances:
                instance_key_bytes = b'\x801000_' + key.encode() + b'\x00i' + instance_id.encode()
                with tempfile.NamedTemporaryFile(delete=False) as f:
                    f.write(instance_key_bytes)
                    instance_key_file = f.name
                try:
                    exec_cmd(f"rados -p {index_pool} rmomapkey {shard_oid} --omap-key-file {instance_key_file}",
                             check_retcode=False)
                finally:
                    os.unlink(instance_key_file)

            # Remove OLH entry
            with tempfile.NamedTemporaryFile(delete=False) as f:
                f.write(olh_key_bytes)
                olh_key_file = f.name
            try:
                exec_cmd(f"rados -p {index_pool} rmomapkey {shard_oid} --omap-key-file {olh_key_file}",
                         check_retcode=False)
            finally:
                os.unlink(olh_key_file)

    # Verify orphaned state - should only have plain entries now
    for key, instance in orphaned_entries:
        out = exec_cmd(f'radosgw-admin bi list --bucket {bucket.name} --object {key}')
        entries = json.loads(out.replace(b'\x80', b'0x80'))
        entry_types = [e['type'] for e in entries]
        if 'instance' in entry_types or 'olh' in entry_types:
            log.error(f'Key {key} still has instance/olh entries after removal attempt')
        else:
            log.info(f'Successfully created orphaned list entry for {key}:{instance}')

    return orphaned_entries
