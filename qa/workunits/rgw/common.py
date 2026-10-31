#!/usr/bin/env python3

import errno
import io
import subprocess
import logging as log
import boto3
import boto3.s3.transfer
import botocore.exceptions
import random
import json
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

def object_stat(bucket_name, object_key):
    """Run radosgw-admin object stat and return parsed JSON."""
    out = exec_cmd(
        f'radosgw-admin object stat --bucket={bucket_name} --object={object_key}'
    )
    # some attrs (e.g. crypt.keysel) contain raw binary that isn't valid UTF-8
    if isinstance(out, bytes):
        out = out.decode('utf-8', errors='replace')
    return json.loads(out)

def get_compression_type(stat):
    """
    Extract the compression_type from object stat output.
    Returns None if the object is not compressed.
    """
    compression = stat.get('compression')
    if compression is None:
        return None
    ct = compression.get('compression_type', 'none')
    if ct.lower() == 'none':
        return None
    return ct.lower()

def get_storage_class(stat):
    """
    Extract the storage class from object stat output.
    The storage class attr lives in attrs['user.rgw.storage_class'].
    If absent, the object is in the STANDARD storage class.
    """
    attrs = stat.get('attrs', {})
    sc = attrs.get('user.rgw.storage_class', '')
    # The value may be a raw string possibly with trailing null bytes
    sc = sc.strip().strip('\x00')
    if not sc:
        return 'STANDARD'
    return sc

def get_crypt_mode(stat):
    """
    Extract the encryption mode from object stat output.
    Returns None if the object is not encrypted.
    """
    attrs = stat.get('attrs', {})
    mode = attrs.get('user.rgw.crypt.mode', '')
    mode = mode.strip().strip('\x00')
    return mode if mode else None

def get_crypt_attr_raw(bucket_name, object_key, name):
    """
    Read a crypt attr whole, straight off the object's head rados object.

    Object stat can't be used where the exact bytes matter: it truncates
    an attr at the first null byte.
    """
    out = exec_cmd(f'radosgw-admin object manifest --bucket={bucket_name}'
                   f' --object={object_key}')
    # the head object is always the first entry
    head = json.loads(out)['objects'][0]['raw_obj']
    return exec_cmd(f'rados -p {head["pool"]} getxattr {head["oid"]}'
                    f' user.rgw.crypt.{name}')

def make_compressible_body(size_bytes):
    """Generate compressible data of the requested size."""
    pattern = b'The quick brown fox jumps over the lazy dog. '
    repeats = (size_bytes // len(pattern)) + 1
    return (pattern * repeats)[:size_bytes]

MULTIPART_THRESHOLD = 8 * 1024 * 1024

def upload_object(client, bucket_name, object_key, body, extra_args):
    """Upload body, using multipart above MULTIPART_THRESHOLD."""
    if len(body) > MULTIPART_THRESHOLD:
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=MULTIPART_THRESHOLD,
        )
        client.upload_fileobj(io.BytesIO(body), bucket_name, object_key,
                              ExtraArgs=extra_args,
                              Config=transfer_config)
    else:
        client.put_object(Bucket=bucket_name, Key=object_key, Body=body,
                          **extra_args)

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

