import threading
import logging
import random
import math
import time
import subprocess
import urllib.request
import hashlib
from multiprocessing import Process
import os
import string
import shutil
import pytest
import json
from collections import namedtuple
import boto3
from boto3.s3.transfer import TransferConfig
from dataclasses import dataclass

from . import(
    configfile,
    get_config_host,
    get_config_port,
    get_access_key,
    get_secret_key
)

@dataclass
class Dedup_Stats:
    skip_too_small: int = 0
    skip_too_small_bytes: int = 0
    skip_shared_manifest: int = 0
    skip_singleton: int = 0
    skip_singleton_bytes: int = 0
    skip_src_record: int = 0
    skip_changed_object: int = 0
    corrupted_etag: int = 0
    sha256_mismatch: int = 0
    valid_sha256: int = 0
    invalid_sha256: int = 0
    set_sha256: int = 0
    total_processed_objects: int = 0
    size_before_dedup: int = 0
    #loaded_objects: int = 0
    set_shared_manifest_src : int = 0
    deduped_obj: int = 0
    singleton_obj : int = 0
    unique_obj : int = 0
    dedup_bytes_estimate : int = 0
    duplicate_obj : int = 0
    dup_head_size_estimate : int = 0
    dup_head_size : int = 0
    deduped_obj_bytes : int = 0
    non_default_storage_class_objs_bytes : int = 0
    potential_singleton_obj : int = 0
    potential_unique_obj : int = 0
    potential_duplicate_obj : int = 0
    potential_dedup_space : int = 0

@dataclass
class Dedup_Ratio:
    s3_bytes_before: int = 0
    s3_bytes_after: int = 0
    ratio: float = 0.0

full_dedup_state_was_checked = False
full_dedup_state_disabled = True

# configure logging for the tests module
log = logging.getLogger(__name__)
num_buckets = 0
num_files   = 0
num_users   = 0
num_conns   = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(16))
test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/../'

#-----------------------------------------------
def bash(cmd, **kwargs):
    #log.debug('running command: %s', ' '.join(cmd))
    kwargs['stdout'] = subprocess.PIPE
    process = subprocess.Popen(cmd, **kwargs)
    s = process.communicate()[0].decode('utf-8')
    return (s, process.returncode)

#-----------------------------------------------
def admin(args, **kwargs):
    """ radosgw-admin command """
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', 'noname'] + args
    return bash(cmd, **kwargs)

#-----------------------------------------------
def rados(args, **kwargs):
    """ rados command """
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_rados', 'noname'] + args
    return bash(cmd, **kwargs)

#-----------------------------------------------
def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    bucket_name = run_prefix + '-' + str(num_buckets)
    log.debug("bucket_name=%s", bucket_name);
    return bucket_name

#-----------------------------------------------
def get_buckets(num_buckets):
    bucket_names=[]
    for i in range(num_buckets):
        bucket_name=gen_bucket_name()
        bucket_names.append(bucket_name)

    return bucket_names


#==============================================
g_tenant_connections=[]
g_tenants=[]
g_simple_connection=[]

#-----------------------------------------------
def close_all_connections():
    global g_simple_connection

    for conn in g_simple_connection:
        log.debug("close simple connection")
        conn.close()

    for conn in g_tenant_connections:
        log.debug("close tenant connection")
        conn.close()

#-----------------------------------------------
def get_connections(req_count):
    global g_simple_connection
    conns=[]

    for i in range(min(req_count, len(g_simple_connection))):
        log.debug("recycle existing connection")
        conns.append(g_simple_connection[i])

    if len(conns) < req_count:
        hostname = get_config_host()
        port_no = get_config_port()
        access_key = get_access_key()
        secret_key = get_secret_key()
        if port_no == 443 or port_no == 8443:
            scheme = 'https://'
        else:
            scheme = 'http://'

        for i in range(req_count - len(conns)):
            log.debug("generate new connection")
            client = boto3.client('s3',
                                  endpoint_url=scheme+hostname+':'+str(port_no),
                                  aws_access_key_id=access_key,
                                  aws_secret_access_key=secret_key)
            g_simple_connection.append(client);
            conns.append(client);

    return conns

#-----------------------------------------------
def get_single_connection():
    conns=get_connections(1)
    return conns[0]


#-----------------------------------------------
def another_user(uid, tenant, display_name):
    global num_users
    num_users += 1

    access_key = run_prefix + "_" +str(num_users) + "_" + str(time.time())
    secret_key = run_prefix + "_" +str(num_users) + "_" + str(time.time())

    cmd = ['user', 'create', '--uid', uid, '--tenant', tenant, '--access-key', access_key, '--secret-key', secret_key, '--display-name', display_name]
    result = admin(cmd)
    assert result[1] == 0

    hostname = get_config_host()
    port_no = get_config_port()
    if port_no == 443 or port_no == 8443:
        scheme = 'https://'
    else:
        scheme = 'http://'

    endpoint=scheme+hostname+':'+str(port_no)
    client = boto3.client('s3',
                          endpoint_url=endpoint,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)

    return client

#-------------------------------------------------------------------------------
def gen_connections_multi2(req_count):
    g_tenant_connections=[]
    g_tenants=[]
    global num_conns

    log.debug("gen_connections_multi: Create connection and buckets ...")
    suffix=run_prefix

    tenants=[]
    bucket_names=[]
    conns=[]

    for i in range(min(req_count, len(g_tenants))):
        log.debug("recycle existing tenants connection")
        conns.append(g_tenants_connection[i])
        tenants.append(g_tenants[i])
        # we need to create a new bucket as we remove existing buckets at cleanup
        bucket_name=gen_bucket_name()
        bucket_names.append(bucket_name)
        bucket=conns[i].create_bucket(Bucket=bucket_name)

    if len(conns) < req_count:
        for i in range(req_count - len(conns)):
            num_conns += 1
            user=gen_object_name("user", num_conns) + suffix
            display_name=gen_object_name("display", num_conns) + suffix
            tenant=gen_object_name("tenant", num_conns) + suffix
            g_tenants.append(tenant)
            tenants.append(tenant)
            bucket_name=gen_bucket_name()
            bucket_names.append(bucket_name)
            log.debug("U=%s, T=%s, B=%s", user, tenant, bucket_name);

            conn=another_user(user, tenant, display_name)
            bucket=conn.create_bucket(Bucket=bucket_name)
            g_tenant_connections.append(conn)
            conns.append(conn)

    log.debug("gen_connections_multi: All connection and buckets are set")
    return (tenants, bucket_names, conns)


#-------------------------------------------------------------------------------
def gen_connections_multi(num_tenants):
    global num_conns

    tenants=[]
    bucket_names=[]
    conns=[]
    log.debug("gen_connections_multi: Create connection and buckets ...")
    suffix=run_prefix
    for i in range(0, num_tenants):
        num_conns += 1
        user=gen_object_name("user", num_conns) + suffix
        display_name=gen_object_name("display", num_conns) + suffix
        tenant=gen_object_name("tenant", num_conns) + suffix
        tenants.append(tenant)
        bucket_name=gen_bucket_name()
        bucket_names.append(bucket_name)
        log.debug("U=%s, T=%s, B=%s", user, tenant, bucket_name);

        conn=another_user(user, tenant, display_name)
        bucket=conn.create_bucket(Bucket=bucket_name)
        conns.append(conn)

    log.debug("gen_connections_multi: All connection and buckets are set")
    return (tenants, bucket_names, conns)


#####################
# dedup tests
#####################
OUT_DIR="/tmp/dedup/"
KB=(1024)
MB=(1024*KB)
POTENTIAL_OBJ_SIZE=(64*KB)
RADOS_OBJ_SIZE=(4*MB)
MULTIPART_SIZE=(16*MB)
default_config = TransferConfig(multipart_threshold=MULTIPART_SIZE, multipart_chunksize=MULTIPART_SIZE)
ETAG_ATTR="user.rgw.etag"
POOLNAME="default.rgw.buckets.data"

#-------------------------------------------------------------------------------
def write_file(filename, size):
    full_filename = OUT_DIR + filename

    fout = open(full_filename, "wb")
    fout.write(os.urandom(size))
    fout.close()


#-------------------------------------------------------------------------------
def print_size(caller, size):
    if (size < MB):
        log.debug("%s::size=%.2f KiB (%d Bytes)", caller, size/KB, size)
    else:
        log.debug("%s::size=%.2f MiB", caller, size/MB)


#-------------------------------------------------------------------------------
def write_random(files, size, min_copies_count=1, max_copies_count=4):
    global num_files
    assert(max_copies_count <= 4)
    num_files += 1
    filename = "OBJ_" + str(num_files) + str(size)
    copies_count=random.randint(min_copies_count, max_copies_count)
    files.append((filename, size, copies_count))
    write_file(filename, size)


#-------------------------------------------------------------------------------
def gen_files_fixed_copies(files, count, size, copies_count):
    global num_files

    for i in range(0, count):
        num_files += 1
        filename = "OBJ_" + str(num_files)

        files.append((filename, size, copies_count))
        log.debug("gen_files_fixed_size:%s, %d, %d", filename, size, copies_count)
        write_file(filename, size)


#-------------------------------------------------------------------------------
def gen_files_fixed_size(files, count, size, max_copies_count=4):
    global num_files

    for i in range(0, count):
        copies_count=random.randint(1, max_copies_count)
        num_files += 1
        filename = "OBJ_" + str(num_files)

        files.append((filename, size, copies_count))
        log.debug("gen_files_fixed_size:%s, %d, %d", filename, size, copies_count)
        write_file(filename, size)


#-------------------------------------------------------------------------------
def gen_files_in_range(files, count, min_size, max_size, alignment=RADOS_OBJ_SIZE):
    assert(min_size <= max_size)
    if min_size == 0:
        assert max_size > 1024

    size_range = max_size - min_size
    size=0
    for i in range(0, count):
        size = min_size + random.randint(1, size_range-1)
        if size == 0:
            size = 1024 * random.randint(1, 1024)

        log.debug("gen_files_in_range::size=%.2f MiB", size/MB)
        size_aligned = size - (size % alignment)
        if size_aligned == 0:
            size_aligned += alignment
            if size_aligned == 0:
                size_aligned = 4*MB

        assert(size)
        assert(size_aligned)
        # force dedup by setting min_copies_count to 2
        write_random(files, size_aligned, 2, 3)
        write_random(files, size, 1, 3)


#-------------------------------------------------------------------------------
def gen_files(files, start_size, factor, max_copies_count=4):
    size = start_size
    for i in range(1, factor+1):
        size2 = size + random.randint(1, size-1)
        write_random(files, size, 1, max_copies_count)
        write_random(files, size2, 1, max_copies_count)
        size  = size * 2;


#-------------------------------------------------------------------------------
def count_space_in_all_buckets():
    result = rados(['df'])
    assert result[1] == 0
    log.debug("=============================================")
    for line in result[0].splitlines():
        if line.startswith(POOLNAME):
            log.debug(line[:45])
        elif line.startswith("POOL_NAME"):
            log.debug(line[:45])
            log.debug("=============================================")


#-------------------------------------------------------------------------------
def count_objects_in_bucket(bucket_name, conn):
    max_keys=1000
    marker=""
    obj_count=0
    while True:
        log.debug("bucket_name=%s", bucket_name)
        listing=conn.list_objects(Bucket=bucket_name, Marker=marker, MaxKeys=max_keys)
        if 'Contents' not in listing or len(listing['Contents'])== 0:
            return 0

        obj_count += len(listing['Contents'])

        if listing['IsTruncated']:
            marker=listing['NextMarker']
            log.debug("marker=%s, obj_count=%d", marker, obj_count)
            continue
        else:
            return obj_count


#-------------------------------------------------------------------------------
def count_object_parts_in_all_buckets(verbose=False):
    result = rados(['lspools'])
    assert result[1] == 0
    found=False
    for pool in result[0].split():
        if pool == POOLNAME:
            found=True
            log.debug("Pool %s was found", POOLNAME)
            break

    if found == False:
        log.debug("Pool %s doesn't exists!", POOLNAME)
        return 0

    result = rados(['ls', '-p ', POOLNAME])
    assert result[1] == 0

    names=result[0].split()
    count = 0
    for name in names:
        #log.debug(name)
        count = count + 1

    if verbose:
        log.debug("Pool has %d rados objects", count)

    return count


#-------------------------------------------------------------------------------
def cleanup_local():
    if os.path.isdir(OUT_DIR):
        log.debug("Removing old directory " + OUT_DIR)
        shutil.rmtree(OUT_DIR)
        return True
    else:
        return False


#-------------------------------------------------------------------------------
def delete_bucket_with_all_objects(bucket_name, conn):
    max_keys=1000
    marker=""
    obj_count=0
    while True:
        listing=conn.list_objects(Bucket=bucket_name, Marker=marker, MaxKeys=max_keys)
        if 'Contents' not in listing or len(listing['Contents'])== 0:
            log.debug("Bucket '%s' is empty, skipping...", bucket_name)
            return

        objects=[]
        for obj in listing['Contents']:
            log.debug(obj['Key'])
            objects.append({'Key': obj['Key']})

        obj_count += len(objects)
        # delete objects from the bucket
        conn.delete_objects(Bucket=bucket_name, Delete={'Objects':objects})
        if listing['IsTruncated']:
            marker=listing['NextMarker']
            log.debug("marker=%s, obj_count=%d", marker, obj_count)
            continue
        else:
            break

    log.debug("Removing Bucket '%s', obj_count=%d", bucket_name, obj_count)
    conn.delete_bucket(Bucket=bucket_name)

#-------------------------------------------------------------------------------
def verify_pool_is_empty():
    result = admin(['gc', 'process', '--include-all'])
    assert result[1] == 0
    assert count_object_parts_in_all_buckets() == 0


#-------------------------------------------------------------------------------
def cleanup(bucket_name, conn):
    if cleanup_local():
        log.debug("delete_all_objects for bucket <%s>",bucket_name)
        delete_bucket_with_all_objects(bucket_name, conn)

    verify_pool_is_empty()


#-------------------------------------------------------------------------------
def cleanup_all_buckets(bucket_names, conns):
    if cleanup_local():
        for (bucket_name, conn) in zip(bucket_names, conns):
            log.debug("delete_all_objects for bucket <%s>",bucket_name)
            delete_bucket_with_all_objects(bucket_name, conn)

    verify_pool_is_empty()


#-------------------------------------------------------------------------------
def gen_object_name(filename, index):
    return filename + "_" + str(index)


#-------------------------------------------------------------------------------
def calc_rados_obj_count(num_copies, obj_size, config):
    rados_obj_count = 0
    threshold = config.multipart_threshold
    if threshold < RADOS_OBJ_SIZE:
        if obj_size < 1*MB:
            rados_obj_count = 2
        else:
            rados_obj_count = math.ceil(obj_size/threshold)
            if obj_size >= threshold:
                rados_obj_count += 1

        log.debug(">>>obj_size=%.2f MiB, rados_obj_count=%d, num_copies=%d",
                  float(obj_size)/MB, rados_obj_count, num_copies)
        return rados_obj_count

    # split the object into parts
    full_parts_count = (obj_size // threshold)
    if full_parts_count :
        # each part is written separately so the last part can be incomplete
        rados_objs_per_full_part = math.ceil(threshold/RADOS_OBJ_SIZE)
        rados_obj_count = (full_parts_count * rados_objs_per_full_part)
        # add one part for an empty head-object
        rados_obj_count += 1

    partial_part = (obj_size % threshold)
    if partial_part:
        count = math.ceil(partial_part/RADOS_OBJ_SIZE)
        rados_obj_count += count

    log.debug("obj_size=%d/%.2f MiB, rados_obj_count=%d, num_copies=%d",
              obj_size, float(obj_size)/MB, rados_obj_count, num_copies)
    return rados_obj_count


#-------------------------------------------------------------------------------
def calc_dedupable_space(obj_size, config):
    dup_head_size=0
    threshold = config.multipart_threshold
    # Objects with size bigger than MULTIPART_SIZE are uploaded as multi-part
    # multi-part objects got a zero size Head objects
    if obj_size >= threshold:
        dedupable_space = obj_size
    elif obj_size > RADOS_OBJ_SIZE:
        dedupable_space = obj_size - RADOS_OBJ_SIZE
        dup_head_size = RADOS_OBJ_SIZE
    else:
        dedupable_space = 0

    log.debug("obj_size=%.2f MiB, dedupable_space=%.2f MiB",
              float(obj_size)/MB, float(dedupable_space)/MB)
    return (dedupable_space, dup_head_size)

BLOCK_SIZE=4096
#-------------------------------------------------------------------------------
def calc_on_disk_byte_size(byte_size):
    return (((byte_size+BLOCK_SIZE-1)//BLOCK_SIZE)*BLOCK_SIZE)


#-------------------------------------------------------------------------------
def calc_expected_stats(dedup_stats, obj_size, num_copies, config):
    dups_count = (num_copies - 1)
    on_disk_byte_size = calc_on_disk_byte_size(obj_size)
    log.debug("obj_size=%d, on_disk_byte_size=%d", obj_size, on_disk_byte_size)
    threshold = config.multipart_threshold
    dedup_stats.skip_shared_manifest = 0
    dedup_stats.size_before_dedup += (on_disk_byte_size * num_copies)
    if on_disk_byte_size <= RADOS_OBJ_SIZE and threshold > RADOS_OBJ_SIZE:
        dedup_stats.skip_too_small += num_copies
        dedup_stats.skip_too_small_bytes += (on_disk_byte_size * num_copies)

        if on_disk_byte_size >= POTENTIAL_OBJ_SIZE:
            if num_copies == 1:
                dedup_stats.potential_singleton_obj += 1
            else:
                dedup_stats.potential_unique_obj += 1
                dedup_stats.potential_duplicate_obj += dups_count
                dedup_stats.potential_dedup_space += (on_disk_byte_size * dups_count)

        return

    dedup_stats.total_processed_objects += num_copies
    #dedup_stats.loaded_objects += num_copies

    if num_copies == 1:
        dedup_stats.singleton_obj += 1
        dedup_stats.skip_singleton += 1
        dedup_stats.skip_singleton_bytes += on_disk_byte_size
    else:
        dedup_stats.skip_src_record += 1
        dedup_stats.set_shared_manifest_src += 1
        dedup_stats.set_sha256 += num_copies
        dedup_stats.invalid_sha256 += num_copies
        dedup_stats.unique_obj += 1
        dedup_stats.duplicate_obj += dups_count
        dedup_stats.deduped_obj += dups_count
        ret=calc_dedupable_space(on_disk_byte_size, config)
        deduped_obj_bytes=ret[0]
        dup_head_size=ret[1]
        dedup_stats.deduped_obj_bytes += (deduped_obj_bytes * dups_count)
        dedup_stats.dup_head_size += (dup_head_size * dups_count)
        dedup_stats.dup_head_size_estimate += (dup_head_size * dups_count)
        deduped_block_bytes=((deduped_obj_bytes+BLOCK_SIZE-1)//BLOCK_SIZE)*BLOCK_SIZE
        dedup_stats.dedup_bytes_estimate += (deduped_block_bytes * dups_count)


#-------------------------------------------------------------------------------
def calc_expected_results(files, config):
    duplicated_tail_objs=0
    rados_objects_total=0

    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        assert(obj_size)

        if num_copies > 0:
            log.debug("calc_expected_results::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
            rados_obj_count=calc_rados_obj_count(num_copies, obj_size, config)
            rados_objects_total += (rados_obj_count * num_copies)
            duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))

    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.debug("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)

    return expected_rados_obj_count_post_dedup


#-------------------------------------------------------------------------------
def upload_objects(bucket_name, files, indices, conn, config, check_obj_count=True):
    dedup_stats = Dedup_Stats()
    total_space=0
    duplicated_space=0
    duplicated_tail_objs=0
    rados_objects_total=0
    s3_objects_total=0

    for (f, idx) in zip(files, indices):
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        assert(obj_size)
        calc_expected_stats(dedup_stats, obj_size, num_copies, config)
        total_space += (obj_size * num_copies)
        ret=calc_dedupable_space(obj_size, config)
        dedupable_space=ret[0]
        dup_head_size=ret[1]
        duplicated_space += ((num_copies-1) * dedupable_space)
        rados_obj_count=calc_rados_obj_count(num_copies, obj_size, config)
        rados_objects_total += (rados_obj_count * num_copies)
        duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))
        log.debug("upload_objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
        s3_objects_total += num_copies
        if s3_objects_total and (s3_objects_total % 1000 == 0):
            log.debug("%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
                     s3_objects_total, rados_objects_total, total_space/MB)
        for i in range(idx, num_copies):
            key = gen_object_name(filename, i)
            #log.debug("upload_file %s/%s with crc32", bucket_name, key)
            conn.upload_file(OUT_DIR + filename, bucket_name, key, Config=config, ExtraArgs={'ChecksumAlgorithm': 'crc32'})

    log.debug("==========================================")
    log.debug("Summery:\n%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
             s3_objects_total, rados_objects_total, total_space/MB)
    log.debug("Based on calculation we should have %d rados objects", rados_objects_total)
    log.debug("Based on calculation we should have %d duplicated tail objs", duplicated_tail_objs)
    log.debug("Based on calculation we should have %.2f MiB total in pool", total_space/MB)
    log.debug("Based on calculation we should have %.2f MiB duplicated space in pool", duplicated_space/MB)

    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.debug("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)
    expcted_space_post_dedup=(total_space-duplicated_space)
    log.debug("Post dedup expcted data in pool = %.2f MiB", expcted_space_post_dedup/MB)
    if check_obj_count:
        assert rados_objects_total == count_object_parts_in_all_buckets()

    expected_results=(expected_rados_obj_count_post_dedup, expcted_space_post_dedup)
    return (expected_rados_obj_count_post_dedup, dedup_stats, s3_objects_total)


#-------------------------------------------------------------------------------
def upload_objects_multi(files, conns, bucket_names, indices, config, check_obj_count=True):
    max_tenants=len(conns)
    dedup_stats = Dedup_Stats()
    total_space=0
    duplicated_space=0
    duplicated_tail_objs=0
    rados_objects_total=0
    s3_objects_total=0
    for (f, idx) in zip(files, indices):
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        assert(obj_size)
        calc_expected_stats(dedup_stats, obj_size, num_copies, config)
        total_space += (obj_size * num_copies)
        ret=calc_dedupable_space(obj_size, config)
        dedupable_space=ret[0]
        dup_head_size=ret[1]
        duplicated_space += ((num_copies-1) * dedupable_space)
        rados_obj_count=calc_rados_obj_count(num_copies, obj_size, config)
        rados_objects_total += (rados_obj_count * num_copies)
        duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))
        log.debug("upload_objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
        s3_objects_total += num_copies
        if s3_objects_total and (s3_objects_total % 1000 == 0):
            log.debug("%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
                     s3_objects_total, rados_objects_total, total_space/MB)
        for i in range(idx, num_copies):
            ten_id = i % max_tenants
            key = gen_object_name(filename, i)
            conns[ten_id].upload_file(OUT_DIR + filename, bucket_names[ten_id], key, Config=config)
            log.debug("upload_objects::<%s/%s>", bucket_names[ten_id], key)

    log.debug("==========================================")
    log.debug("Summery:%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
             s3_objects_total, rados_objects_total, total_space/MB)
    log.debug("Based on calculation we should have %d rados objects", rados_objects_total)
    log.debug("Based on calculation we should have %d duplicated tail objs", duplicated_tail_objs)
    log.debug("Based on calculation we should have %.2f MiB total in pool", total_space/MB)
    log.debug("Based on calculation we should have %.2f MiB duplicated space in pool", duplicated_space/MB)

    s3_object_count=0
    for (bucket_name, conn) in zip(bucket_names, conns):
        s3_object_count += count_objects_in_bucket(bucket_name, conn)

    log.debug("bucket listings reported a total of %d s3 objects", s3_object_count)
    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.debug("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)
    expcted_space_post_dedup=(total_space-duplicated_space)
    log.debug("Post dedup expcted data in pool = %.2f MiB", expcted_space_post_dedup/MB)
    if check_obj_count:
        assert rados_objects_total == count_object_parts_in_all_buckets()
        assert (s3_object_count == s3_objects_total)

    expected_results=(expected_rados_obj_count_post_dedup, expcted_space_post_dedup)
    return (expected_rados_obj_count_post_dedup, dedup_stats, s3_objects_total)


#---------------------------------------------------------------------------
def proc_upload(proc_id, num_procs, files, conn, bucket_name, indices, config):
    count=0
    for (f, idx) in zip(files, indices):
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        assert(obj_size)
        log.debug("upload_objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
        for i in range(idx, num_copies):
            target_proc = (count % num_procs)
            count += 1
            if (proc_id == target_proc):
                key = gen_object_name(filename, i)
                conn.upload_file(OUT_DIR+filename, bucket_name, key, Config=config)
                log.debug("[%d]upload_objects::<%s/%s>", proc_id, bucket_name, key)


#---------------------------------------------------------------------------
def procs_upload_objects(files, conns, bucket_names, indices, config, check_obj_count=True):
    num_procs=len(conns)
    proc_list=list()
    for idx in range(num_procs):
        # Seems the processes are much faster than threads (probably due to python gil)
        p=Process(target=proc_upload,
                  args=(idx, num_procs, files, conns[idx], bucket_names[idx], indices, config))
        proc_list.append(p)
        proc_list[idx].start()

    dedup_stats = Dedup_Stats()
    total_space=0
    duplicated_space=0
    duplicated_tail_objs=0
    rados_objects_total=0
    s3_objects_total=0
    for (f, idx) in zip(files, indices):
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        assert(obj_size)
        calc_expected_stats(dedup_stats, obj_size, num_copies, config)
        total_space += (obj_size * num_copies)
        ret=calc_dedupable_space(obj_size, config)
        dedupable_space=ret[0]
        dup_head_size=ret[1]
        duplicated_space += ((num_copies-1) * dedupable_space)
        rados_obj_count=calc_rados_obj_count(num_copies, obj_size, config)
        rados_objects_total += (rados_obj_count * num_copies)
        duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))
        log.debug("upload_objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
        s3_objects_total += num_copies

    # wait for all worker proc to join
    for idx in range(num_procs):
        proc_list[idx].join()

    log.debug("==========================================")
    log.debug("Summery:%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
             s3_objects_total, rados_objects_total, total_space/MB)
    log.debug("Based on calculation we should have %d rados objects", rados_objects_total)
    log.debug("Based on calculation we should have %d duplicated tail objs", duplicated_tail_objs)
    log.debug("Based on calculation we should have %.2f MiB total in pool", total_space/MB)
    log.debug("Based on calculation we should have %.2f MiB duplicated space in pool", duplicated_space/MB)

    s3_object_count=0
    for (bucket_name, conn) in zip(bucket_names, conns):
        s3_object_count += count_objects_in_bucket(bucket_name, conn)

    log.debug("bucket listings reported a total of %d s3 objects", s3_object_count)
    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.debug("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)
    expcted_space_post_dedup=(total_space-duplicated_space)
    log.debug("Post dedup expcted data in pool = %.2f MiB", expcted_space_post_dedup/MB)
    if check_obj_count:
        assert rados_objects_total == count_object_parts_in_all_buckets()
        assert (s3_object_count == s3_objects_total)

    expected_results=(expected_rados_obj_count_post_dedup, expcted_space_post_dedup)
    return (expected_rados_obj_count_post_dedup, dedup_stats, s3_objects_total)


#-------------------------------------------------------------------------------
def verify_objects(bucket_name, files, conn, expected_results, config):
    tempfile = OUT_DIR + "temp"
    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        log.debug("comparing file=%s, size=%d, copies=%d", filename, obj_size, num_copies)
        for i in range(0, num_copies):
            key = gen_object_name(filename, i)
            #log.debug("download_file(%s) with crc32", key)
            conn.download_file(bucket_name, key, tempfile, Config=config, ExtraArgs={'ChecksumMode': 'crc32'})
            #conn.download_file(bucket_name, key, tempfile, Config=config)
            result = bash(['cmp', tempfile, OUT_DIR + filename])
            assert result[1] == 0 ,"Files %s and %s differ!!" % (key, tempfile)
            os.remove(tempfile)

    assert expected_results == count_object_parts_in_all_buckets(True)
    log.debug("verify_objects::completed successfully!!")


#-------------------------------------------------------------------------------
def verify_objects_multi(files, conns, bucket_names, expected_results, config):
    max_tenants=len(conns)
    tempfile = OUT_DIR + "temp"
    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        log.debug("comparing file=%s, size=%d, copies=%d", filename, obj_size, num_copies)
        for i in range(0, num_copies):
            key = gen_object_name(filename, i)
            log.debug("comparing object %s with file %s", key, filename)
            ten_id = i % max_tenants
            conns[ten_id].download_file(bucket_names[ten_id], key, tempfile, Config=config)
            result = bash(['cmp', tempfile, OUT_DIR + filename])
            assert result[1] == 0 ,"Files %s and %s differ!!" % (key, tempfile)
            os.remove(tempfile)

    assert expected_results == count_object_parts_in_all_buckets(True)
    log.debug("verify_objects::completed successfully!!")


#-------------------------------------------------------------------------------
def thread_verify(thread_id, num_threads, files, conn, bucket, config):
    tempfile = OUT_DIR + "temp" + str(thread_id)
    count = 0
    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        log.debug("comparing file=%s, size=%d, copies=%d", filename, obj_size, num_copies)
        for i in range(0, num_copies):
            target_thread = count % num_threads
            count += 1
            if thread_id == target_thread:
                key = gen_object_name(filename, i)
                log.debug("comparing object %s with file %s", key, filename)
                conn.download_file(bucket, key, tempfile, Config=config)
                result = bash(['cmp', tempfile, OUT_DIR + filename])
                assert result[1] == 0 ,"Files %s and %s differ!!" % (key, tempfile)
                os.remove(tempfile)


#-------------------------------------------------------------------------------
def threads_verify_objects(files, conns, bucket_names, expected_results, config):
    num_threads=len(conns)
    thread_list=list()

    for idx in range(num_threads):
        t=threading.Thread(target=thread_verify,
                           args=(idx, num_threads, files, conns[idx], bucket_names[idx], config))
        thread_list.append(t)
        thread_list[idx].start()

    # wait for all worker thread to join
    for idx in range(num_threads):
        thread_list[idx].join()

    assert expected_results == count_object_parts_in_all_buckets(True)
    log.debug("verify_objects::completed successfully!!")


#-------------------------------------------------------------------------------
def get_stats_line_val(line):
    return int(line.rsplit("=", maxsplit=1)[1].strip())


#-------------------------------------------------------------------------------
def print_dedup_stats(dedup_stats):
    for key in dedup_stats.__dict__:
        log.warning("dedup_stats[%s] = %d", key, dedup_stats.__dict__[key])


#-------------------------------------------------------------------------------
def print_dedup_stats_diff(actual, expected):
    for (key1, key2) in zip(actual.__dict__, expected.__dict__):
        if (actual.__dict__[key1] != expected.__dict__[key2]):
            log.error("actual[%s] = %d != expected[%s] = %d",
                      key1, actual.__dict__[key1], key2, expected.__dict__[key2])


#-------------------------------------------------------------------------------
def reset_full_dedup_stats(dedup_stats):
    dedup_stats.total_processed_objects = 0
    dedup_stats.set_shared_manifest_src = 0
    dedup_stats.deduped_obj = 0
    dedup_stats.dup_head_size = 0
    dedup_stats.deduped_obj_bytes = 0
    dedup_stats.skip_shared_manifest = 0
    dedup_stats.skip_src_record = 0
    dedup_stats.skip_singleton = 0
    dedup_stats.skip_singleton_bytes = 0
    dedup_stats.skip_changed_object = 0
    dedup_stats.corrupted_etag = 0
    dedup_stats.sha256_mismatch = 0
    dedup_stats.valid_sha256 = 0
    dedup_stats.invalid_sha256 = 0
    dedup_stats.set_sha256 = 0


#-------------------------------------------------------------------------------
def read_full_dedup_stats(dedup_stats, md5_stats):
    main = md5_stats['main']
    dedup_stats.total_processed_objects = main['Total processed objects']
    dedup_stats.set_shared_manifest_src = main['Set Shared-Manifest SRC']
    dedup_stats.deduped_obj = main['Deduped Obj (this cycle)']
    dedup_stats.deduped_obj_bytes = main['Deduped Bytes(this cycle)']

    skipped = md5_stats['skipped']
    dedup_stats.skip_shared_manifest = skipped['Skipped shared_manifest']
    dedup_stats.skip_src_record = skipped['Skipped source record']
    dedup_stats.skip_singleton = skipped['Skipped singleton objs']
    if dedup_stats.skip_singleton:
        dedup_stats.skip_singleton_bytes = skipped['Skipped singleton Bytes']
    key='Skipped Changed Object'
    if key in skipped:
        dedup_stats.skip_changed_object = skipped[key]

    notify=md5_stats['notify']
    dedup_stats.valid_sha256 = notify['Valid SHA256 attrs']
    dedup_stats.invalid_sha256 = notify['Invalid SHA256 attrs']
    key='Set SHA256'
    if key in notify:
        dedup_stats.set_sha256 = notify[key]

    sys_failures = md5_stats['system failures']
    key='Corrupted ETAG'
    if key in sys_failures:
        dedup_stats.corrupted_etag = sys_failures[key]

    log_failures = md5_stats['logical failures']
    key='SHA256 mismatch'
    if key in log_failures:
        dedup_stats.sha256_mismatch = log_failures[key]


#-------------------------------------------------------------------------------
def read_dedup_ratio(json):
    dedup_ratio=Dedup_Ratio()
    dedup_ratio.s3_bytes_before=json['s3_bytes_before']
    dedup_ratio.s3_bytes_after=json['s3_bytes_after']
    dedup_ratio.ratio=json['dedup_ratio']

    log.debug("Completed! ::ratio=%f", dedup_ratio.ratio)
    return dedup_ratio

#-------------------------------------------------------------------------------
def verify_dedup_ratio(expected_dedup_stats, dedup_ratio):
    s3_bytes_before = expected_dedup_stats.size_before_dedup
    s3_dedup_bytes  = expected_dedup_stats.dedup_bytes_estimate
    s3_bytes_after  = s3_bytes_before - s3_dedup_bytes
    skipped_bytes   = (expected_dedup_stats.skip_too_small_bytes +
                       expected_dedup_stats.non_default_storage_class_objs_bytes)
    #s3_bytes_after -= skipped_bytes
    if (s3_bytes_before > s3_bytes_after) and s3_bytes_after:
        ratio = s3_bytes_before/s3_bytes_after
    else:
        ratio = 0

    log.debug("s3_bytes_before = %d/%d", s3_bytes_before, dedup_ratio.s3_bytes_before)
    log.debug("s3_dedup_bytes = %d", expected_dedup_stats.dedup_bytes_estimate);
    log.debug("s3_bytes_after = %d/%d", s3_bytes_after, dedup_ratio.s3_bytes_after)
    log.debug("ratio = %f/%f", ratio, dedup_ratio.ratio)

    assert s3_bytes_before == dedup_ratio.s3_bytes_before
    assert s3_bytes_after == dedup_ratio.s3_bytes_after
    assert ratio == dedup_ratio.ratio

#-------------------------------------------------------------------------------
def read_dedup_stats(dry_run):
    dedup_work_was_completed = False
    dedup_stats=Dedup_Stats()
    dedup_ratio_estimate=Dedup_Ratio()
    dedup_ratio_actual=Dedup_Ratio()

    result = admin(['dedup', 'stats'])
    assert result[1] == 0

    jstats=json.loads(result[0])
    worker_stats=jstats['worker_stats']
    main=worker_stats['main']
    skipped=worker_stats['skipped']
    notify=worker_stats['notify']
    dedup_stats.size_before_dedup = main['Accum byte size Ingress Objs']
    key='Ingress skip: too small objs'
    if key in skipped:
        dedup_stats.skip_too_small = skipped[key]
        dedup_stats.skip_too_small_bytes = skipped['Ingress skip: too small bytes']

    key='non default storage class objs bytes'
    if key in notify:
        dedup_stats.non_default_storage_class_objs_bytes = notify[key]

    key='md5_stats'
    if key in jstats:
        md5_stats=jstats[key]
        main=md5_stats['main']
        #dedup_stats.loaded_objects = main['Loaded objects']
        if dry_run == False:
            read_full_dedup_stats(dedup_stats, md5_stats)

        dedup_stats.singleton_obj = main['Singleton Obj']
        dedup_stats.unique_obj = main['Unique Obj']
        dedup_stats.duplicate_obj = main['Duplicate Obj']
        dedup_stats.dedup_bytes_estimate = main['Dedup Bytes Estimate']

        potential = md5_stats['Potential Dedup']
        dedup_stats.dup_head_size_estimate = potential['Duplicated Head Bytes Estimate']
        dedup_stats.dup_head_size = potential['Duplicated Head Bytes']
        dedup_stats.potential_singleton_obj = potential['Singleton Obj (64KB-4MB)']
        dedup_stats.potential_unique_obj = potential['Unique Obj (64KB-4MB)']
        dedup_stats.potential_duplicate_obj = potential['Duplicate Obj (64KB-4MB)']
        dedup_stats.potential_dedup_space = potential['Dedup Bytes Estimate (64KB-4MB)']

    dedup_work_was_completed=jstats['completed']
    if dedup_work_was_completed:
        dedup_ratio_estimate=read_dedup_ratio(jstats['dedup_ratio_estimate'])
        dedup_ratio_actual=read_dedup_ratio(jstats['dedup_ratio_actual'])
    else:
        log.debug("Uncompleted!")

    return (dedup_work_was_completed, dedup_stats, dedup_ratio_estimate, dedup_ratio_actual)


#-------------------------------------------------------------------------------
def exec_dedup_internal(expected_dedup_stats, dry_run, max_dedup_time):
    log.debug("sending exec_dedup request: dry_run=%d", dry_run)
    if dry_run:
        result = admin(['dedup', 'estimate'])
        reset_full_dedup_stats(expected_dedup_stats)
    else:
        result = admin(['dedup', 'restart'])

    assert result[1] == 0
    log.debug("wait for dedup to complete")

    dedup_time = 0
    dedup_timeout = 5
    dedup_stats = Dedup_Stats()
    dedup_ratio=Dedup_Ratio()
    wait_for_completion = True
    while wait_for_completion:
        assert dedup_time < max_dedup_time
        time.sleep(dedup_timeout)
        dedup_time += dedup_timeout
        ret = read_dedup_stats(dry_run)
        if ret[0]:
            wait_for_completion = False
            log.info("dedup completed in %d seconds", dedup_time)
            return (dedup_time, ret[1], ret[2], ret[3])


#-------------------------------------------------------------------------------
def exec_dedup(expected_dedup_stats, dry_run, verify_stats=True):
    # dedup should complete in less than 5 minutes
    max_dedup_time = 5*60
    if expected_dedup_stats.deduped_obj > 10000:
        max_dedup_time = 20 * 60
    elif expected_dedup_stats.deduped_obj > 5000:
        max_dedup_time = 10 * 60
    elif expected_dedup_stats.deduped_obj > 1000:
        max_dedup_time = 5 * 60

    ret=exec_dedup_internal(expected_dedup_stats, dry_run, max_dedup_time)
    dedup_time = ret[0]
    dedup_stats = ret[1]
    dedup_ratio_estimate = ret[2]
    dedup_ratio_actual = ret[3]

    if verify_stats == False:
        return ret

    if dedup_stats.potential_unique_obj or expected_dedup_stats.potential_unique_obj:
        log.debug("potential_unique_obj= %d / %d ", dedup_stats.potential_unique_obj,
                  expected_dedup_stats.potential_unique_obj)

    #dedup_stats.set_sha256 = dedup_stats.invalid_sha256
    if dedup_stats != expected_dedup_stats:
        log.debug("==================================================")
        print_dedup_stats_diff(dedup_stats, expected_dedup_stats)
        #print_dedup_stats(dedup_stats)
        log.debug("==================================================\n")
        assert dedup_stats == expected_dedup_stats

    verify_dedup_ratio(expected_dedup_stats, dedup_ratio_estimate)
    log.debug("expcted_dedup::stats check completed successfully!!")
    return ret


#-------------------------------------------------------------------------------
def prepare_test():
    cleanup_local()
    #make sure we are starting with all buckets empty
    if count_object_parts_in_all_buckets() != 0:
        log.warning("The system was left dirty from previous run");
        log.warning("Make sure to remove all objects before starting");
        assert(0)

    os.mkdir(OUT_DIR)

#-------------------------------------------------------------------------------
def copy_potential_stats(new_dedup_stats, dedup_stats):
    new_dedup_stats.potential_singleton_obj = dedup_stats.potential_singleton_obj
    new_dedup_stats.potential_unique_obj    = dedup_stats.potential_unique_obj
    new_dedup_stats.potential_duplicate_obj = dedup_stats.potential_duplicate_obj
    new_dedup_stats.potential_dedup_space   = dedup_stats.potential_dedup_space


#-------------------------------------------------------------------------------
def small_single_part_objs_dedup(conn, bucket_name, dry_run):
    # 1) generate small random files and store them on disk
    # 2) upload a random number of copies from each file to bucket
    # 3) execute DEDUP!!
    #    Read dedup stat-counters:
    #    5.a verify that objects smaller than RADOS_OBJ_SIZE were skipped
    prepare_test()
    try:
        files=[]
        num_files = 8
        base_size = 4*KB
        log.debug("generate files: base size=%d KiB, max_size=%d KiB",
                  base_size/KB, (pow(2, num_files) * base_size)/KB)
        gen_files(files, base_size, num_files)
        bucket = conn.create_bucket(Bucket=bucket_name)
        log.debug("upload objects to bucket <%s> ...", bucket_name)
        indices = [0] * len(files)
        ret = upload_objects(bucket_name, files, indices, conn, default_config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        # expected stats for small objects - all zeros except for skip_too_small
        small_objs_dedup_stats = Dedup_Stats()
        #small_objs_dedup_stats.loaded_objects=dedup_stats.loaded_objects
        copy_potential_stats(small_objs_dedup_stats, dedup_stats)
        small_objs_dedup_stats.size_before_dedup = dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small_bytes=dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small = s3_objects_total
        assert small_objs_dedup_stats == dedup_stats

        exec_dedup(dedup_stats, dry_run)
        if dry_run == False:
            log.debug("Verify all objects")
            verify_objects(bucket_name, files, conn, expected_results, default_config)

    finally:
        # cleanup must be executed even after a failure
        cleanup(bucket_name, conn)


#-------------------------------------------------------------------------------
def simple_dedup(conn, files, bucket_name, run_cleanup_after, config, dry_run):
    # 1) generate random files and store them on disk
    # 2) upload a random number of copies of each file to bucket
    # 3) calculate current count of rados objects and pool size
    # 4) calculate expected count of rados objects and pool size *post dedup*

    # 5) execute DEDUP!!
    #    Read dedup stat-counters:
    #    5.a verify that objects smaller than RADOS_OBJ_SIZE were skipped
    #    5.b verify that all objects larger than RADOS_OBJ_SIZE were processed/loaded
    #    5.c verify that objects with single occurrence were skipped
    #    5.d verify that we calculate the correct number of unique and dedup objects
    #
    # 6) Read all objects from bucket and compare them to their stored copy *before dedup*
    #         This step is used to make sure objects were not corrupted by dedup
    # 7) count number and size of in-pool rados objects and compare with expected
    #         This step is used to make sure dedup removed *all* duplications
    # 8) delete all objects from bucket using s3 API
    # 9) call GC to make sure everything was removed
    #10) verify that there is nothing left on pool (i.e. ref-count is working)
    try:
        log.debug("conn.create_bucket(%s)", bucket_name)
        bucket = conn.create_bucket(Bucket=bucket_name)
        indices = [0] * len(files)
        log.debug("upload objects to bucket <%s> ...", bucket_name)
        ret = upload_objects(bucket_name, files, indices, conn, config)
        expected_results = ret[0]
        dedup_stats = ret[1]

        exec_dedup(dedup_stats, dry_run)
        if dry_run == False:
            log.debug("Verify all objects")
            verify_objects(bucket_name, files, conn, expected_results, config)

        return ret
    finally:
        if run_cleanup_after:
            # cleanup must be executed even after a failure
            cleanup(bucket_name, conn)


#-------------------------------------------------------------------------------
def simple_dedup_with_tenants(files, conns, bucket_names, config, dry_run=False):
    indices=[0] * len(files)
    ret=upload_objects_multi(files, conns, bucket_names, indices, config)
    expected_results = ret[0]
    dedup_stats = ret[1]
    exec_dedup(dedup_stats, dry_run)
    if dry_run == False:
        log.debug("Verify all objects")
        verify_objects_multi(files, conns, bucket_names, expected_results, config)

    return ret


#-------------------------------------------------------------------------------
def dedup_basic_with_tenants_common(files, max_copies_count, config, dry_run):
    try:
        ret=gen_connections_multi2(max_copies_count)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]
        simple_dedup_with_tenants(files, conns, bucket_names, config, dry_run)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
def threads_simple_dedup_with_tenants(files, conns, bucket_names, config, dry_run=False):
    indices=[0] * len(files)

    start = time.time_ns()
    upload_ret=procs_upload_objects(files, conns, bucket_names, indices, config)
    upload_time_sec = (time.time_ns() - start) / (1000*1000*1000)
    expected_results = upload_ret[0]
    dedup_stats = upload_ret[1]
    s3_objects_total = upload_ret[2]

    exec_ret=exec_dedup(dedup_stats, dry_run)
    exec_time_sec=exec_ret[0]
    verify_time_sec=0
    if dry_run == False:
        log.debug("Verify all objects")
        start = time.time_ns()
        threads_verify_objects(files, conns, bucket_names,
                               expected_results, config)
        verify_time_sec = (time.time_ns() - start)  / (1000*1000*1000)

    log.info("[%d] obj_count=%d, upload=%d(sec), exec=%d(sec), verify=%d(sec)",
             len(conns), s3_objects_total, upload_time_sec, exec_time_sec, verify_time_sec);
    return upload_ret


#-------------------------------------------------------------------------------
def threads_dedup_basic_with_tenants_common(files, num_conns, config, dry_run):
    try:
        ret=gen_connections_multi2(num_conns)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]
        threads_simple_dedup_with_tenants(files, conns, bucket_names, config, dry_run)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
def check_full_dedup_state():
    global full_dedup_state_was_checked
    global full_dedup_state_disabled
    log.debug("check_full_dedup_state:: sending FULL Dedup request")
    result = admin(['dedup', 'restart'])
    if result[1] == 0:
        log.debug("full dedup is enabled!")
        full_dedup_state_disabled = False
        result = admin(['dedup', 'abort'])
        assert result[1] == 0
    else:
        log.debug("full dedup is disabled, skip all full dedup tests")
        full_dedup_state_disabled = True

    full_dedup_state_was_checked = True
    return full_dedup_state_disabled


#-------------------------------------------------------------------------------
def full_dedup_is_disabled():
    global full_dedup_state_was_checked
    global full_dedup_state_disabled

    if not full_dedup_state_was_checked:
        full_dedup_state_disabled = check_full_dedup_state()

    if full_dedup_state_disabled:
        log.debug("Full Dedup is DISABLED, skipping test...")

    return full_dedup_state_disabled


CORRUPTIONS = ("no corruption", "change_etag", "illegal_hex_value",
               "change_num_parts", "illegal_separator",
               "illegal_dec_val_num_parts", "illegal_num_parts_overflow")

#------------------------------------------------------------------------------
def change_object_etag(rados_name, new_etag):
    result = rados(['-p ', POOLNAME, 'setxattr', rados_name, ETAG_ATTR, new_etag])
    assert result[1] == 0

#------------------------------------------------------------------------------
def gen_new_etag(etag, corruption, expected_dedup_stats):
    expected_dedup_stats.skip_changed_object = 0
    expected_dedup_stats.corrupted_etag = 0

    if corruption == "change_etag":
        # replace one character in the ETAG (will report changed ETAG)
        expected_dedup_stats.skip_changed_object = 1
        ch="a"
        if etag[0] == ch:
            ch="b"

        return etag.replace(etag[0], ch, 1)

    elif corruption == "illegal_hex_value":
        # set an illegal hex value (will report corrupted ETAG)
        expected_dedup_stats.corrupted_etag = 1
        ch="Z"
        return etag.replace(etag[0], ch, 1)

    elif corruption == "change_num_parts":
        # change num_parts (will report changed ETAG)
        expected_dedup_stats.skip_changed_object = 1
        return etag + "1"

    elif corruption == "illegal_separator":
        # change the num_parts separtor (will report corrupted ETAG)
        expected_dedup_stats.corrupted_etag = 1
        idx=len(etag) - 2
        ch="a"
        return etag.replace(etag[idx], ch, 1)

    elif corruption == "illegal_dec_val_num_parts":
        # set an illegal decimal val in num_parts (will report corrupted ETAG)
        expected_dedup_stats.corrupted_etag = 1
        return etag + "a"

    elif corruption == "illegal_num_parts_overflow":
        # expand num_part beyond the legal 10,000 (will report corrupted ETAG)
        expected_dedup_stats.corrupted_etag = 1
        return etag + "1111"

#------------------------------------------------------------------------------
def corrupt_etag(key, corruption, expected_dedup_stats):
    log.debug("key=%s, corruption=%s", key, corruption);
    result = rados(['ls', '-p ', POOLNAME])
    assert result[1] == 0

    names=result[0].split()
    for name in names:
        log.debug("name=%s", name)
        if key in name:
            log.debug("key=%s is a substring of name=%s", key, name);
            rados_name = name
            break;

    result = rados(['-p ', POOLNAME, 'getxattr', rados_name, ETAG_ATTR])
    assert result[1] == 0
    old_etag = result[0]

    new_etag=gen_new_etag(old_etag, corruption, expected_dedup_stats)

    log.debug("Corruption:: %s\nold_etag=%s\nnew_etag=%s",
             corruption, old_etag, new_etag)
    change_object_etag(rados_name, new_etag)
    return (rados_name, old_etag)

#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_etag_corruption():
    #return

    if full_dedup_is_disabled():
        return

    bucket_name = gen_bucket_name()
    log.debug("test_dedup_etag_corruption: connect to AWS ...")
    conn=get_single_connection()
    prepare_test()
    try:
        files=[]
        num_files = 1
        # generate a single object of MULTIPART_SIZE with 2 identical copies
        gen_files_fixed_copies(files, num_files, MULTIPART_SIZE, 2)

        bucket = conn.create_bucket(Bucket=bucket_name)
        indices = [0] * len(files)
        ret = upload_objects(bucket_name, files, indices, conn, default_config)
        expected_results = ret[0]
        expected_dedup_stats = ret[1]
        s3_objects_total = ret[2]
        f=files[0]
        filename=f[0]
        key=gen_object_name(filename, 0)

        for corruption in CORRUPTIONS:
            if corruption != "no corruption":
                corrupted=corrupt_etag(key, corruption, expected_dedup_stats)
                # no dedup will happen because of the inserted corruption
                expected_dedup_stats.deduped_obj=0
                expected_dedup_stats.deduped_obj_bytes=0
                expected_dedup_stats.set_shared_manifest_src=0

            dry_run=False
            ret=exec_dedup(expected_dedup_stats, dry_run)
            #dedup_stats=ret[1]
            dedup_ratio_estimate=ret[2]
            dedup_ratio_actual=ret[3]

            if corruption == "no corruption":
                expected_dedup_stats.valid_sha256=1
                expected_dedup_stats.invalid_sha256=0
                expected_dedup_stats.set_sha256=0

            s3_bytes_before=expected_dedup_stats.size_before_dedup
            expected_ratio_actual=Dedup_Ratio()
            expected_ratio_actual.s3_bytes_before=s3_bytes_before
            expected_ratio_actual.s3_bytes_after=s3_bytes_before
            expected_ratio_actual.ratio=0
            if corruption != "no corruption":
                assert expected_ratio_actual == dedup_ratio_actual
                change_object_etag(corrupted[0], corrupted[1])

    finally:
        # cleanup must be executed even after a failure
        cleanup(bucket_name, conn)

#-------------------------------------------------------------------------------
def write_bin_file(files, bin_arr, filename):
    full_filename = OUT_DIR + filename
    fout = open(full_filename, "wb")
    fout.write(bin_arr)
    fout.close()
    files.append((filename, len(bin_arr), 1))

#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_md5_collisions():
    #return

    if full_dedup_is_disabled():
        return

    s1="d131dd02c5e6eec4693d9a0698aff95c2fcab58712467eab4004583eb8fb7f8955ad340609f4b30283e488832571415a085125e8f7cdc99fd91dbdf280373c5bd8823e3156348f5bae6dacd436c919c6dd53e2b487da03fd02396306d248cda0e99f33420f577ee8ce54b67080a80d1ec69821bcb6a8839396f9652b6ff72a70"
    s2="d131dd02c5e6eec4693d9a0698aff95c2fcab50712467eab4004583eb8fb7f8955ad340609f4b30283e4888325f1415a085125e8f7cdc99fd91dbd7280373c5bd8823e3156348f5bae6dacd436c919c6dd53e23487da03fd02396306d248cda0e99f33420f577ee8ce54b67080280d1ec69821bcb6a8839396f965ab6ff72a70"

    s1_bin=bytes.fromhex(s1)
    s2_bin=bytes.fromhex(s2)

    s1_hash=hashlib.md5(s1_bin).hexdigest()
    s2_hash=hashlib.md5(s2_bin).hexdigest()
    # data is different
    assert s1 != s2
    # but MD5 is identical
    assert s1_hash == s2_hash

    prepare_test()
    files=[]
    try:
        write_bin_file(files, s1_bin, "s1")
        write_bin_file(files, s2_bin, "s2")

        bucket_name = gen_bucket_name()
        log.debug("test_md5_collisions: connect to AWS ...")
        config2=TransferConfig(multipart_threshold=64, multipart_chunksize=1*MB)
        conn=get_single_connection()
        bucket = conn.create_bucket(Bucket=bucket_name)
        indices = [0] * len(files)
        upload_objects(bucket_name, files, indices, conn, config2)

        dedup_stats = Dedup_Stats()
        # we wrote 2 different small objects (BLOCK_SIZE) with the same md5
        dedup_stats.total_processed_objects=2
        #dedup_stats.loaded_objects=dedup_stats.total_processed_objects
        # the objects will seem like a duplications with 1 unique and 1 duplicate
        dedup_stats.unique_obj=1
        dedup_stats.duplicate_obj=1
        dedup_stats.skip_src_record=1
        # the objects are 128 Bytes long so will take the min of BLOCK_SIZE each
        dedup_stats.size_before_dedup=2*BLOCK_SIZE
        # the md5 collision confuses the estimate
        dedup_stats.dedup_bytes_estimate=BLOCK_SIZE
        # SHA256 check will expose the problem
        dedup_stats.invalid_sha256=dedup_stats.total_processed_objects
        dedup_stats.set_sha256=dedup_stats.total_processed_objects
        dedup_stats.sha256_mismatch=1
        s3_bytes_before=dedup_stats.size_before_dedup
        expected_ratio_actual=Dedup_Ratio()
        expected_ratio_actual.s3_bytes_before=s3_bytes_before
        expected_ratio_actual.s3_bytes_after=s3_bytes_before
        expected_ratio_actual.ratio=0

        dry_run=False
        log.debug("test_md5_collisions: first call to exec_dedup")
        ret=exec_dedup(dedup_stats, dry_run)
        dedup_ratio_actual=ret[3]

        assert expected_ratio_actual == dedup_ratio_actual

        dedup_stats.valid_sha256=dedup_stats.total_processed_objects
        dedup_stats.invalid_sha256=0
        dedup_stats.set_sha256=0

        log.debug("test_md5_collisions: second call to exec_dedup")
        ret=exec_dedup(dedup_stats, dry_run)
        dedup_ratio_actual=ret[3]

        assert expected_ratio_actual == dedup_ratio_actual

    finally:
        # cleanup must be executed even after a failure
        cleanup(bucket_name, conn)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small():
    #return

    if full_dedup_is_disabled():
        return

    bucket_name = gen_bucket_name()
    log.debug("test_dedup_small: connect to AWS ...")
    conn=get_single_connection()
    small_single_part_objs_dedup(conn, bucket_name, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    max_copies_count=3
    files=[]
    num_files=10 # [4KB-4MB]
    base_size = 4*KB
    log.debug("generate files: base size=%d KiB, max_size=%d KiB",
             base_size/KB, (pow(2, num_files) * base_size)/KB)
    try:
        gen_files(files, base_size, num_files, max_copies_count)
        indices=[0] * len(files)
        ret=gen_connections_multi2(max_copies_count)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]

        ret=upload_objects_multi(files, conns, bucket_names, indices, default_config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        # expected stats for small objects - all zeros except for skip_too_small
        small_objs_dedup_stats = Dedup_Stats()
        #small_objs_dedup_stats.loaded_objects=dedup_stats.loaded_objects
        copy_potential_stats(small_objs_dedup_stats, dedup_stats)
        small_objs_dedup_stats.size_before_dedup=dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small_bytes=dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small=s3_objects_total
        assert small_objs_dedup_stats == dedup_stats

        dry_run=False
        exec_dedup(dedup_stats, dry_run)
        log.debug("Verify all objects")
        verify_objects_multi(files, conns, bucket_names, expected_results, default_config)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#------------------------------------------------------------------------------
# Trivial incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Run a second dedup *without making any change*
# 3) The stats-counters should show the same dedup ratio, but no change
#    should be made to the system
@pytest.mark.basic_test
def test_dedup_inc_0_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_dedup_inc_0: connect to AWS ...")
    max_copies_count=3
    config=default_config
    ret=gen_connections_multi2(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files=11
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(files, conns, bucket_names, config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        dedup_stats2 = dedup_stats
        dedup_stats2.dup_head_size = 0
        dedup_stats2.skip_shared_manifest=dedup_stats.deduped_obj
        dedup_stats2.skip_src_record=dedup_stats.set_shared_manifest_src
        dedup_stats2.set_shared_manifest_src=0
        dedup_stats2.deduped_obj=0
        dedup_stats2.deduped_obj_bytes=0
        dedup_stats2.valid_sha256=dedup_stats.invalid_sha256
        dedup_stats2.invalid_sha256=0
        dedup_stats2.set_sha256=0

        log.debug("test_dedup_inc_0_with_tenants: incremental dedup:")
        # run dedup again and make sure nothing has changed
        dry_run=False
        exec_dedup(dedup_stats2, dry_run)
        verify_objects_multi(files, conns, bucket_names, expected_results, config)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#------------------------------------------------------------------------------
# Trivial incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Run a second dedup *without making any change*
# 3) The stats-counters should show the same dedup ratio, but no change
#    should be made to the system
@pytest.mark.basic_test
def test_dedup_inc_0():
    #return

    if full_dedup_is_disabled():
        return

    config=default_config
    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_inc_0: connect to AWS ...")
    conn=get_single_connection()
    try:
        files=[]
        num_files = 11
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(conn, files, bucket_name, False, config, False)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        dedup_stats2 = dedup_stats
        dedup_stats2.dup_head_size = 0
        dedup_stats2.skip_shared_manifest=dedup_stats.deduped_obj
        dedup_stats2.skip_src_record=dedup_stats.set_shared_manifest_src
        dedup_stats2.set_shared_manifest_src=0
        dedup_stats2.deduped_obj=0
        dedup_stats2.deduped_obj_bytes=0
        dedup_stats2.valid_sha256=dedup_stats.invalid_sha256
        dedup_stats2.invalid_sha256=0
        dedup_stats2.set_sha256=0

        log.debug("test_dedup_inc_0: incremental dedup:")
        # run dedup again and make sure nothing has changed
        dry_run=False
        exec_dedup(dedup_stats2, dry_run)
        verify_objects(bucket_name, files, conn, expected_results, config)
    finally:
        # cleanup must be executed even after a failure
        cleanup(bucket_name, conn)


#-------------------------------------------------------------------------------
# Basic incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the *original objects*
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_1_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_dedup_inc_1_with_tenants: connect to AWS ...")
    max_copies_count=6
    config=default_config
    ret=gen_connections_multi2(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files=17
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(files, conns, bucket_names, config)
        expected_results_base=ret[0]
        stats_base=ret[1]

        # upload more copies of the same objects
        indices=[]
        files_combined=[]
        for f in files:
            filename=f[0]
            obj_size=f[1]
            num_copies_base=f[2]
            # indices holds the start index of the new copies
            indices.append(num_copies_base)
            num_copies_to_add=random.randint(0, 2)
            num_copies_combined=num_copies_to_add+num_copies_base
            files_combined.append((filename, obj_size, num_copies_combined))

        ret=upload_objects_multi(files_combined, conns, bucket_names, indices, config, False)
        expected_results=ret[0]
        stats_combined=ret[1]
        stats_combined.skip_shared_manifest = stats_base.deduped_obj
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.dup_head_size       -= stats_base.dup_head_size
        stats_combined.skip_src_record     += stats_base.set_shared_manifest_src

        stats_combined.set_shared_manifest_src -= stats_base.set_shared_manifest_src
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        stats_combined.valid_sha256    = stats_base.set_sha256
        stats_combined.invalid_sha256 -= stats_base.set_sha256
        stats_combined.set_sha256     -= stats_base.set_sha256

        log.debug("test_dedup_inc_1_with_tenants: incremental dedup:")
        # run dedup again
        dry_run=False
        exec_dedup(stats_combined, dry_run)
        verify_objects_multi(files_combined, conns, bucket_names, expected_results, config)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
# Basic incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the *original objects*
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_1():
    #return

    if full_dedup_is_disabled():
        return

    config=default_config
    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_inc_1: connect to AWS ...")
    conn=get_single_connection()
    try:
        files=[]
        num_files = 4
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(conn, files, bucket_name, False, config, False)
        expected_results_base = ret[0]
        stats_base = ret[1]

        # upload more copies of the same objects
        indices=[]
        files_combined=[]
        for f in files:
            filename=f[0]
            obj_size=f[1]
            num_copies_base=f[2]
            # indices holds the start index of the new copies
            indices.append(num_copies_base)
            num_copies_to_add=random.randint(0, 2)
            num_copies_combined=num_copies_to_add+num_copies_base
            files_combined.append((filename, obj_size, num_copies_combined))

        ret=upload_objects(bucket_name, files_combined, indices, conn, config, False)
        expected_results = ret[0]
        stats_combined = ret[1]
        stats_combined.skip_shared_manifest = stats_base.deduped_obj
        stats_combined.dup_head_size       -= stats_base.dup_head_size
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.skip_src_record     += stats_base.set_shared_manifest_src

        stats_combined.set_shared_manifest_src -= stats_base.set_shared_manifest_src
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        stats_combined.valid_sha256    = stats_base.set_sha256
        stats_combined.invalid_sha256 -= stats_base.set_sha256
        stats_combined.set_sha256     -= stats_base.set_sha256

        log.debug("test_dedup_inc_1: incremental dedup:")
        # run dedup again
        dry_run=False
        exec_dedup(stats_combined, dry_run)
        verify_objects(bucket_name, files_combined, conn, expected_results, config)
    finally:
        # cleanup must be executed even after a failure
        cleanup(bucket_name, conn)


#-------------------------------------------------------------------------------
# Simple incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the original objects
# 3) Add new objects to buckets
# 4) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_2_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_dedup_inc_2_with_tenants: connect to AWS ...")
    max_copies_count=6
    config=default_config
    ret=gen_connections_multi2(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files = 17
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(files, conns, bucket_names, config)
        expected_results_base=ret[0]
        stats_base=ret[1]

        # upload more copies of the same files
        indices=[]
        files_combined=[]
        for f in files:
            filename=f[0]
            obj_size=f[1]
            num_copies_base=f[2]
            # indices holds the start index of the new copies
            indices.append(num_copies_base)
            num_copies_inc=random.randint(0, 2)
            num_copies_combined=num_copies_inc+num_copies_base
            files_combined.append((filename, obj_size, num_copies_combined))

        # add new files
        num_files_new = 13
        gen_files_in_range(files_combined, num_files_new, 2*MB, 32*MB)
        pad_count = len(files_combined) - len(files)
        for i in range(0, pad_count):
            indices.append(0)

        assert(len(indices) == len(files_combined))
        ret=upload_objects_multi(files_combined, conns, bucket_names, indices, config, False)
        expected_results = ret[0]
        stats_combined = ret[1]
        stats_combined.skip_shared_manifest = stats_base.deduped_obj
        stats_combined.dup_head_size       -= stats_base.dup_head_size
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.skip_src_record     += stats_base.set_shared_manifest_src

        stats_combined.set_shared_manifest_src -= stats_base.set_shared_manifest_src
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        stats_combined.valid_sha256    = stats_base.set_sha256
        stats_combined.invalid_sha256 -= stats_base.set_sha256
        stats_combined.set_sha256     -= stats_base.set_sha256

        log.debug("test_dedup_inc_2_with_tenants: incremental dedup:")
        # run dedup again
        dry_run=False
        exec_dedup(stats_combined, dry_run)
        verify_objects_multi(files_combined, conns, bucket_names, expected_results, config)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
# Simple incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the original objects
# 3) Add new objects to buckets
# 4) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_2():
    #return

    if full_dedup_is_disabled():
        return

    config=default_config
    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_inc_2: connect to AWS ...")
    conn=get_single_connection()
    try:
        files=[]
        num_files = 17
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(conn, files, bucket_name, False, config, False)
        expected_results_base = ret[0]
        stats_base = ret[1]

        # upload more copies of the same files
        indices=[]
        files_combined=[]
        for f in files:
            filename=f[0]
            obj_size=f[1]
            num_copies_base=f[2]
            indices.append(num_copies_base)
            num_copies_inc=random.randint(0, 2)
            num_copies_combined=num_copies_inc+num_copies_base
            files_combined.append((filename, obj_size, num_copies_combined))

        # add new files
        num_files_new = 13
        gen_files_in_range(files_combined, num_files_new, 2*MB, 32*MB)
        pad_count = len(files_combined) - len(files)
        for i in range(0, pad_count):
            indices.append(0)

        assert(len(indices) == len(files_combined))
        ret=upload_objects(bucket_name, files_combined, indices, conn, config, False)
        expected_results = ret[0]
        stats_combined = ret[1]
        stats_combined.skip_shared_manifest = stats_base.deduped_obj
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.dup_head_size       -= stats_base.dup_head_size
        stats_combined.skip_src_record     += stats_base.set_shared_manifest_src

        stats_combined.set_shared_manifest_src -= stats_base.set_shared_manifest_src
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        stats_combined.valid_sha256    = stats_base.set_sha256
        stats_combined.invalid_sha256 -= stats_base.set_sha256
        stats_combined.set_sha256     -= stats_base.set_sha256

        log.debug("test_dedup_inc_2: incremental dedup:")
        # run dedup again
        dry_run=False
        exec_dedup(stats_combined, dry_run)
        verify_objects(bucket_name, files_combined, conn, expected_results,
                       config)
    finally:
        # cleanup must be executed even after a failure
        cleanup(bucket_name, conn)


#-------------------------------------------------------------------------------
# Incremental dedup with object removal:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Remove copies of some objects
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_with_remove_multi_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_dedup_inc_with_remove_multi_tenants: connect to AWS ...")
    max_copies_count=6
    config=default_config
    ret=gen_connections_multi2(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files = 17
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(files, conns, bucket_names, config)
        expected_results_base = ret[0]
        stats_base = ret[1]

        # REMOVE some objects and update stats/expected
        src_record=0
        shared_manifest=0
        valid_sha=0
        object_keys=[]
        files_sub=[]
        dedup_stats = Dedup_Stats()
        for f in files:
            filename=f[0]
            obj_size=f[1]
            num_copies=f[2]
            num_remove=random.randint(0, num_copies)
            num_copies_2=num_copies-num_remove
            log.debug("objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies_2);
            if num_copies_2:
                if num_copies_2 > 1 and obj_size > RADOS_OBJ_SIZE:
                    valid_sha += num_copies_2
                    src_record += 1
                    shared_manifest += (num_copies_2 - 1)

                files_sub.append((filename, obj_size, num_copies_2))
                calc_expected_stats(dedup_stats, obj_size, num_copies_2, config)

            start_idx=num_copies_2
            for i in range(start_idx, num_copies):
                key = gen_object_name(filename, i)
                log.debug("delete object Bucket=%s, Key=%s", bucket_names[i], key);
                conns[i].delete_object(Bucket=bucket_names[i], Key=key)

        # must call garbage collection for a predictable count
        result = admin(['gc', 'process', '--include-all'])
        assert result[1] == 0

        # run dedup again
        dedup_stats.set_shared_manifest_src=0
        dedup_stats.deduped_obj=0
        dedup_stats.dup_head_size=0
        dedup_stats.deduped_obj_bytes=0
        dedup_stats.skip_src_record=src_record
        dedup_stats.skip_shared_manifest=shared_manifest
        dedup_stats.valid_sha256=valid_sha
        dedup_stats.invalid_sha256=0
        dedup_stats.set_sha256=0

        log.debug("test_dedup_inc_with_remove: incremental dedup:")
        dry_run=False
        exec_dedup(dedup_stats, dry_run)
        expected_results=calc_expected_results(files_sub, config)
        verify_objects_multi(files_sub, conns, bucket_names, expected_results, config)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
# Incremental dedup with object removal:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Remove copies of some objects
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_with_remove():
    #return

    if full_dedup_is_disabled():
        return

    config=default_config
    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_inc_with_remove: connect to AWS ...")
    conn=get_single_connection()
    try:
        files=[]
        num_files = 17
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(conn, files, bucket_name, False, config, False)
        expected_results_base = ret[0]
        stats_base = ret[1]

        # REMOVE some objects and update stats/expected
        src_record=0
        shared_manifest=0
        valid_sha=0
        object_keys=[]
        files_sub=[]
        dedup_stats = Dedup_Stats()
        for f in files:
            filename=f[0]
            obj_size=f[1]
            num_copies=f[2]
            num_remove=random.randint(0, num_copies)
            num_copies_2=num_copies-num_remove
            log.debug("objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies_2);
            if num_copies_2:
                if num_copies_2 > 1 and obj_size > RADOS_OBJ_SIZE:
                    valid_sha += num_copies_2
                    src_record += 1
                    shared_manifest += (num_copies_2 - 1)

                files_sub.append((filename, obj_size, num_copies_2))
                calc_expected_stats(dedup_stats, obj_size, num_copies_2, config)

            start_idx=num_copies_2
            for i in range(start_idx, num_copies):
                key = gen_object_name(filename, i)
                log.debug("delete key::%s::", key);
                object_keys.append(key)

            if len(object_keys) == 0:
                log.debug("Skiping file=%s, num_remove=%d", filename, num_remove)
                continue

            response=conn.delete_objects(Bucket=bucket_name,
                                         Delete={"Objects": [{"Key": key} for key in object_keys]})

        # must call garbage collection for predictable count
        result = admin(['gc', 'process', '--include-all'])
        assert result[1] == 0

        # run dedup again
        dedup_stats.set_shared_manifest_src=0
        dedup_stats.deduped_obj=0
        dedup_stats.dup_head_size=0
        dedup_stats.deduped_obj_bytes=0
        dedup_stats.skip_src_record=src_record
        dedup_stats.skip_shared_manifest=shared_manifest
        dedup_stats.valid_sha256=valid_sha
        dedup_stats.invalid_sha256=0
        dedup_stats.set_sha256=0

        log.debug("test_dedup_inc_with_remove: incremental dedup:")
        log.debug("stats_base.size_before_dedup=%d", stats_base.size_before_dedup)
        log.debug("dedup_stats.size_before_dedup=%d", dedup_stats.size_before_dedup)
        dry_run=False
        exec_dedup(dedup_stats, dry_run)
        expected_results=calc_expected_results(files_sub, config)
        verify_objects(bucket_name, files_sub, conn, expected_results, config)
    finally:
        # cleanup must be executed even after a failure
        cleanup(bucket_name, conn)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_multipart_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_dedup_multipart_with_tenants: connect to AWS ...")
    max_copies_count=3
    num_files=8
    files=[]
    min_size=MULTIPART_SIZE
    # create files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on RADOS_OBJ_SIZE
    gen_files_in_range(files, num_files, min_size, min_size*8)

    # add files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on MULTIPART_SIZE
    gen_files_in_range(files, num_files, min_size, min_size*8, MULTIPART_SIZE)

    # add file with excatly MULTIPART_SIZE
    write_random(files, MULTIPART_SIZE, 2, 2)

    dedup_basic_with_tenants_common(files, max_copies_count, default_config, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_multipart():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_multipart: connect to AWS ...")
    conn=get_single_connection()
    files=[]

    num_files=8
    min_size=MULTIPART_SIZE
    # create files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on RADOS_OBJ_SIZE
    gen_files_in_range(files, num_files, min_size, min_size*8)

    # add files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on MULTIPART_SIZE
    gen_files_in_range(files, num_files, min_size, min_size*8, MULTIPART_SIZE)

    # add file with excatly MULTIPART_SIZE
    write_random(files, MULTIPART_SIZE, 2, 2)

    simple_dedup(conn, files, bucket_name, True, default_config, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_basic_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    max_copies_count=3
    num_files=23
    file_size=33*MB
    files=[]
    log.debug("test_dedup_basic_with_tenants: connect to AWS ...")
    gen_files_fixed_size(files, num_files, file_size, max_copies_count)
    dedup_basic_with_tenants_common(files, max_copies_count, default_config, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_basic():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_basic: connect to AWS ...")
    conn=get_single_connection()
    files=[]
    num_files=5
    base_size = MULTIPART_SIZE
    log.debug("generate files: base size=%d MiB, max_size=%d MiB",
             base_size/MB, (pow(2, num_files) * base_size)/MB)
    gen_files(files, base_size, num_files)
    log.debug("call simple_dedup()")
    simple_dedup(conn, files, bucket_name, True, default_config, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small_multipart_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    max_copies_count=4
    num_files=10
    min_size=4*KB
    max_size=512*KB
    files=[]
    config=TransferConfig(multipart_threshold=min_size, multipart_chunksize=1*MB)
    log.debug("test_dedup_small_multipart_with_tenants: connect to AWS ...")

    # create files in range [4KB-512KB] aligned on 4KB
    gen_files_in_range(files, num_files, min_size, max_size, min_size)
    dedup_basic_with_tenants_common(files, max_copies_count, config, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small_multipart():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_dedup_small_multipart: connect to AWS ...")
    config2=TransferConfig(multipart_threshold=4*KB, multipart_chunksize=1*MB)
    conn=get_single_connection()
    files=[]
    bucket_name=gen_bucket_name()
    bucket = conn.create_bucket(Bucket=bucket_name)
    num_files = 10
    min_size = 4*KB
    max_size = 512*KB

    # create files in range [4KB-512KB] aligned on 4KB
    gen_files_in_range(files, num_files, min_size, max_size, min_size)
    simple_dedup(conn, files, bucket_name, True, config2, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_large_scale_with_tenants():
    return

    if full_dedup_is_disabled():
        return

    prepare_test()
    max_copies_count=3
    num_threads=16
    num_files=8*1024
    size=1*KB
    files=[]
    config=TransferConfig(multipart_threshold=size, multipart_chunksize=1*MB)
    log.debug("test_dedup_large_scale_with_tenants: connect to AWS ...")
    gen_files_fixed_size(files, num_files, size, max_copies_count)
    threads_dedup_basic_with_tenants_common(files, num_threads, config, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_large_scale():
    return

    if full_dedup_is_disabled():
        return

    prepare_test()
    max_copies_count=3
    num_threads=16
    num_files=8*1024
    size=1*KB
    files=[]
    config=TransferConfig(multipart_threshold=size, multipart_chunksize=1*MB)
    log.debug("test_dedup_dry_large_scale_with_tenants: connect to AWS ...")
    gen_files_fixed_size(files, num_files, size, max_copies_count)
    threads_dedup_basic_with_tenants_common(files, num_threads, config, False)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_empty_bucket():
    return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_empty_bucket: connect to AWS ...")

    max_copies_count=2
    config = default_config

    files=[]
    try:
        ret=gen_connections_multi2(max_copies_count)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
def inc_step_with_tenants(stats_base, files, conns, bucket_names, config):
    max_copies_count=len(conns)
    # upload more copies of the same files
    indices=[]
    files_combined=[]
    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies_base=f[2]
        # indices holds the start index of the new copies
        indices.append(num_copies_base)
        num_copies_inc=random.randint(0, 2)
        num_copies_combined=num_copies_inc+num_copies_base
        files_combined.append((filename, obj_size, num_copies_combined))

    # add new files
    num_files_new = 11
    gen_files_in_range(files_combined, num_files_new, 2*MB, 32*MB)
    pad_count = len(files_combined) - len(files)
    for i in range(0, pad_count):
        indices.append(0)

    assert(len(indices) == len(files_combined))
    ret=upload_objects_multi(files_combined, conns, bucket_names, indices, config, False)
    expected_results = ret[0]
    stats_combined = ret[1]

    src_record=0
    for f in files_combined:
        obj_size=f[1]
        num_copies=f[2]
        if num_copies > 1 and obj_size > RADOS_OBJ_SIZE:
            src_record += 1

    stats_combined.skip_shared_manifest = stats_base.deduped_obj
    stats_combined.skip_src_record      = src_record
    stats_combined.set_shared_manifest_src -= stats_base.set_shared_manifest_src
    stats_combined.dup_head_size       -= stats_base.dup_head_size
    stats_combined.deduped_obj         -= stats_base.deduped_obj
    stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

    stats_combined.valid_sha256    = stats_base.set_sha256
    stats_combined.invalid_sha256 -= stats_base.set_sha256
    stats_combined.set_sha256     -= stats_base.set_sha256

    log.debug("test_dedup_inc_2_with_tenants: incremental dedup:")
    # run dedup again
    dry_run=False
    exec_dedup(stats_combined, dry_run)
    verify_objects_multi(files_combined, conns, bucket_names, expected_results, config)

    return (files_combined, stats_combined)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
#@pytest.mark.inc_test
def test_dedup_inc_loop_with_tenants():
    #return

    if full_dedup_is_disabled():
        return

    prepare_test()
    log.debug("test_dedup_inc_loop_with_tenants: connect to AWS ...")
    max_copies_count=3
    config=default_config
    ret=gen_connections_multi2(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files = 13
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(files, conns, bucket_names, config)
        stats_base=ret[1]

        for idx in range(0, 9):
            ret = inc_step_with_tenants(stats_base, files, conns, bucket_names, config)
            files=ret[0]
            stats_last=ret[1]
            stats_base.set_shared_manifest_src += stats_last.set_shared_manifest_src
            stats_base.dup_head_size       += stats_last.dup_head_size
            stats_base.deduped_obj         += stats_last.deduped_obj
            stats_base.deduped_obj_bytes   += stats_last.deduped_obj_bytes
            stats_base.set_sha256          += stats_last.set_sha256
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
#                                 DRY RUN TESTS
#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_small_with_tenants():
    #return

    log.debug("test_dedup_dry_small_with_tenants: connect to AWS ...")
    prepare_test()
    max_copies_count=3
    files=[]
    num_files=10 # [4KB-4MB]
    base_size = 4*KB
    log.debug("generate files: base size=%d KiB, max_size=%d KiB",
             base_size/KB, (pow(2, num_files) * base_size)/KB)
    try:
        gen_files(files, base_size, num_files, max_copies_count)
        indices=[0] * len(files)
        ret=gen_connections_multi2(max_copies_count)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]

        ret=upload_objects_multi(files, conns, bucket_names, indices, default_config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        # expected stats for small objects - all zeros except for skip_too_small
        small_objs_dedup_stats = Dedup_Stats()
        copy_potential_stats(small_objs_dedup_stats, dedup_stats)
        small_objs_dedup_stats.size_before_dedup=dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small_bytes=dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small=s3_objects_total
        assert small_objs_dedup_stats == dedup_stats
        dry_run=True
        exec_dedup(dedup_stats, dry_run)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_multipart():
    #return

    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_dry_multipart: connect to AWS ...")
    conn=get_single_connection()
    files=[]

    num_files=8
    min_size=MULTIPART_SIZE
    # create files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on RADOS_OBJ_SIZE
    # create files in range [MULTIPART_SIZE, 1GB] aligned on RADOS_OBJ_SIZE
    gen_files_in_range(files, num_files, min_size, 1024*MB)

    # add files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on MULTIPART_SIZE
    gen_files_in_range(files, num_files, min_size, min_size*8, MULTIPART_SIZE)

    # add file with excatly MULTIPART_SIZE
    write_random(files, MULTIPART_SIZE, 2, 2)

    simple_dedup(conn, files, bucket_name, True, default_config, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_basic():
    #return

    prepare_test()
    bucket_name = gen_bucket_name()
    log.debug("test_dedup_dry_basic: connect to AWS ...")
    conn=get_single_connection()
    files=[]
    num_files=5
    base_size = 2*MB
    log.debug("generate files: base size=%d MiB, max_size=%d MiB",
             base_size/MB, (pow(2, num_files) * base_size)/MB)
    gen_files(files, base_size, num_files)
    log.debug("call simple_dedup()")
    simple_dedup(conn, files, bucket_name, True, default_config, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_small_multipart():
    #return

    prepare_test()
    log.debug("test_dedup_dry_small_multipart: connect to AWS ...")
    config2 = TransferConfig(multipart_threshold=4*KB, multipart_chunksize=1*MB)
    conn=get_single_connection()
    files=[]
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(Bucket=bucket_name)
    num_files = 10
    min_size = 4*KB
    max_size = 512*KB

    # create files in range [4KB-512KB] aligned on 4KB
    gen_files_in_range(files, num_files, min_size, max_size, min_size)
    simple_dedup(conn, files, bucket_name, True, config2, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_small():
    #return

    bucket_name = gen_bucket_name()
    log.debug("test_dedup_dry_small: connect to AWS ...")
    conn=get_single_connection()
    small_single_part_objs_dedup(conn, bucket_name, True)


#-------------------------------------------------------------------------------
# 1) generate a mix of small and large random files and store them on disk
# 2) upload a random number of copies from each file to bucket
# 3) execute DEDUP!!
# 4) Read dedup stat-counters:
# 5) verify that objects smaller than RADOS_OBJ_SIZE were skipped
# 6) verify that dedup ratio is reported correctly
@pytest.mark.basic_test
def test_dedup_dry_small_large_mix():
    #return

    dry_run=True
    log.debug("test_dedup_dry_small_large_mix: connect to AWS ...")
    prepare_test()

    num_threads=4
    max_copies_count=3
    small_file_size=1*MB
    mid_file_size=8*MB
    large_file_size=16*MB
    num_small_files=128
    num_mid_files=32
    num_large_files=16
    files=[]
    conns=[]
    bucket_names=get_buckets(num_threads)
    try:
        gen_files_fixed_size(files, num_small_files, small_file_size, max_copies_count)
        gen_files_fixed_size(files, num_mid_files, mid_file_size, max_copies_count)
        gen_files_fixed_size(files, num_large_files, large_file_size, max_copies_count)

        start = time.time_ns()
        conns=get_connections(num_threads)
        for i in range(num_threads):
            conns[i].create_bucket(Bucket=bucket_names[i])

        indices = [0] * len(files)
        ret=procs_upload_objects(files, conns, bucket_names, indices, default_config)
        upload_time_sec = (time.time_ns() - start) / (1000*1000*1000)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]
        log.debug("obj_count=%d, upload_time=%d(sec)", s3_objects_total,
                 upload_time_sec)
        exec_dedup(dedup_stats, dry_run)
        if dry_run == False:
            verify_objects(bucket_name, files, conn, expected_results, default_config)
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_basic_with_tenants():
    #return

    prepare_test()
    max_copies_count=3
    num_files=23
    file_size=33*MB
    files=[]
    log.debug("test_dedup_basic_with_tenants: connect to AWS ...")
    gen_files_fixed_size(files, num_files, file_size, max_copies_count)
    dedup_basic_with_tenants_common(files, max_copies_count, default_config, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_multipart_with_tenants():
    #return

    prepare_test()
    log.debug("test_dedup_dry_multipart_with_tenants: connect to AWS ...")
    max_copies_count=3
    num_files=8
    files=[]
    min_size=MULTIPART_SIZE
    # create files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on RADOS_OBJ_SIZE
    gen_files_in_range(files, num_files, min_size, min_size*32)

    # add files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on MULTIPART_SIZE
    gen_files_in_range(files, num_files, min_size, min_size*8, MULTIPART_SIZE)

    # add file with excatly MULTIPART_SIZE
    write_random(files, MULTIPART_SIZE, 2, 2)

    dedup_basic_with_tenants_common(files, max_copies_count, default_config, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_small_multipart_with_tenants():
    #return

    prepare_test()
    max_copies_count=4
    num_files=10
    min_size=4*KB
    max_size=512*KB
    files=[]
    config=TransferConfig(multipart_threshold=min_size, multipart_chunksize=1*MB)
    log.debug("test_dedup_small_multipart_with_tenants: connect to AWS ...")

    # create files in range [4KB-512KB] aligned on 4KB
    gen_files_in_range(files, num_files, min_size, max_size, min_size)
    dedup_basic_with_tenants_common(files, max_copies_count, config, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_large_scale_with_tenants():
    #return

    prepare_test()
    max_copies_count=3
    num_threads=64
    num_files=32*1024
    size=1*KB
    files=[]
    config=TransferConfig(multipart_threshold=size, multipart_chunksize=1*MB)
    log.debug("test_dedup_dry_large_scale_with_tenants: connect to AWS ...")
    gen_files_fixed_size(files, num_files, size, max_copies_count)
    threads_dedup_basic_with_tenants_common(files, num_threads, config, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_dry_large_scale():
    #return

    prepare_test()
    max_copies_count=3
    num_threads=64
    num_files=32*1024
    size=1*KB
    files=[]
    config=TransferConfig(multipart_threshold=size, multipart_chunksize=1*MB)
    log.debug("test_dedup_dry_large_scale_new: connect to AWS ...")
    gen_files_fixed_size(files, num_files, size, max_copies_count)
    conns=get_connections(num_threads)
    bucket_names=get_buckets(num_threads)
    for i in range(num_threads):
        conns[i].create_bucket(Bucket=bucket_names[i])
    try:
        threads_simple_dedup_with_tenants(files, conns, bucket_names, config, True)
    except:
        log.warning("test_dedup_dry_large_scale: failed!!")
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(bucket_names, conns)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_cleanup():
    close_all_connections()

