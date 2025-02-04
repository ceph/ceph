import logging
import random
import math
import time
import subprocess
import os
import string
import shutil
import pytest
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
    skip_shared_manifest: int = 0
    skip_singleton: int = 0
    skip_singleton_bytes: int = 0
    skip_src_record: int = 0
    total_processed_objects: int = 0
    size_before_dedup: int = 0
    loaded_objects: int = 0
    set_shared_manifest : int = 0
    deduped_obj: int = 0
    singleton_obj : int = 0
    unique_obj : int = 0
    duplicate_bytes : int = 0
    duplicate_obj : int = 0
    deduped_obj_bytes : int = 0

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
    #log.info('running command: %s', ' '.join(cmd))
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
    return run_prefix + '-' + str(num_buckets)


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


#-----------------------------------------------
def connection():
    hostname = get_config_host()
    port_no = get_config_port()
    access_key = get_access_key()
    secret_key = get_secret_key()
    if port_no == 443 or port_no == 8443:
        scheme = 'https://'
    else:
        scheme = 'http://'

    client = boto3.client('s3',
                          endpoint_url=scheme+hostname+':'+str(port_no),
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)

    return client

#####################
# dedup tests
#####################
OUT_DIR="/tmp/dedup/"
KB=(1024)
MB=(1024*KB)
RADOS_OBJ_SIZE=(4*MB)
MULTIPART_SIZE=(16*MB)
default_config = TransferConfig(multipart_threshold=MULTIPART_SIZE, multipart_chunksize=MULTIPART_SIZE)


#-------------------------------------------------------------------------------
def write_file(out_dir, filename, size):
    full_filename = out_dir + filename

    fout = open(full_filename, "wb")
    fout.write(os.urandom(size))
    fout.close()


#-------------------------------------------------------------------------------
def print_size(caller, size):
    if (size < MB):
        log.info("%s::size=%.2f KiB (%d Bytes)", caller, size/KB, size)
    else:
        log.info("%s::size=%.2f MiB", caller, size/MB)


#-------------------------------------------------------------------------------
def write_random(out_dir, files, size, min_copies_count=1, max_copies_count=4):
    global num_files
    assert(max_copies_count <= 4)
    num_files += 1
    filename = "OBJ_" + str(num_files) + str(size)
    copies_count=random.randint(min_copies_count, max_copies_count)
    files.append((filename, size, copies_count))
    write_file(out_dir, filename, size)


#-------------------------------------------------------------------------------
def gen_files_fixed_size(out_dir, files, count, size, max_copies_count=4):
    global num_files

    for i in range(0, count):
        copies_count=random.randint(1, max_copies_count)
        num_files += 1
        filename = "OBJ_" + str(num_files)

        files.append((filename, size, copies_count))
        log.debug("gen_files_fixed_size:%s, %d, %d", filename, size, copies_count)
        write_file(out_dir, filename, size)


#-------------------------------------------------------------------------------
def gen_files_in_range(out_dir, files, count, min_size, max_size, alignment=RADOS_OBJ_SIZE):
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
        write_random(out_dir, files, size_aligned, 2, 3)
        write_random(out_dir, files, size, 1, 3)


#-------------------------------------------------------------------------------
def gen_files(out_dir, files, start_size, factor, max_copies_count=4):
    size = start_size
    for i in range(1, factor+1):
        size2 = size + random.randint(1, size-1)
        write_random(out_dir, files, size, 1, max_copies_count)
        write_random(out_dir, files, size2, 1, max_copies_count)
        size  = size * 2;


#-------------------------------------------------------------------------------
def count_space_in_all_buckets():
    poolname = 'default.rgw.buckets.data'
    result = rados(['df'])
    assert result[1] == 0
    log.info("=============================================")
    for line in result[0].splitlines():
        if line.startswith(poolname):
            log.info(line[:45])
        elif line.startswith("POOL_NAME"):
            log.info(line[:45])
            log.info("=============================================")


#-------------------------------------------------------------------------------
def count_objects_in_bucket(bucket_name, conn):
    max_keys=1000
    marker=""
    obj_count=0
    while True:
        log.info("bucket_name=%s", bucket_name)
        listing=conn.list_objects(Bucket=bucket_name, Marker=marker, MaxKeys=max_keys)
        if 'Contents' not in listing or len(listing['Contents'])== 0:
            return 0

        obj_count += len(listing['Contents'])

        if listing['IsTruncated']:
            marker=listing['NextMarker']
            log.info("marker=%s, obj_count=%d", marker, obj_count)
            continue
        else:
            return obj_count


#-------------------------------------------------------------------------------
def count_object_parts_in_all_buckets(verbose=False):
    poolname = 'default.rgw.buckets.data'
    result = rados(['lspools'])
    assert result[1] == 0
    found=False
    for pool in result[0].split():
        if pool == poolname:
            found=True
            log.debug("Pool %s was found", poolname)
            break

    if found == False:
        log.debug("Pool %s doesn't exists!", poolname)
        return (0, 0)

    result = rados(['ls', '- -l', '-p ', poolname])
    assert result[1] == 0

    names=result[0].split()
    total_size=0
    count = 0
    for name in names:
        #log.info(name)
        count = count + 1
        size = 0
        for token in name.rsplit("::", maxsplit=1):
            size=token

        total_size += int(size)

    if verbose:
        log.info("Pool has %d rados objects", count)
        log.info("Pool size is %d Bytes (%.2f MiB)", total_size, total_size/MB)

    return (count, total_size)


#-------------------------------------------------------------------------------
def cleanup_local(out_dir):
    if os.path.isdir(out_dir):
        log.debug("Removing old directory " + out_dir)
        shutil.rmtree(out_dir)
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
            log.info("Bucket '%s' is empty, skipping...", bucket_name)
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
            log.info("marker=%s, obj_count=%d", marker, obj_count)
            continue
        else:
            break

    log.debug("Removing Bucket '%s', obj_count=%d", bucket_name, obj_count)
    conn.delete_bucket(Bucket=bucket_name)

#-------------------------------------------------------------------------------
def verify_pool_is_empty():
    result = admin(['gc', 'process', '--include-all'])
    assert result[1] == 0
    assert count_object_parts_in_all_buckets() == (0, 0)


#-------------------------------------------------------------------------------
def cleanup(out_dir, bucket_name, conn):
    if cleanup_local(out_dir):
        log.debug("delete_all_objects for bucket <%s>",bucket_name)
        delete_bucket_with_all_objects(bucket_name, conn)

    verify_pool_is_empty()


#-------------------------------------------------------------------------------
def cleanup_all_buckets(out_dir, bucket_names, conns):
    if cleanup_local(out_dir):
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
    threshold = config.multipart_threshold
    # Objects with size bigger than MULTIPART_SIZE are uploaded as multi-part
    # multi-part objects got a zero size Head objects
    if obj_size >= threshold:
        dedupable_space = obj_size
    elif obj_size > RADOS_OBJ_SIZE:
        dedupable_space = obj_size - RADOS_OBJ_SIZE
    else:
        dedupable_space = 0

    log.debug("obj_size=%.2f MiB, dedupable_space=%.2f MiB",
              float(obj_size)/MB, float(dedupable_space)/MB)
    return dedupable_space

BLOCK_SIZE=4096
#-------------------------------------------------------------------------------
def calc_expected_stats(dedup_stats, obj_size, num_copies, config):
    threshold = config.multipart_threshold
    dedup_stats.skip_shared_manifest = 0
    dedup_stats.size_before_dedup += (obj_size * num_copies)
    if obj_size <= RADOS_OBJ_SIZE and threshold > RADOS_OBJ_SIZE:
        dedup_stats.skip_too_small += num_copies
        return

    dedup_stats.total_processed_objects += num_copies
    dedup_stats.loaded_objects += num_copies

    if num_copies == 1:
        dedup_stats.singleton_obj += 1
        dedup_stats.skip_singleton += 1
        dedup_stats.skip_singleton_bytes += obj_size
    else:
        dedup_stats.skip_src_record += 1
        dedup_stats.set_shared_manifest += 1
        dedup_stats.unique_obj += 1
        dups_count = (num_copies - 1)
        dedup_stats.duplicate_obj += dups_count
        dedup_stats.deduped_obj += dups_count
        deduped_obj_bytes=calc_dedupable_space(obj_size, config)
        dedup_stats.deduped_obj_bytes += (deduped_obj_bytes * dups_count)
        # roundup to next 4KB
        #blocks_bytes = ((obj_size+BLOCK_SIZE-1)//BLOCK_SIZE)*BLOCK_SIZE
        #deduped_block_bytes=calc_dedupable_space(blocks_bytes, config)
        deduped_block_bytes=((deduped_obj_bytes+BLOCK_SIZE-1)//BLOCK_SIZE)*BLOCK_SIZE
        dedup_stats.duplicate_bytes += (deduped_block_bytes * dups_count)


#-------------------------------------------------------------------------------
def calc_expected_results(files, config):
    total_space=0
    duplicated_space=0
    duplicated_tail_objs=0
    rados_objects_total=0

    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        assert(obj_size)

        if num_copies > 0:
            log.debug("calc_expected_results::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
            total_space += (obj_size * num_copies)
            dedupable_space=calc_dedupable_space(obj_size, config)
            duplicated_space += ((num_copies-1) * dedupable_space)
            rados_obj_count=calc_rados_obj_count(num_copies, obj_size, config)
            rados_objects_total += (rados_obj_count * num_copies)
            duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))

    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.debug("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)
    expcted_space_post_dedup=(total_space-duplicated_space)
    log.debug("Post dedup expcted data in pool = %.2f MiB", expcted_space_post_dedup/MB)

    return (expected_rados_obj_count_post_dedup, expcted_space_post_dedup)


#-------------------------------------------------------------------------------
def upload_objects(out_dir, bucket_name, files, indices, conn, config, check_obj_count=True):
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
        dedupable_space=calc_dedupable_space(obj_size, config)
        duplicated_space += ((num_copies-1) * dedupable_space)
        rados_obj_count=calc_rados_obj_count(num_copies, obj_size, config)
        rados_objects_total += (rados_obj_count * num_copies)
        duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))
        log.debug("upload_objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
        s3_objects_total += num_copies
        if s3_objects_total and (s3_objects_total % 1000 == 0):
            log.info("%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
                     s3_objects_total, rados_objects_total, total_space/MB)
        for i in range(idx, num_copies):
            key = gen_object_name(filename, i)
            #log.info("upload_file %s/%s with crc32", bucket_name, key)
            conn.upload_file(out_dir + filename, bucket_name, key, Config=config, ExtraArgs={'ChecksumAlgorithm': 'crc32'})
            #conn.upload_file(out_dir + filename, bucket_name, key, Config=config, ExtraArgs={'ChecksumType': 'crc32'})
            #conn.upload_file(out_dir + filename, bucket_name, key, Config=config)

    log.debug("==========================================")
    log.info("Summery:\n%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
             s3_objects_total, rados_objects_total, total_space/MB)
    log.debug("Based on calculation we should have %d rados objects", rados_objects_total)
    log.debug("Based on calculation we should have %d duplicated tail objs", duplicated_tail_objs)
    log.info("Based on calculation we should have %.2f MiB total in pool", total_space/MB)
    log.info("Based on calculation we should have %.2f MiB duplicated space in pool", duplicated_space/MB)

    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.debug("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)
    expcted_space_post_dedup=(total_space-duplicated_space)
    log.info("Post dedup expcted data in pool = %.2f MiB", expcted_space_post_dedup/MB)
    if check_obj_count:
        assert (rados_objects_total, total_space) == count_object_parts_in_all_buckets()

    dedup_stats.size_before_dedup=total_space
    #dedup_stats.duplicate_bytes=duplicated_space
    expected_results=(expected_rados_obj_count_post_dedup, expcted_space_post_dedup)
    return (expected_results, dedup_stats, s3_objects_total)


#-------------------------------------------------------------------------------
def upload_objects_multi(out_dir, files, conns, bucket_names, indices, config, check_obj_count=True):
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
        dedupable_space=calc_dedupable_space(obj_size, config)
        duplicated_space += ((num_copies-1) * dedupable_space)
        rados_obj_count=calc_rados_obj_count(num_copies, obj_size, config)
        rados_objects_total += (rados_obj_count * num_copies)
        duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))
        log.debug("upload_objects::%s::size=%d, num_copies=%d", filename, obj_size, num_copies);
        s3_objects_total += num_copies
        if s3_objects_total and (s3_objects_total % 1000 == 0):
            log.info("%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
                     s3_objects_total, rados_objects_total, total_space/MB)
        for i in range(idx, num_copies):
            ten_id = i % max_tenants
            key = gen_object_name(filename, i)
            conns[ten_id].upload_file(out_dir + filename, bucket_names[ten_id], key, Config=config)
            log.debug("upload_objects::<%s/%s>", bucket_names[ten_id], key)

    log.debug("==========================================")
    log.info("Summery:%d S3 objects were uploaded (%d rados objects), total size = %.2f MiB",
             s3_objects_total, rados_objects_total, total_space/MB)
    log.debug("Based on calculation we should have %d rados objects", rados_objects_total)
    log.debug("Based on calculation we should have %d duplicated tail objs", duplicated_tail_objs)
    log.debug("Based on calculation we should have %.2f MiB total in pool", total_space/MB)
    log.debug("Based on calculation we should have %.2f MiB duplicated space in pool", duplicated_space/MB)

    s3_object_count=0
    for (bucket_name, conn) in zip(bucket_names, conns):
        s3_object_count += count_objects_in_bucket(bucket_name, conn)

    log.info("bucket listings reported a total of %d s3 objects", s3_object_count)
    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.debug("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)
    expcted_space_post_dedup=(total_space-duplicated_space)
    log.debug("Post dedup expcted data in pool = %.2f MiB", expcted_space_post_dedup/MB)
    if check_obj_count:
        assert (rados_objects_total, total_space) == count_object_parts_in_all_buckets()
        assert (s3_object_count == s3_objects_total)

    dedup_stats.size_before_dedup=total_space
    #dedup_stats.duplicate_bytes=duplicated_space
    expected_results=(expected_rados_obj_count_post_dedup, expcted_space_post_dedup)
    return (expected_results, dedup_stats, s3_objects_total)


#-------------------------------------------------------------------------------
def verify_objects(out_dir, bucket_name, files, conn, expected_results, config):
    tempfile = out_dir + "temp"
    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        log.debug("comparing file=%s, size=%d, copies=%d", filename, obj_size, num_copies)
        for i in range(0, num_copies):
            key = gen_object_name(filename, i)
            #log.info("download_file(%s) with crc32", key)
            conn.download_file(bucket_name, key, tempfile, Config=config, ExtraArgs={'ChecksumMode': 'crc32'})
            #conn.download_file(bucket_name, key, tempfile, Config=config)
            result = bash(['cmp', tempfile, out_dir + filename])
            assert result[1] == 0 ,"Files %s and %s differ!!" % (key, tempfile)
            os.remove(tempfile)

    assert expected_results == count_object_parts_in_all_buckets(True)
    log.info("verify_objects::completed successfully!!")


#-------------------------------------------------------------------------------
def verify_objects_multi(out_dir, files, conns, bucket_names, expected_results, config):
    max_tenants=len(conns)
    tempfile = out_dir + "temp"
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
            result = bash(['cmp', tempfile, out_dir + filename])
            assert result[1] == 0 ,"Files %s and %s differ!!" % (key, tempfile)
            os.remove(tempfile)

    assert expected_results == count_object_parts_in_all_buckets(True)
    log.info("verify_objects::completed successfully!!")


#-------------------------------------------------------------------------------
def get_stats_line_val(line):
    return int(line.rsplit("=", maxsplit=1)[1].strip())


#-------------------------------------------------------------------------------
def print_dedup_stats(dedup_stats):
    for key in dedup_stats.__dict__:
        log.info("dedup_stats[%s] = %d", key, dedup_stats.__dict__[key])


#-------------------------------------------------------------------------------
def print_dedup_stats_diff(actual, expected):
    for (key1, key2) in zip(actual.__dict__, expected.__dict__):
        if (actual.__dict__[key1] != expected.__dict__[key2]):
            log.error("actual[%s] = %d != expected[%s] = %d",
                      key1, actual.__dict__[key1], key2, expected.__dict__[key2])


#-------------------------------------------------------------------------------
def read_dedup_stats():
    result = admin(['dedup', 'stats'])
    assert result[1] == 0

    dedup_work_was_completed = False
    dedup_stats = Dedup_Stats()
    for line in result[0].splitlines():
        if line.startswith("Total processed objects"):
            dedup_stats.total_processed_objects = get_stats_line_val(line)

        if line.startswith("Accum byte size Ingress Objs"):
            dedup_stats.size_before_dedup = get_stats_line_val(line)

        if line.startswith("Duplicate Blocks Bytes"):
            dedup_stats.duplicate_bytes = get_stats_line_val(line)

        if line.startswith("Loaded objects"):
            dedup_stats.loaded_objects = get_stats_line_val(line)

        if line.startswith("Ingress skip: too small objs"):
            dedup_stats.skip_too_small = get_stats_line_val(line)

        if line.startswith("Skipped shared_manifest"):
            dedup_stats.skip_shared_manifest = get_stats_line_val(line)

        elif line.startswith("Skipped singleton objs"):
            dedup_stats.skip_singleton = get_stats_line_val(line)

        if line.startswith("Skipped singleton Bytes"):
            dedup_stats.skip_singleton_bytes = get_stats_line_val(line)

        if line.startswith("Skipped source record"):
            dedup_stats.skip_src_record = get_stats_line_val(line)

        elif line.startswith("Set Shared-Manifest"):
            dedup_stats.set_shared_manifest = get_stats_line_val(line)

        if line.startswith("Deduped Bytes(this cycle)"):
            dedup_stats.deduped_obj_bytes = get_stats_line_val(line)

        if line.startswith("Deduped Obj (this cycle)"):
            dedup_stats.deduped_obj = get_stats_line_val(line)

        elif line.startswith("Singleton Obj"):
            dedup_stats.singleton_obj = get_stats_line_val(line)

        if line.startswith("Unique Obj"):
            dedup_stats.unique_obj = get_stats_line_val(line)

        elif line.startswith("Duplicate Obj"):
            dedup_stats.duplicate_obj = get_stats_line_val(line)

        elif line.startswith("DEDUP WORK WAS COMPLETED"):
            dedup_work_was_completed = True

    return (dedup_work_was_completed, dedup_stats)


#-------------------------------------------------------------------------------
def exec_dedup(expcted_dedup_stats, verify_stats=True):
    log.info("sending exec_dedup request")
    result = admin(['dedup', 'restart'])
    assert result[1] == 0
    log.info(result[0])
    log.info("wait for dedup to complete")

    # dedup should complete in less than 5 minutes
    max_dedup_time = 1*60
    if expcted_dedup_stats.total_processed_objects > 10000:
        max_dedup_time = 5 * 60
    
    dedup_time = 0
    dedup_timeout = 5
    dedup_stats = Dedup_Stats()
    wait_for_completion = True
    while wait_for_completion:
        assert dedup_time < max_dedup_time
        time.sleep(dedup_timeout)
        dedup_time += dedup_timeout
        ret = read_dedup_stats()
        if ret[0]:
            wait_for_completion = False
            dedup_stats = ret[1]
            log.info("dedup completed in %d seconds", dedup_time)

    if verify_stats == False:
        expcted_dedup_stats.skip_shared_manifest = dedup_stats.skip_shared_manifest

    if dedup_stats != expcted_dedup_stats:
        log.info("==================================================")
        print_dedup_stats_diff(dedup_stats, expcted_dedup_stats)
        log.info("==================================================\n")
        assert dedup_stats == expcted_dedup_stats

    log.info("expcted_dedup::stats check completed successfully!!")


#-------------------------------------------------------------------------------
def prepare_test(out_dir):
    cleanup_local(out_dir)
    #make sure we are starting with all buckets empty
    if count_object_parts_in_all_buckets() != (0, 0):
        log.warn("The system was left dirty from previous run");
        log.warn("Make sure to remove all objects before starting");

    assert count_object_parts_in_all_buckets() == (0, 0)
    os.mkdir(out_dir)


#-------------------------------------------------------------------------------
def small_single_part_objs_dedup(out_dir, conn, bucket_name, run_cleanup_after=True):
    # 1) generate small random files and store them on disk
    # 2) upload a random number of copies from each file to bucket
    # 3) execute DEDUP!!
    #    Read dedup stat-counters:
    #    5.a verify that objects smaller than RADOS_OBJ_SIZE were skipped
    prepare_test(out_dir)
    try:
        files=[]
        num_files = 10
        base_size = 4*KB
        log.info("generate files: base size=%d KiB, max_size=%d KiB",
                 base_size/KB, (pow(2, num_files) * base_size)/KB)
        gen_files(out_dir, files, base_size, num_files)
        bucket = conn.create_bucket(Bucket=bucket_name)
        log.info("upload objects to bucket <%s> ...", bucket_name)
        indices = [0] * len(files)
        ret = upload_objects(out_dir, bucket_name, files, indices, conn, default_config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        # expected stats for small objects - all zeros except for skip_too_small
        small_objs_dedup_stats = Dedup_Stats()
        small_objs_dedup_stats.size_before_dedup = dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small = s3_objects_total
        assert small_objs_dedup_stats == dedup_stats

        exec_dedup(dedup_stats)
        log.info("Verify all objects")
        verify_objects(out_dir, bucket_name, files, conn, expected_results, default_config)
    finally:
        # cleanup must be executed even after a failure
        if run_cleanup_after:
            cleanup(out_dir, bucket_name, conn)


#-------------------------------------------------------------------------------
def simple_dedup(out_dir, conn, files, bucket_name, run_cleanup_after, config):
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
        log.info("conn.create_bucket(%s)", bucket_name)
        bucket = conn.create_bucket(Bucket=bucket_name)
        indices = [0] * len(files)
        log.info("upload objects to bucket <%s> ...", bucket_name)
        ret = upload_objects(out_dir, bucket_name, files, indices, conn, config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        exec_dedup(dedup_stats)
        log.info("Verify all objects")
        verify_objects(out_dir, bucket_name, files, conn, expected_results, config)
        return ret
    finally:
        # cleanup must be executed even after a failure
        if run_cleanup_after:
            cleanup(out_dir, bucket_name, conn)

#-------------------------------------------------------------------------------
def gen_connections_multi(max_copies_count):
    global num_conns

    tenants=[]
    bucket_names=[]
    conns=[]
    log.info("gen_connections_multi: Create connection and buckets ...")
    suffix=run_prefix
    for i in range(0, max_copies_count):
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

    log.info("gen_connections_multi: All connection and buckets are set")
    return (tenants, bucket_names, conns)


#-------------------------------------------------------------------------------
def simple_dedup_with_tenants(out_dir, files, conns, bucket_names, config):
    indices=[0] * len(files)
    ret=upload_objects_multi(out_dir, files, conns, bucket_names, indices, config)
    expected_results = ret[0]
    dedup_stats = ret[1]
    exec_dedup(dedup_stats)
    log.info("Verify all objects")
    verify_objects_multi(out_dir, files, conns, bucket_names, expected_results, config)
    return ret


#-------------------------------------------------------------------------------
def dedup_basic_with_tenants_common(out_dir, files, max_copies_count, config, cleanup=True):
    try:
        ret=gen_connections_multi(max_copies_count)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]
        simple_dedup_with_tenants(OUT_DIR, files, conns, bucket_names, config)
    finally:
        if cleanup:
            cleanup_all_buckets(out_dir, bucket_names, conns)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small():
    #return

    bucket_name = gen_bucket_name()
    log.info("test_dedup_small: connect to AWS ...")
    conn = connection()
    small_single_part_objs_dedup(OUT_DIR, conn, bucket_name, True)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small_with_tenants():
    #return

    prepare_test(OUT_DIR)
    max_copies_count=3
    files=[]
    num_files=10 # [4KB-4MB]
    base_size = 4*KB
    log.info("generate files: base size=%d KiB, max_size=%d KiB",
             base_size/KB, (pow(2, num_files) * base_size)/KB)
    try:
        gen_files(OUT_DIR, files, base_size, num_files, max_copies_count)
        indices=[0] * len(files)
        ret=gen_connections_multi(max_copies_count)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]

        ret=upload_objects_multi(OUT_DIR, files, conns, bucket_names, indices, default_config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        # expected stats for small objects - all zeros except for skip_too_small
        small_objs_dedup_stats = Dedup_Stats()
        small_objs_dedup_stats.size_before_dedup=dedup_stats.size_before_dedup
        small_objs_dedup_stats.skip_too_small=s3_objects_total
        assert small_objs_dedup_stats == dedup_stats

        exec_dedup(dedup_stats)
        log.info("Verify all objects")
        verify_objects_multi(OUT_DIR, files, conns, bucket_names, expected_results, default_config)
    finally:
        cleanup_all_buckets(OUT_DIR, bucket_names, conns)


#------------------------------------------------------------------------------
# Trivial incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Run a second dedup *without making any change*
# 3) The stats-counters should show the same dedup ratio, but no change
#    should be made to the system
@pytest.mark.basic_test
def test_dedup_inc_0_with_tenants():
    #return

    prepare_test(OUT_DIR)
    log.info("test_dedup_inc_0: connect to AWS ...")
    max_copies_count=3
    config=default_config
    ret=gen_connections_multi(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files=11
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(OUT_DIR, files, conns, bucket_names, config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        dedup_stats2 = dedup_stats
        dedup_stats2.skip_shared_manifest = dedup_stats.set_shared_manifest + dedup_stats.deduped_obj
        dedup_stats2.skip_src_record = 0
        dedup_stats2.set_shared_manifest = 0
        dedup_stats2.deduped_obj = 0
        dedup_stats2.deduped_obj_bytes = 0

        log.info("test_dedup_inc_0_with_tenants: incremental dedup:")
        # run dedup again and make sure nothing has changed
        exec_dedup(dedup_stats2)
        verify_objects_multi(OUT_DIR, files, conns, bucket_names, expected_results, config)
    finally:
        cleanup_all_buckets(OUT_DIR, bucket_names, conns)


#------------------------------------------------------------------------------
# Trivial incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Run a second dedup *without making any change*
# 3) The stats-counters should show the same dedup ratio, but no change
#    should be made to the system
@pytest.mark.basic_test
def test_dedup_inc_0():
    #return

    config=default_config
    prepare_test(OUT_DIR)
    bucket_name = gen_bucket_name()
    log.info("test_dedup_inc_0: connect to AWS ...")
    conn = connection()
    try:
        files=[]
        num_files = 11
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(OUT_DIR, conn, files, bucket_name, False, config)
        expected_results = ret[0]
        dedup_stats = ret[1]
        s3_objects_total = ret[2]

        dedup_stats2 = dedup_stats
        dedup_stats2.skip_shared_manifest = dedup_stats.set_shared_manifest + dedup_stats.deduped_obj
        dedup_stats2.skip_src_record = 0
        dedup_stats2.set_shared_manifest = 0
        dedup_stats2.deduped_obj = 0
        dedup_stats2.deduped_obj_bytes = 0

        log.info("test_dedup_inc_0: incremental dedup:")
        # run dedup again and make sure nothing has changed
        exec_dedup(dedup_stats2)
        verify_objects(OUT_DIR, bucket_name, files, conn, expected_results, config)
    finally:
        cleanup(OUT_DIR, bucket_name, conn)


#-------------------------------------------------------------------------------
# Basic incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the *original objects*
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_1_with_tenants():
    #return

    prepare_test(OUT_DIR)
    log.info("test_dedup_inc_1_with_tenants: connect to AWS ...")
    max_copies_count=6
    config=default_config
    ret=gen_connections_multi(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files=17
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(OUT_DIR, files, conns, bucket_names, config)
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

        ret=upload_objects_multi(OUT_DIR, files_combined, conns, bucket_names, indices, config, False)
        expected_results=ret[0]
        stats_combined=ret[1]
        stats_combined.skip_shared_manifest = stats_base.set_shared_manifest + stats_base.deduped_obj
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.set_shared_manifest -= stats_base.set_shared_manifest
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        log.info("test_dedup_inc_1_with_tenants: incremental dedup:")
        # run dedup again
        exec_dedup(stats_combined)
        verify_objects_multi(OUT_DIR, files_combined, conns, bucket_names, expected_results, config)
    finally:
        cleanup_all_buckets(OUT_DIR, bucket_names, conns)


#-------------------------------------------------------------------------------
# Basic incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the *original objects*
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_1():
    #return

    config=default_config
    prepare_test(OUT_DIR)
    bucket_name = gen_bucket_name()
    log.info("test_dedup_inc_1: connect to AWS ...")
    conn = connection()
    try:
        files=[]
        num_files = 4
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(OUT_DIR, conn, files, bucket_name, False, config)
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

        ret=upload_objects(OUT_DIR, bucket_name, files_combined, indices, conn, config, False)
        expected_results = ret[0]
        stats_combined = ret[1]
        stats_combined.skip_shared_manifest = stats_base.set_shared_manifest + stats_base.deduped_obj
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.set_shared_manifest -= stats_base.set_shared_manifest
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        log.info("test_dedup_inc_1: incremental dedup:")
        # run dedup again
        exec_dedup(stats_combined)
        verify_objects(OUT_DIR, bucket_name, files_combined, conn, expected_results, config)
    finally:
        cleanup(OUT_DIR, bucket_name, conn)


#-------------------------------------------------------------------------------
# Simple incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the original objects
# 3) Add new objects to buckets
# 4) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_2_with_tenants():
    #return

    prepare_test(OUT_DIR)
    log.info("test_dedup_inc_2_with_tenants: connect to AWS ...")
    max_copies_count=6
    config=default_config
    ret=gen_connections_multi(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files = 17
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(OUT_DIR, files, conns, bucket_names, config)
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
        gen_files_in_range(OUT_DIR, files_combined, num_files_new, 2*MB, 32*MB)
        pad_count = len(files_combined) - len(files)
        for i in range(0, pad_count):
            indices.append(0)

        assert(len(indices) == len(files_combined))
        ret=upload_objects_multi(OUT_DIR, files_combined, conns, bucket_names, indices, config, False)
        expected_results = ret[0]
        stats_combined = ret[1]
        stats_combined.skip_shared_manifest = stats_base.set_shared_manifest + stats_base.deduped_obj
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.set_shared_manifest -= stats_base.set_shared_manifest
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        log.info("test_dedup_inc_2_with_tenants: incremental dedup:")
        # run dedup again
        exec_dedup(stats_combined)
        verify_objects_multi(OUT_DIR, files_combined, conns, bucket_names, expected_results, config)
    finally:
        cleanup_all_buckets(OUT_DIR, bucket_names, conns)


#-------------------------------------------------------------------------------
# Simple incremental dedup:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Add more copies of the original objects
# 3) Add new objects to buckets
# 4) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_2():
    #return

    config=default_config
    prepare_test(OUT_DIR)
    bucket_name = gen_bucket_name()
    log.info("test_dedup_inc_2: connect to AWS ...")
    conn = connection()
    try:
        files=[]
        num_files = 17
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(OUT_DIR, conn, files, bucket_name, False, config)
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
        gen_files_in_range(OUT_DIR, files_combined, num_files_new, 2*MB, 32*MB)
        pad_count = len(files_combined) - len(files)
        for i in range(0, pad_count):
            indices.append(0)

        assert(len(indices) == len(files_combined))
        ret=upload_objects(OUT_DIR, bucket_name, files_combined, indices, conn, config, False)
        expected_results = ret[0]
        stats_combined = ret[1]
        stats_combined.skip_shared_manifest = stats_base.set_shared_manifest + stats_base.deduped_obj
        stats_combined.skip_src_record     -= stats_base.skip_src_record
        stats_combined.set_shared_manifest -= stats_base.set_shared_manifest
        stats_combined.deduped_obj         -= stats_base.deduped_obj
        stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

        log.info("test_dedup_inc_2: incremental dedup:")
        # run dedup again
        exec_dedup(stats_combined)
        verify_objects(OUT_DIR, bucket_name, files_combined, conn, expected_results,
                       config)

    finally:
        cleanup(OUT_DIR, bucket_name, conn)


#-------------------------------------------------------------------------------
# Incremental dedup with object removal:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Remove copies of some objects
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_with_remove_multi_tenants():
    #return

    prepare_test(OUT_DIR)
    log.info("test_dedup_inc_with_remove_multi_tenants: connect to AWS ...")
    max_copies_count=6
    config=default_config
    ret=gen_connections_multi(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files = 17
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(OUT_DIR, files, conns, bucket_names, config)
        expected_results_base = ret[0]
        stats_base = ret[1]

        # REMOVE some objects and update stats/expected
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
        dedup_stats.set_shared_manifest=0
        dedup_stats.deduped_obj=0
        dedup_stats.deduped_obj_bytes=0
        dedup_stats.skip_src_record=0

        log.info("test_dedup_inc_with_remove: incremental dedup:")
        exec_dedup(dedup_stats, False)
        expected_results=calc_expected_results(files_sub, config)
        verify_objects_multi(OUT_DIR, files_sub, conns, bucket_names, expected_results, config)

    finally:
        cleanup_all_buckets(OUT_DIR, bucket_names, conns)


#-------------------------------------------------------------------------------
# Incremental dedup with object removal:
# 1) Run the @simple_dedup test above without cleanup post dedup
# 2) Remove copies of some objects
# 3) Run another dedup
@pytest.mark.basic_test
def test_dedup_inc_with_remove():
    #return

    config=default_config
    prepare_test(OUT_DIR)
    bucket_name = gen_bucket_name()
    log.info("test_dedup_inc_with_remove: connect to AWS ...")
    conn = connection()
    try:
        files=[]
        num_files = 17
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret = simple_dedup(OUT_DIR, conn, files, bucket_name, False, config)
        expected_results_base = ret[0]
        stats_base = ret[1]

        # REMOVE some objects and update stats/expected
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
                files_sub.append((filename, obj_size, num_copies_2))
                calc_expected_stats(dedup_stats, obj_size, num_copies_2, config)

            start_idx=num_copies_2
            for i in range(start_idx, num_copies):
                key = gen_object_name(filename, i)
                log.debug("delete key::%s::", key);
                object_keys.append(key)

            if len(object_keys) == 0:
                log.info("Skiping file=%s, num_remove=%d", filename, num_remove)
                continue

            response=conn.delete_objects(Bucket=bucket_name,
                                         Delete={"Objects": [{"Key": key} for key in object_keys]})
            # must call garbage collection for predictable count
            result = admin(['gc', 'process', '--include-all'])
            assert result[1] == 0

        # run dedup again
        dedup_stats.set_shared_manifest=0
        dedup_stats.deduped_obj=0
        dedup_stats.deduped_obj_bytes=0
        dedup_stats.skip_src_record=0

        log.info("test_dedup_inc_with_remove: incremental dedup:")
        log.info("stats_base.size_before_dedup=%d", stats_base.size_before_dedup)
        log.info("dedup_stats.size_before_dedup=%d", dedup_stats.size_before_dedup)

        exec_dedup(dedup_stats, False)
        expected_results=calc_expected_results(files_sub, config)
        verify_objects(OUT_DIR, bucket_name, files_sub, conn, expected_results,
                       config)

    finally:
        cleanup(OUT_DIR, bucket_name, conn)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_multipart_with_tenants():
    #return

    prepare_test(OUT_DIR)
    log.info("test_dedup_multipart_with_tenants: connect to AWS ...")
    max_copies_count=3
    num_files=8
    files=[]
    min_size=MULTIPART_SIZE
    # create files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on RADOS_OBJ_SIZE
    gen_files_in_range(OUT_DIR, files, num_files, min_size, min_size*8)

    # add files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on MULTIPART_SIZE
    gen_files_in_range(OUT_DIR, files, num_files, min_size, min_size*8, MULTIPART_SIZE)

    # add file with excatly MULTIPART_SIZE
    write_random(OUT_DIR, files, MULTIPART_SIZE, 2, 2)

    dedup_basic_with_tenants_common(OUT_DIR, files, max_copies_count, default_config)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_multipart():
    #return

    prepare_test(OUT_DIR)
    bucket_name = gen_bucket_name()
    log.info("test_dedup_multipart: connect to AWS ...")
    conn = connection()
    files=[]

    num_files=8
    min_size=MULTIPART_SIZE
    # create files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on RADOS_OBJ_SIZE
    gen_files_in_range(OUT_DIR, files, num_files, min_size, min_size*8)

    # add files in range [MULTIPART_SIZE, 4*MULTIPART_SIZE] aligned on MULTIPART_SIZE
    gen_files_in_range(OUT_DIR, files, num_files, min_size, min_size*8, MULTIPART_SIZE)

    # add file with excatly MULTIPART_SIZE
    write_random(OUT_DIR, files, MULTIPART_SIZE, 2, 2)

    simple_dedup(OUT_DIR, conn, files, bucket_name, True, default_config)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_basic_with_tenants():
    #return

    prepare_test(OUT_DIR)
    max_copies_count=3
    num_files=23
    file_size=33*MB
    files=[]
    log.info("test_dedup_basic_with_tenants: connect to AWS ...")
    gen_files_fixed_size(OUT_DIR, files, num_files, file_size, max_copies_count)
    dedup_basic_with_tenants_common(OUT_DIR, files, max_copies_count, default_config)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_basic():
    #return

    prepare_test(OUT_DIR)
    bucket_name = gen_bucket_name()
    log.info("test_dedup_basic: connect to AWS ...")
    conn = connection()
    files=[]
    num_files=5
    base_size = MULTIPART_SIZE
    log.info("generate files: base size=%d MiB, max_size=%d MiB",
             base_size/MB, (pow(2, num_files) * base_size)/MB)
    gen_files(OUT_DIR, files, base_size, num_files)
    log.info("call simple_dedup()")
    simple_dedup(OUT_DIR, conn, files, bucket_name, True, default_config)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small_multipart_with_tenants():
    #return

    prepare_test(OUT_DIR)
    max_copies_count=4
    num_files=10
    min_size=4*KB
    max_size=512*KB
    files=[]
    config=TransferConfig(multipart_threshold=min_size, multipart_chunksize=1*MB)
    log.info("test_dedup_small_multipart_with_tenants: connect to AWS ...")

    # create files in range [4KB-512KB] aligned on 4KB
    gen_files_in_range(OUT_DIR, files, num_files, min_size, max_size, min_size)
    dedup_basic_with_tenants_common(OUT_DIR, files, max_copies_count, config)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_small_multipart():
    #return

    prepare_test(OUT_DIR)
    log.info("test_dedup_small_multipart: connect to AWS ...")
    config2 = TransferConfig(multipart_threshold=4*KB, multipart_chunksize=1*MB)
    conn = connection()
    out_dir=OUT_DIR
    files=[]
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(Bucket=bucket_name)
    num_files = 10
    min_size = 4*KB
    max_size = 512*KB

    # create files in range [4KB-512KB] aligned on 4KB
    gen_files_in_range(OUT_DIR, files, num_files, min_size, max_size, min_size)
    simple_dedup(OUT_DIR, conn, files, bucket_name, True, config2)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_large_scale_with_tenants():
    #return

    prepare_test(OUT_DIR)
    max_copies_count=3
    num_files=1*1024
    size=1*KB
    files=[]
    config=TransferConfig(multipart_threshold=size, multipart_chunksize=1*MB)
    log.info("test_dedup_large_scale: connect to AWS ...")
    gen_files_fixed_size(OUT_DIR, files, num_files, size, max_copies_count)
    dedup_basic_with_tenants_common(OUT_DIR, files, max_copies_count, config)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_large_scale():
    return

    start = time.time_ns()
    prepare_test(OUT_DIR)
    log.info("test_dedup_large_scale: connect to AWS ...")
    config2 = TransferConfig(multipart_threshold=4*KB, multipart_chunksize=1*MB)
    conn = connection()
    out_dir=OUT_DIR
    files=[]
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(Bucket=bucket_name)
    num_files = 1024
    size = 4*KB

    gen_files_fixed_size(OUT_DIR, files, num_files, size)
    simple_dedup(OUT_DIR, conn, files, bucket_name, True, config2)
    end = time.time_ns()
    log.info("test_dedup_large_scale::elapsed time = %d msec",
             (end - start)/(1000*1000));


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_empty_bucket():
    #return

    prepare_test(OUT_DIR)
    log.info("test_empty_bucket: connect to AWS ...")

    max_copies_count=2
    config = default_config

    files=[]
    try:
        ret=gen_connections_multi(max_copies_count)
        tenants=ret[0]
        bucket_names=ret[1]
        conns=ret[2]
    finally:
        # cleanup must be executed even after a failure
        cleanup_all_buckets(OUT_DIR, bucket_names, conns)


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
    gen_files_in_range(OUT_DIR, files_combined, num_files_new, 2*MB, 32*MB)
    pad_count = len(files_combined) - len(files)
    for i in range(0, pad_count):
        indices.append(0)

    assert(len(indices) == len(files_combined))
    ret=upload_objects_multi(OUT_DIR, files_combined, conns, bucket_names, indices, config, False)
    expected_results = ret[0]
    stats_combined = ret[1]
    stats_combined.skip_shared_manifest = stats_base.set_shared_manifest + stats_base.deduped_obj
    stats_combined.skip_src_record     -= stats_base.skip_src_record
    stats_combined.set_shared_manifest -= stats_base.set_shared_manifest
    stats_combined.deduped_obj         -= stats_base.deduped_obj
    stats_combined.deduped_obj_bytes   -= stats_base.deduped_obj_bytes

    log.info("test_dedup_inc_2_with_tenants: incremental dedup:")
    # run dedup again
    exec_dedup(stats_combined)
    verify_objects_multi(OUT_DIR, files_combined, conns, bucket_names, expected_results, config)

    return (files_combined, stats_combined)


#-------------------------------------------------------------------------------
@pytest.mark.basic_test
def test_dedup_inc_loop_with_tenants():
    #return

    prepare_test(OUT_DIR)
    log.info("test_dedup_inc_loop_with_tenants: connect to AWS ...")
    max_copies_count=3
    config=default_config
    ret=gen_connections_multi(max_copies_count)
    tenants=ret[0]
    bucket_names=ret[1]
    conns=ret[2]
    try:
        files=[]
        num_files = 13
        # gen_files_in_range creates 2-3 copies
        gen_files_in_range(OUT_DIR, files, num_files, 1*MB, 64*MB)
        # upload objects, dedup, verify, but don't cleanup
        ret=simple_dedup_with_tenants(OUT_DIR, files, conns, bucket_names, config)
        stats_base=ret[1]

        for idx in range(0, 3):
            log.info("test_dedup_inc_loop_with_tenants: INC-STEP %d", idx)
            ret = inc_step_with_tenants(stats_base, files, conns, bucket_names, config)
            files=ret[0]
            stats_last=ret[1]
            stats_base.skip_src_record     += stats_last.skip_src_record
            stats_base.set_shared_manifest += stats_last.set_shared_manifest
            stats_base.deduped_obj         += stats_last.deduped_obj
            stats_base.deduped_obj_bytes   += stats_last.deduped_obj_bytes
    finally:
        cleanup_all_buckets(OUT_DIR, bucket_names, conns)


