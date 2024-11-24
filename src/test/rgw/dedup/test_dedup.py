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

from . import(
    configfile,
    get_config_host,
    get_config_port,
    get_access_key,
    get_secret_key
)

# configure logging for the tests module
log = logging.getLogger(__name__)
num_buckets = 0
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
MB=(1024*1024)
RADOS_OBJ_SIZE=(4*MB)
MULTIPART_SIZE=(16*MB)
config = TransferConfig(multipart_threshold=MULTIPART_SIZE, multipart_chunksize=MULTIPART_SIZE)

#-----------------------------------------------
def write_random(out_dir, size, files):
    # when uploading 2 identical objects with size set to MULTIPART_SIZE
    # boto3 generates differnt etag for the each object -> skip it for now
    if size == MULTIPART_SIZE:
        return

    filename = "OBJ_" + str(size)
    log.debug(filename)
    count=random.randint(1, 4)
    files.append((filename, size, count))
    full_filename = out_dir + filename

    fout = open(full_filename, "wb")
    fout.write(os.urandom(size))
    fout.close()

#-----------------------------------------------
def gen_files(out_dir, start_size, factor, files):
    size = start_size
    for i in range(1, factor+1):
        size2 = size + random.randint(1, RADOS_OBJ_SIZE-1)
        write_random(out_dir, size, files)
        write_random(out_dir, size2, files)
        size  = size * 2;
        log.debug("==============================")

#-----------------------------------------------
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

#-----------------------------------------------
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
        count = count + 1
        size = 0
        for token in name.rsplit("::", maxsplit=1):
            size=token

        total_size += int(size)

    if verbose:
        log.info("Pool has %d rados objects", count)
        log.info("Pool size is %d Bytes (%.2f MiB)", total_size, total_size/MB)

    return (count, total_size)

#-----------------------------------------------
def cleanup_local(out_dir):
    if os.path.isdir(out_dir):
        log.debug("Removing old directory " + out_dir)
        shutil.rmtree(out_dir)

#-----------------------------------------------
def delete_all_objects(conn, bucket_name):
    objects = []
    for key in conn.list_objects(Bucket=bucket_name)['Contents']:
        objects.append({'Key': key['Key']})

    # delete objects from the bucket
    response = conn.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})

#-----------------------------------------------
def cleanup(out_dir, bucket_name, conn):
    delete_all_objects(conn, bucket_name)
    result = admin(['gc', 'process', '--include-all'])
    assert result[1] == 0
    conn.delete_bucket(Bucket=bucket_name)
    assert count_object_parts_in_all_buckets() == (0, 0)
    cleanup_local(out_dir)

#-----------------------------------------------
def gen_object_name(filename, index):
    return filename + "::" + str(index)

#-----------------------------------------------
def calc_rados_obj_count(obj_name, obj_size):
    rados_obj_count = 0

    # split the object into parts
    full_parts_count = (obj_size // MULTIPART_SIZE)
    if full_parts_count :
        # each part is written separately so the last part can be incomplete
        rados_objs_per_full_part = math.ceil(MULTIPART_SIZE/RADOS_OBJ_SIZE)
        rados_obj_count = (full_parts_count * rados_objs_per_full_part)
        # add one part for an empty head-object
        rados_obj_count += 1

    partial_part = (obj_size % MULTIPART_SIZE)
    if partial_part:
        count = math.ceil(partial_part/RADOS_OBJ_SIZE)
        rados_obj_count += count

    log.debug("%s::obj_size=%.2f MiB, rados_rados_obj_count=%d",
              obj_name, float(obj_size)/MB, rados_obj_count)
    return rados_obj_count

#-----------------------------------------------
def calc_dedupable_space(obj_name, obj_size):
    # Objects with size bigger than MULTIPART_SIZE are uploaded as multi-part
    # multi-part objects got a zero size Head objects
    if obj_size >= MULTIPART_SIZE:
        dedupable_space = obj_size
    elif obj_size > RADOS_OBJ_SIZE:
        dedupable_space = obj_size - RADOS_OBJ_SIZE
    else:
        dedupable_space = 0

    log.debug("%s::obj_size=%.2f MiB, dedupable_space=%.2f MiB",
              obj_name, float(obj_size)/MB, float(dedupable_space)/MB)
    return dedupable_space

#-----------------------------------------------
def upload_objects(out_dir, bucket_name, files, conn):
    total_space=0
    duplicated_space=0
    duplicated_tail_objs=0
    rados_objects_total=0
    s3_objects_total=0
    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        total_space += (obj_size * num_copies)
        dedupable_space=calc_dedupable_space(filename, obj_size)
        duplicated_space += ((num_copies-1) * dedupable_space)
        rados_obj_count=calc_rados_obj_count(filename, obj_size)
        rados_objects_total += (rados_obj_count * num_copies)
        duplicated_tail_objs += ((num_copies-1) * (rados_obj_count-1))
        for i in range(0, num_copies):
            key = gen_object_name(filename, i)
            log.debug("uploading object %s", key)
            s3_objects_total += 1
            conn.upload_file(out_dir + filename, bucket_name, key, Config=config)

    log.info("upload_objects::%d S3 objects were uploaded", s3_objects_total)
    log.info("Based on calculation we should have %d rados objects", rados_objects_total)
    log.debug("Based on calculation we should have %d duplicated tail objs", duplicated_tail_objs)
    log.info("Based on calculation we should have %.2f MiB total in pool", total_space/MB)
    log.debug("Based on calculation we should have %.2f MiB duplicated space in pool", duplicated_space/MB)

    expected_rados_obj_count_post_dedup=(rados_objects_total-duplicated_tail_objs)
    log.info("Post dedup expcted rados obj count = %d", expected_rados_obj_count_post_dedup)
    expcted_space_post_dedup=(total_space-duplicated_space)
    log.info("Post dedup expcted data in pool = %.2f MiB", expcted_space_post_dedup/MB)
    assert (rados_objects_total, total_space) == count_object_parts_in_all_buckets()
    return (expected_rados_obj_count_post_dedup, expcted_space_post_dedup)

#-----------------------------------------------
def verify_objects(out_dir, bucket_name, files, conn, expcted_results):
    tempfile = out_dir + "temp"
    for f in files:
        filename=f[0]
        obj_size=f[1]
        num_copies=f[2]
        for i in range(0, num_copies):
            key = gen_object_name(filename, i)
            log.debug("comparing object %s with file %s", key, filename)
            conn.download_file(bucket_name, key, tempfile, Config=config)
            result = bash(['cmp', tempfile, out_dir + filename])
            assert result[1] == 0 ,"Files %s and %s differ!!" % (key, tempfile)
            #raise Exception("GBH: Testing cleanup, not a real error")
            os.remove(tempfile)

    assert expcted_results == count_object_parts_in_all_buckets(True)
    #count_space_in_all_buckets()

#-----------------------------------------------
def exec_dedup():
    log.info("\n==========================\nexec_dedup\n==========================")
    result = admin(['dedup', 'restart'])
    assert result[1] == 0
    # TBD - better monitoring for dedup completion !!
    time.sleep(20)

#-----------------------------------------------
def simple_dedup(out_dir, conn, bucket_name):
    # 1) generate random files and store them on disk
    # 2) upload a random copies of each file to bucket
    # 3) calculate current count of rados objects and pool size
    # 4) calculate expected count of rados objects and pool size *post dedup*

    # 5) execute DEDUP!!

    # 6) Read all objects from bucket and compare them to their stored copy *before dedup*
    #         This step is used to make sure objects were not corrupted by dedup
    # 7) count number and size of in-pool rados objects and compare with expected
    #         This step is used to make sure dedup removed *all* duplications
    # 8) delete all objects from bucket using s3 API
    # 9) call GC to make sure everything was removed
    #10) verify that there is nothing left on pool (i.e. ref-count is working)

    cleanup_local(out_dir)
    #make sure we are starting with all buckets empty
    assert count_object_parts_in_all_buckets() == (0, 0)
    os.mkdir(out_dir)
    try:
        files=[]
        gen_files(out_dir, 1*RADOS_OBJ_SIZE, 2, files)
        bucket = conn.create_bucket(Bucket=bucket_name)
        expcted_results = upload_objects(out_dir, bucket_name, files, conn)
        exec_dedup()
        verify_objects(out_dir, bucket_name, files, conn, expcted_results)
    finally:
        # cleanup must be executed even after a failure
        cleanup(out_dir, bucket_name, conn)

#-----------------------------------------------
@pytest.mark.basic_test
def test_dedup_basic():
    bucket_name = gen_bucket_name()
    log.info("test_dedup_basic: bucket_name=%s", bucket_name)
    conn = connection()
    simple_dedup(OUT_DIR, conn, bucket_name)


#-----------------------------------------------
def simple_dedup_dbg(out_dir, conn, bucket_name):
    cleanup_local(out_dir)
    #make sure we are starting with all buckets empty
    assert count_object_parts_in_all_buckets() == (0, 0)
    os.mkdir(out_dir)
    files=[]
    gen_files(out_dir, 4*RADOS_OBJ_SIZE, 1, files)
    bucket = conn.create_bucket(Bucket=bucket_name)
    expcted_results = upload_objects(out_dir, bucket_name, files, conn)
    exec_dedup()
    verify_objects(out_dir, bucket_name, files, conn, expcted_results)
