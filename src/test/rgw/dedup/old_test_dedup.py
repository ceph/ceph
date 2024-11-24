import logging
import random
import time
import subprocess
import os
import string
import pytest
import boto3
import shutil

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
    log.info('running command: %s', ' '.join(cmd))
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

OBJ_PART_SIZE = (4*1024*1024)
OUT_DIR="/tmp/dedup/"

#-----------------------------------------------
def write_random(out_dir, size, files, all_sizes):
    filename = "OBJ_" + str(size)
    log.info(filename)
    files.append(filename)
    all_sizes.append(size)
    full_filename = out_dir + filename

    fout = open(full_filename, "wb")
    fout.write(os.urandom(size))
    fout.close()

#-----------------------------------------------
def gen_files(out_dir, start_size, factor, files, all_sizes):
    size = start_size
    for f in range(1, factor+1):
        size2 = size + random.randint(1, OBJ_PART_SIZE-1)        
        write_random(out_dir, size, files, all_sizes)
        write_random(out_dir, size2, files, all_sizes)
        size  = size * 2;
        log.info("==============================")

#-----------------------------------------------
def cleanup_local(out_dir):
    if os.path.isdir(out_dir):
        log.info("Removing old directory " + out_dir)
        #subprocess.run(["/bin/rm", "-rf", out_dir])
        shutil.rmtree(out_dir)

#-----------------------------------------------
def delete_all_objects(conn, bucket_name):
    objects = []
    for key in conn.list_objects(Bucket=bucket_name)['Contents']:
        objects.append({'Key': key['Key']})
        # delete objects from the bucket
    response = conn.delete_objects(Bucket=bucket_name,
                                   Delete={'Objects': objects})

#-----------------------------------------------
def cleanup(out_dir, bucket_name, conn):
    log.info("Removing Bucket " + bucket_name)
    delete_all_objects(conn, bucket_name)
    log.info("Calling GC")
    result = admin(['gc', 'process', '--include-all'])
    assert result[1] == 0

    conn.delete_bucket(Bucket=bucket_name)
    cleanup_local(out_dir)

#-----------------------------------------------
def count_object_parts():
    result = rados(['ls', '-p default.rgw.buckets.data'])
    assert result[1] == 0

    names=result[0].split()
    count = 0
    for name in names:
        #log.info(name)
        count = count + 1

    return count

#-----------------------------------------------
def upload_objects(out_dir, bucket_name, files, conn):
    for f in files:
        for i in range(1, 3):
            key = f + str(i)
            log.info("uploading object %s", key)
            conn.upload_file(out_dir + f, bucket_name, key)

    part_count = count_object_parts()
    log.info("We had %d parts", part_count)

#-----------------------------------------------
def verify_objects(out_dir, bucket_name, files, conn):
    tempfile = out_dir + "temp"
    for f in files:
        for i in range(1, 3):
            key = f + str(i)
            conn.download_file(bucket_name, key, tempfile)
            log.info("comparing object %s with file %s", key, f)
            result = bash(['cmp', tempfile, out_dir + f])
            assert result[1] == 0 ,"Files %s and %s differ!!" % (key, tempfile)
            os.remove(tempfile)

#-----------------------------------------------
def exec_dedup():
    result = admin(['dedup', 'restart'])
    assert result[1] == 0
    # TBD - better monitoring for dedup completion !!
    time.sleep(10)

#-----------------------------------------------
def simple_dedup(out_dir, conn, bucket_name):
    bucket = conn.create_bucket(Bucket=bucket_name)
    files=[]
    all_sizes=[]
    info = (files, all_sizes)

    ll=[]
    ll.append(("name1", 100))
    ll.append(("name2", 300))
    for l in ll:
        log.info("l[0]=%s, l[1]=%d", l[0], l[1])

    cleanup_local(out_dir)
    os.mkdir(out_dir)
    gen_files(out_dir, OBJ_PART_SIZE, 1, info[0], info[1])
    upload_objects(out_dir, bucket_name, files, conn)
    exec_dedup()
    verify_objects(out_dir, bucket_name, files, conn)

    cleanup(out_dir, bucket_name, conn)
    log.info("test was successful")

#-----------------------------------------------
@pytest.mark.basic_test
def test_dedup_basic():
    bucket_name = gen_bucket_name()
    log.info("test_dedup_basic: bucket_name=%s", bucket_name)
    conn = connection()
    simple_dedup(OUT_DIR, conn, bucket_name)
