#!/usr/bin/python3

import logging as log
from configobj import ConfigObj
import subprocess
import json
import os

"""
Runs a test against a rgw with the data cache enabled. A client must be
set in the config for this task. This client must be the same client
that is in the config for the `rgw` task.

In the `overrides` section `datacache` and `datacache` must be configured for
the `rgw` task and the ceph conf overrides must contain the below config
variables in the client section.

`s3cmd` must be added as an extra_package to the install task.

In the `workunit` task, `- rgw/run-datacache.sh` must be set for the client that
is in the config for the `rgw` task. The `RGW_DATACACHE_PATH` variable must be
set in the workunit's `env` and it must match the `datacache_path` given to the
`rgw` task in `overrides`.
Ex:
- install:
    extra_packages:
      deb: ['s3cmd']
      rpm: ['s3cmd']
- overrides:
    rgw:
      datacache: true
      datacache_path: /tmp/rgw_datacache
    install:
      extra_packages:
        deb: ['s3cmd']
        rpm: ['s3cmd']
    ceph:
      conf:
        client:
          rgw d3n l1 datacache persistent path: /tmp/rgw_datacache/
          rgw d3n l1 datacache size: 10737417240
          rgw d3n l1 local datacache enabled: true
          rgw enable ops log: true
- rgw:
    client.0:
- workunit:
    clients:
      client.0:
      - rgw/run-datacache.sh
    env:
      RGW_DATACACHE_PATH: /tmp/rgw_datacache
    cleanup: true
"""

log.basicConfig(level=log.DEBUG)

""" Constants """
USER = 'rgw_datacache_user'
DISPLAY_NAME = 'DatacacheUser'
ACCESS_KEY = 'NX5QOQKC6BH2IDN8HC7A'
SECRET_KEY = 'LnEsqNNqZIpkzauboDcLXLcYaWwLQ3Kop0zAnKIn'
BUCKET_NAME = 'datacachebucket'
FILE_NAME = '7M.dat'
GET_FILE_NAME = '7M-get.dat'

def exec_cmd(cmd):
    log.debug("exec_cmd(%s)", cmd)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = proc.communicate()
        if proc.returncode == 0:
            log.info('command succeeded')
            if out is not None: log.info(out)
            return out
        else:
            raise Exception("error: %s \nreturncode: %s" % (err, proc.returncode))
    except Exception as e:
        log.error('command failed')
        log.error(e)
        return False

def get_radosgw_endpoint():
    out = exec_cmd('sudo netstat -nltp | egrep "rados|valgr"')  # short for radosgw/valgrind
    x = out.decode('utf8').split(" ")
    port = [i for i in x if ':' in i][0].split(':')[1]
    log.info('radosgw port: %s' % port)
    proto = "http"
    hostname = '127.0.0.1'

    if port == '443':
        proto = "https"

    endpoint = hostname

    log.info("radosgw endpoint is: %s", endpoint)
    return endpoint, proto

def create_s3cmd_config(path, proto):
    """
    Creates a minimal config file for s3cmd
    """
    log.info("Creating s3cmd config...")

    use_https_config = "False"
    log.info("proto for s3cmd config is %s", proto)
    if proto == "https":
        use_https_config = "True"

    s3cmd_config = ConfigObj(
        indent_type='',
        infile={
            'default':
                {
                'host_bucket': 'no.way.in.hell',
                'use_https': use_https_config,
                },
            }
    )

    f = open(path, 'wb')
    s3cmd_config.write(f)
    f.close()
    log.info("s3cmd config written")

def get_cmd_output(cmd_out):
    out = cmd_out.decode('utf8')
    out = out.strip('\n')
    return out

def main():
    """
    execute the datacache test
    """
    # setup for test
    cache_dir = os.environ['RGW_DATACACHE_PATH']
    log.debug("datacache dir from config is: %s", cache_dir)

    out = exec_cmd('pwd')
    pwd = get_cmd_output(out)
    log.debug("pwd is: %s", pwd)

    endpoint, proto = get_radosgw_endpoint()

    # create 7M file to put
    outfile = pwd + '/' + FILE_NAME
    exec_cmd('dd if=/dev/urandom of=%s bs=1M count=7' % (outfile))

    # get file size
    stat_result = os.stat(outfile)
    file_size = stat_result.st_size
    log.debug("file size is: %d", file_size)

    # create user
    exec_cmd('radosgw-admin user create --uid %s --display-name %s --access-key %s --secret %s'
            % (USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY))

    # create s3cmd config
    s3cmd_config_path = pwd + '/s3cfg'
    create_s3cmd_config(s3cmd_config_path, proto)

    # create a bucket
    exec_cmd('s3cmd --access_key=%s --secret_key=%s --config=%s --no-check-hostname --host=%s mb s3://%s'
            % (ACCESS_KEY, SECRET_KEY, s3cmd_config_path, endpoint, BUCKET_NAME))

    # put an object in the bucket
    exec_cmd('s3cmd --access_key=%s --secret_key=%s --config=%s --no-check-hostname --host=%s put %s s3://%s'
            % (ACCESS_KEY, SECRET_KEY, s3cmd_config_path, endpoint, outfile, BUCKET_NAME))

    # get object from bucket
    get_file_path = pwd + '/' + GET_FILE_NAME
    exec_cmd('s3cmd --access_key=%s --secret_key=%s --config=%s --no-check-hostname --host=%s get s3://%s/%s %s --force'
            % (ACCESS_KEY, SECRET_KEY, s3cmd_config_path, endpoint, BUCKET_NAME, FILE_NAME, get_file_path))

    # get info of object
    out = exec_cmd('radosgw-admin object stat --bucket=%s --object=%s' % (BUCKET_NAME, FILE_NAME))

    json_op = json.loads(out)
    bucket_marker = json_op['manifest']['tail_placement']['bucket']['marker']
    object_name = json_op['name']

    # get num parts of the cached object, 7MB is the size of the object, 4MB is the chunk size used in d3n filter
    object_size = file_size
    chunk_size = 4194304
    num_parts = int(object_size/chunk_size)
    if object_size%chunk_size > 0:
        num_parts = num_parts + 1
    log.info("Num parts is: %s", num_parts)

    # check that the cache is enabled (does the cache directory empty)
    out = exec_cmd('find %s -type f | wc -l' % (cache_dir))
    chk_cache_dir = int(get_cmd_output(out))
    log.debug("Check cache dir content: %s", chk_cache_dir)
    if chk_cache_dir == 0:
        log.info("NOTICE: datacache test object not found, inspect if datacache was bypassed or disabled during this check.")
        return
    if chk_cache_dir != 2:
        log.info("ERROR: not all the parts of the datacache test object were found in the cache.")
        return

    # list the files in the cache dir for troubleshooting
    out = exec_cmd('ls -l %s' % (cache_dir))

    # get name of cached object and check if it exists in the cache
    offset = 0
    length = chunk_size
    total_length = chunk_size
    for i in range(num_parts):
        cached_object_name = bucket_marker + "_" + object_name + "_" + str(offset) + "_" + str(length)
        log.debug("Cached object name is: %s", cached_object_name)
        out = exec_cmd('find %s -name %s' % (cache_dir, cached_object_name))
        cached_object_path = get_cmd_output(out)
        log.debug("Path of file in datacache is: %s", cached_object_path)
        offset = offset + chunk_size
        if i == (num_parts - 2): # last part size maybe less than chunk_size
            length = object_size - total_length
        else:
            length = chunk_size
        total_length = total_length + offset


    log.debug("RGW Datacache test SUCCESS")

    # remove datacache dir
    #cmd = exec_cmd('rm -rf %s' % (cache_dir))
    #log.debug("RGW Datacache dir deleted")
    #^ commenting for future reference - the work unit will continue running tests and if the cache_dir is removed
    #  all the writes to cache will fail with errno 2 ENOENT No such file or directory.

main()
log.info("Completed Datacache tests")
