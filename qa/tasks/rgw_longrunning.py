import yaml
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)
import os
import pwd
import cStringIO
import time

log = logging.getLogger(__name__)

DIR = {"v2": {"script": "rgw/v2/tests/s3_swift/",
              "config": "rgw/v2/tests/s3_swift/configs"}}

MEM_UNITS_CONV = {'MiB': 1024 ** 0,
                  'GiB': 1024 ** 1,
                  'TiB': 1024 ** 2,
                  'PiB': 1024 ** 3}


def get_cluster_size_info(clients):
    out = cStringIO.StringIO()
    clients[0].run(args=['sudo', 'ceph', 'df'], stdout=out)
    var = out.readlines()
    cluster_size = dict(zip(var[1].split(), var[2].split()))
    return cluster_size


def do_auto_calculate_io(clients, config):
    # re weight osds
    """
    # this code may be come of use in future, so just keep in comment.

    out = cStringIO.StringIO()
    clients[0].run(args=['sudo', 'ceph', 'osd', 'df'], stdout=out)
    var = out.readlines()

    osd_weights = {i.split()[0]:i.split()[1] for i in var[1:len(var)-2]}

    for id, weight in osd_weights.items():
        clients[0].run(args=[run.Raw(
            'sudo ceph osd reweight %s %s' % (id, weight))])
    """
    # get the cluster size

    cluster_size = get_cluster_size_info(clients)
    available = cluster_size['AVAIL']
    size, mem_unit = int(float(available[:-3])), available[-3:]
    log.info('size: %s, mem_unit: %s' %(size, mem_unit))
    available = size * MEM_UNITS_CONV.get(mem_unit)  # convert size to mbs
    replication = 3  # assuming the replication size is 3
    log.info('available size: %s' % available)
    available = int(float(available) / replication)
    log.info('available size with replication(%s): %s' % (replication, available))
    filling_percent = config.get('filling_percent', 60)  # default filling percent is 60
    log.info('filling percent: %s' % filling_percent)
    usable_size = int((float(filling_percent) / float(100)) * available)
    log.info('usable_size: %s' % usable_size)
    test_config = config.get('config')
    user_count = test_config.get('user_count')
    bucket_count = test_config.get('bucket_count')
    size_ranges = test_config.get('size_ranges')
    version_count = test_config.get('version_count')
    each_user_usable_size = int(float(usable_size) / float(user_count))
    log.info('each_user_usable_size: %s' % each_user_usable_size)
    each_bucket_usable_size = int(float(each_user_usable_size) / float(bucket_count))
    log.info("each user's bucket usable size: %s" % each_bucket_usable_size)
    if version_count is not None:
        log.info('got version count')
        assert version_count != 0, "version count cannot be zero"
        log.info('version_count: %s' % version_count)
        each_bucket_usable_size = int(float(each_bucket_usable_size) / float(version_count))
        log.info("each user's bucket size with versioning: %s" % each_bucket_usable_size)
    part_size = int(float(each_bucket_usable_size) / float(len(size_ranges)))
    log.info('each part size: %s' % part_size)
    temp = []
    for sz in size_ranges:
        objects = int(float(part_size) / float(sz))
        log.info('size: %s objects: %s' % (sz, objects))
        for i in range(objects):
            temp.append(sz)
    mapped_sizes = {indx: sz for indx, sz in enumerate(temp)}
    log.info('total objects: %s ' % len(mapped_sizes))
    estimated_filling_size = sum(mapped_sizes.values())
    log.info('estimated_filling_size: %s' % estimated_filling_size)
    clients[0].run(args=['sudo', 'ceph', 'osd', 'crush', 'tunables', 'optimal'])
    time.sleep(60)
    return mapped_sizes


@contextlib.contextmanager
def task(ctx, config):
    """
     pre-validation still pending
    """

    log.info('starting rgw-longrunning')
    log.info('config %s' % config)
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"
    config_file_name = config['test'] + ".yaml"
    log.info('test_version: %s' % config.get('test_version', 'v2'))
    log.info('test: %s' % config['test'])
    branch = config.get('branch', 'master')
    log.info('script: %s' % config.get('script', config['test'] + ".py"))
    test_root_dir = 'rgw-tests'
    test_base_path = os.path.join(test_root_dir, 'ceph-qe-scripts')
    script = os.path.join(test_base_path,
                          DIR[config.get('test_version', 'v2')]['script'],
                          config.get('script', config['test'] + ".py"))
    config_file = os.path.join(test_base_path,
                               DIR[config.get('test_version', 'v2')]['config'],
                               config_file_name)
    log.info('script: %s' % script)
    log.info('config_file: %s' % config_file)
    soot = ['venv', 'rgw-tests', 'io_info.yaml', '*.json', 'Download.*', 'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*',
            '*.key.*']
    cleanup = lambda x: clients[0].run(args=[run.Raw('sudo rm -rf %s' % x)])
    remotes = ctx.cluster.only(teuthology.is_type('client'))
    clients = [
        remote for remote,
                   roles_for_host in remotes.remotes.iteritems()]
    map(cleanup, soot)
    clients[0].run(args=['mkdir', test_root_dir])
    log.info('cloning the repo to %s' % clients[0].hostname)
    clients[0].run(
        args=[
            'cd',
            '%s' % test_root_dir,
            run.Raw(';'),
            'git',
            'clone',
            'https://github.com/red-hat-storage/ceph-qe-scripts.git',
            '-b',
            '%s' % branch
        ])
    mapped_sizes = do_auto_calculate_io(clients, config)
    test_config = {'config': config.get('config')}
    test_config['config']['objects_count'] = len(mapped_sizes)
    test_config['config']['mapped_sizes'] = mapped_sizes
    log.info('config: %s' % test_config)
    log.info('creating configuration from data: %s' % test_config)
    local_file = os.path.join('/tmp/',
                              config_file_name + "_" + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name)
    with open(local_file, 'w') as outfile:
        outfile.write(yaml.dump(test_config, default_flow_style=False))
    log.info('local_file: %s' % local_file)
    log.info('copying temp yaml to the client node')
    clients[0].put_file(local_file, config_file)
    clients[0].run(args=['ls', '-lt', os.path.join(test_base_path,
                                                   DIR[config.get('test_version', 'v2')]['config'])])
    clients[0].run(args=['cat', config_file])
    # os.remove(local_file)
    clients[0].run(args=['virtualenv', 'venv'])
    clients[0].run(
        args=[
            'source',
            'venv/bin/activate',
            run.Raw(';'),
            run.Raw('pip install boto boto3 names PyYaml ConfigParser'),
            run.Raw(';'),
            'deactivate'])
    time.sleep(60)
    log.info('trying to restart rgw service after sleep 60 secs')
    clients[0].run(args=[run.Raw('sudo systemctl restart ceph-radosgw.target')])
    log.info('starting the tests after sleep of 60 secs')
    time.sleep(60)
    clients[0].run(
        args=[run.Raw(
            'sudo venv/bin/python2.7 %s -c %s ' % (script, config_file))])
    try:
        yield
    finally:
        log.info('Test completed')
        log.info('Cluster size after test completion')
        cluster_size = get_cluster_size_info(clients)
        log.info('available: %s' % cluster_size['AVAIL'])
        log.info("Deleting leftovers")
        map(cleanup, soot)
