"""
Run ceph-dedup-daemon
"""
import contextlib
import logging
import gevent
from teuthology import misc as teuthology
import json
import time
from io import StringIO

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run ceph-dedup-daemon.
    The config should be as follows::
        ceph-dedup-daemon:
          clients: [client list]
          pool: <pool name>
          chunk_pool: <chunk pool name>
          chunk_size: <chunk size>
          chunk_algorithm: <chunk algorithm, fixed|fastcdc>
          fingerprint_algorithm: <fingerprint algorithm, sha1|sha256|sha512>
          chunk_dedup_threashold: <the number of duplicate chunks to trigger chunk dedup>
          max_thread: <the number of threads>
          wakeup_period: <duration>
          wait_before_validation: <seconds> # Validation will start after wait_before_validation.
                                            # Note that foreground I/Os must be done before the 
                                            # validation starts because the validation procedure 
                                            # is not able to check the reference is correct under
                                            # overwrite workload. Apart from validation, 
                                            # ceph-dedup-daemon runs as soon as the task is started 
                                            # regardless of wait_before_validation.
    For example::
        tasks:
        - exec:
            client.0:
              - sudo ceph osd pool create low_tier 4
        - deduplication:
            clients: [client.0]
            op: 'sample-dedup'
            pool: 'default.rgw.buckets.data'
            chunk_pool: 'low_tier'
            chunk_size: 131072
            chunk_algorithm: 'fastcdc'
            fingerprint_algorithm: 'sha1'
            chunk_dedup_threshold: 5
            max_thread: 2
            wakeup_period: 20
            wait_before_validation: 460
    """
    log.info('Beginning deduplication...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    args = [
        'ceph-dedup-daemon']
    if config.get('chunk_pool', None):
        args.extend(['--chunk-pool', config.get('chunk_pool', None)])
    if config.get('chunk_size', False):
        args.extend(['--chunk-size', str(config.get('chunk_size', 131072))])
    if config.get('chunk_algorithm', False):
        args.extend(['--chunk-algorithm', config.get('chunk_algorithm', None)] )
    if config.get('fingerprint_algorithm', False):
        args.extend(['--fingerprint-algorithm', config.get('fingerprint_algorithm', None)] )
    if config.get('chunk_dedup_threshold', False):
        args.extend(['--chunk-dedup-threshold', str(config.get('chunk_dedup_threshold', 1))])
    if config.get('max_thread', False):
        args.extend(['--max-thread', str(config.get('max_thread', 2))])
    if config.get('sampling_ratio', False):
        args.extend(['--sampling-ratio', str(config.get('sampling_ratio', 100))])
    if config.get('wakeup_period', False):
        args.extend(['--wakeup-period', str(config.get('wakeup_period', 20))])
    if config.get('pool', False):
        args.extend(['--pool', config.get('pool', None)])
    wait_before_validation = config.get('wait_before_validation', 0)

    def thread():
        run_remote(args, False, 0)

    def run_remote(args, need_wait, client_num, return_without_retry = False):
        clients = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        log.info('clients are %s' % clients)
        role = 'client.{id}'.format(id=client_num)
        if role not in clients:
            raise Exception('wrong client {c}'.format(c=role))
        assert isinstance(role, str)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        testdir = teuthology.get_testdir(ctx)
        cmd_args = [
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir)]
        cmd_args.extend(args)
        log.info("cmd: %s", cmd_args)
        tries = 0
        while True:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            proc = remote.run(
                args=cmd_args,
                wait=need_wait, check_status=False,
                stdout=StringIO(),
                )
            log.info('exitstatus {r}'.format(r=proc.exitstatus))
            if proc.exitstatus == 0 or need_wait == False:
                log.info('proc stdout ', proc.stdout.getvalue())
                return proc.stdout.getvalue().strip()
            if return_without_retry == True:
                return None
            tries += 1
            if tries > 60:
                raise Exception('timed out getting correct exitstatus')
            time.sleep(30)

    def get_chunk_objs(chunk_pool):
        chunk_obj_list = run_remote(('rados ls -p ' + chunk_pool).split(), True, 1).split()
        if chunk_obj_list == False:
            return None
        else:
            return chunk_obj_list

    def get_ref_list(chunk_pool, chunk_obj):
        # get reference list of chunk object
        dump_str = run_remote(
            ('ceph-dedup-tool --op dump-chunk-refs --chunk-pool '
            + chunk_pool + ' --object ' + chunk_obj).split(),
            True, 1, True
        )
        if dump_str is None:
            return None
        log.info('{0} obj has {1} refs'
            .format(chunk_obj, json.loads(dump_str)['count']))

        # check if chunk object's reference object exists in base-tier
        ref_list = json.loads(dump_str)['refs']
        return ref_list

    # To validate whether the sample-dedup operation works well, this function checks if
    #   1. sample-dedup has been started and
    #   2. reference of chunk objects' exists in correct base pool
    def validate():
        log.info('start validating ceph-dedup')
        base_pool = config.get('pool', None)
        chunk_pool = config.get('chunk_pool', None)
        max_validation_cnt = 15
        retry_cnt = 0
        # chunk objs for re-validation after chunk-repair
        retry_chunk_objs = list() 

        # check whether sample-dedup has been started
        chunk_obj_list = get_chunk_objs(chunk_pool)
        while (chunk_obj_list == None or len(chunk_obj_list) == 0) and retry_cnt < max_validation_cnt:
            # retry getting # chunk objs after 30 secs of sleep
            time.sleep(30)
            chunk_obj_list = get_chunk_objs(chunk_pool)
            retry_cnt += 1
            log.info('chunk pool empty. retry ', retry_cnt)
        assert retry_cnt < max_validation_cnt

        log.info('ceph-dedup started successfully')

        retry_cnt = 0
        max_validation_cnt = 5
        # validate chunk pool for max_validation_cnt times
        while retry_cnt < max_validation_cnt:
            for chunk_obj in chunk_obj_list:
                ref_list = get_ref_list(chunk_pool, chunk_obj)
                if ref_list is None:
                    retry_cnt -= 2
                    assert retry_cnt > -(max_validation_cnt * 2)
                    time.sleep(15)
                    break
                for ref in ref_list:
                    ret = run_remote(
                        ('rados -p ' + base_pool + ' stat ' + ref['oid'])
                        .split(), True, 1
                    )
                    # check if ref exists in base pool
                    if ret == False or len(ret) == 0:
                        # if ref not exists in base pool, try repair in order to avoid 
                        # false-positive inconsistent reference
                        ret = run_remote(('ceph osd pool stats ' + base_pool).split(), True, 1)
                        assert len(ret) > 0
                        base_pool_id = ret.split()[3]
                        ret = run_remote(
                            ('ceph-dedup-tool --op chunk-repair --chunk-pool '
                            + chunk_pool + ' --object ' + chunk_obj + ' --target-ref ' 
                            + ref['oid'] + ' --target-ref-pool-id ' + base_pool_id)
                            .split(), True, 1
                        )
                        retry_chunk_objs.append(chunk_obj)
                    log.info('{0} obj exists in {1}'.format(ref['oid'], base_pool))

            # retry validation for repaired objects
            for chunk_obj in retry_chunk_objs:
                ref_list = get_ref_list(chunk_pool, chunk_obj)
                assert ref_list is not None
                for ref in ref_list:
                    ret = run_remote(
                        ('rados -p ' + base_pool + ' stat ' + ref['oid'])
                        .split(), True, 1
                    )
                    assert len(ret) > 0
                    log.info(
                        '{0} obj exists in {1} after repair'.format(ref['oid'], 
                        base_pool)
                    )
            retry_chunk_objs = list()

            # get chunk objects for the next loop
            chunk_obj_list = get_chunk_objs(chunk_pool)
            retry_cnt += 1
            time.sleep(30)
        return True


    running = gevent.spawn(thread)
    if wait_before_validation != 0:
        time.sleep(wait_before_validation)
    checker = gevent.spawn(validate)

    try:
        yield
    finally:
        log.info('joining ceph-dedup-daemon')
        running.get()
        checker.get()
