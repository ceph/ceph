import contextlib
import logging 
import os

from teuthology.orchestra import run 
from teuthology import misc as teuthology

#create a logger object named after our file, so our logger object has a name crimosn_store_bench
#store-bench.cc gets compiled into binary crimson-store-bench, our python code executes this binary
log=logging.getLogger(__name__)

@contextlib.contextmanager
#for every teuthology job we must define a task called task taht takes 2 params ,ctx +config 
#ctx is the cluster specifictaions, so what machines are running this file osd0,osd1 
#config : what comes from our yaml , so all the specifications for the functions we need to run

def task(ctx,config):
    log.info("Beginning crimson store bench test")
    testdir=teuthology.get_testdir(ctx)
    store_dir=os.path.join(testdir,'store_bench_dir')
    role='osd.0'
    (remote,)=ctx.cluster.only(role).remotes.keys()

    work_load_type=config.get('work_load_type','pg_log')
    duration=config.get('duration',30)
    num_concurrent_io=config.get('num_concurrent_io',4)
    smp=config.get('smp',4)
    bench_cmd=' '.join([
        'crimson-store-bench',
        '--store-path',store_dir,
        '--work-load-type',work_load_type,
        '--duration',str(duration),
        '--num-concurrent-io',str(num_concurrent_io),
        '--smp',str(smp),
        '--seastore_device_size','10G',
    ])
    if config.get('dump_metrics', False):
        bench_cmd += ' --dump-metrics'

    if work_load_type=='pg_log':
        bench_cmd += ' --num-logs ' + str(config.get('num_logs', 4))
        bench_cmd += ' --log-size ' + str(config.get('log_size', 1024))
        bench_cmd += ' --log-length ' + str(config.get('log_length', 256))
    
    elif work_load_type == 'rgw_index':
        bench_cmd += ' --num_indices ' + str(config.get('num_indices', 16))
        bench_cmd += ' --key_size ' + str(config.get('key_size', 1024))
        bench_cmd += ' --value_size ' + str(config.get('value_size', 1024))
        bench_cmd += ' --target_keys_per_bucket ' + str(config.get('target_keys_per_bucket', 256))
        bench_cmd += ' --tolerance_range ' + str(config.get('tolerance_range', 10))
        bench_cmd += ' --num_buckets_per_collection ' + str(config.get('num_buckets_per_collection', 16))


    elif work_load_type == 'random_write':
        if 'io_size' in config:
            bench_cmd += ' --io-size ' + str(config.get('io_size'))
        if 'size_per_shard' in config:
            bench_cmd += ' --size-per-shard ' + str(config.get('size_per_shard'))
        if 'size_per_obj' in config:
            bench_cmd += ' --size-per-obj ' + str(config.get('size_per_obj'))
        if 'colls_per_shard' in config:
            bench_cmd += ' --colls-per-shard ' + str(config.get('colls_per_shard'))
    
    full_cmd=(
        'rm -rf {store_dir} && '
        'mkdir -p {store_dir} && '
        'touch {store_dir}/block && '
        'truncate -s 10G {store_dir}/block && '
        '{bench_cmd}'
    ).format(store_dir=store_dir, bench_cmd=bench_cmd)

    log.info("running the process %s",full_cmd)
    proc=remote.run(args=['bash','-c',full_cmd],
                    logger=log.getChild('actual_benchmark_output'),
                    stdin=run.PIPE,
                    wait=False)
    
    try:
        yield 
    finally:
        log.info("waiting for the process to finish")
        proc.wait()
        log.info("cleaning up store_bench_dir")
        remote.run(args=['rm', '-rf', store_dir])
