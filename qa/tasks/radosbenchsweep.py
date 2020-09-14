"""
Rados benchmarking sweep
"""
import contextlib
import logging
import re

from io import BytesIO
from itertools import product

from teuthology.orchestra import run
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Execute a radosbench parameter sweep

    Puts radosbench in a loop, taking values from the given config at each
    iteration. If given, the min and max values below create a range, e.g.
    min_replicas=1 and max_replicas=3 implies executing with 1-3 replicas.

    Parameters:

        clients: [client list]
        time: seconds to run (default=120)
        sizes: [list of object sizes] (default=[4M])
        mode: <write|read|seq> (default=write)
        repetitions: execute the same configuration multiple times (default=1)
        min_num_replicas: minimum number of replicas to use (default = 3)
        max_num_replicas: maximum number of replicas to use (default = 3)
        min_num_osds: the minimum number of OSDs in a pool (default=all)
        max_num_osds: the maximum number of OSDs in a pool (default=all)
        file: name of CSV-formatted output file (default='radosbench.csv')
        columns: columns to include (default=all)
          - rep: execution number (takes values from 'repetitions')
          - num_osd: number of osds for pool
          - num_replica: number of replicas
          - avg_throughput: throughput
          - avg_latency: latency
          - stdev_throughput:
          - stdev_latency:

    Example:
    - radsobenchsweep:
        columns: [rep, num_osd, num_replica, avg_throughput, stdev_throughput]
    """
    log.info('Beginning radosbenchsweep...')
    assert isinstance(config, dict), 'expecting dictionary for configuration'

    # get and validate config values
    # {

    # only one client supported for now
    if len(config.get('clients', [])) != 1:
        raise Exception("Only one client can be specified")

    # only write mode
    if config.get('mode', 'write') != 'write':
        raise Exception("Only 'write' mode supported for now.")

    # OSDs
    total_osds_in_cluster = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    min_num_osds = config.get('min_num_osds', total_osds_in_cluster)
    max_num_osds = config.get('max_num_osds', total_osds_in_cluster)

    if max_num_osds > total_osds_in_cluster:
        raise Exception('max_num_osds cannot be greater than total in cluster')
    if min_num_osds < 1:
        raise Exception('min_num_osds cannot be less than 1')
    if min_num_osds > max_num_osds:
        raise Exception('min_num_osds cannot be greater than max_num_osd')
    osds = range(0, (total_osds_in_cluster + 1))

    # replicas
    min_num_replicas = config.get('min_num_replicas', 3)
    max_num_replicas = config.get('max_num_replicas', 3)

    if min_num_replicas < 1:
        raise Exception('min_num_replicas cannot be less than 1')
    if min_num_replicas > max_num_replicas:
        raise Exception('min_num_replicas cannot be greater than max_replicas')
    if max_num_replicas > max_num_osds:
        raise Exception('max_num_replicas cannot be greater than max_num_osds')
    replicas = range(min_num_replicas, (max_num_replicas + 1))

    # object size
    sizes = config.get('size', [4 << 20])

    # repetitions
    reps = range(config.get('repetitions', 1))

    # file
    fname = config.get('file', 'radosbench.csv')
    f = open('{}/{}'.format(ctx.archive, fname), 'w')
    f.write(get_csv_header(config) + '\n')
    # }

    # set default pools size=1 to avoid 'unhealthy' issues
    ctx.manager.set_pool_property('data', 'size', 1)
    ctx.manager.set_pool_property('metadata', 'size', 1)
    ctx.manager.set_pool_property('rbd', 'size', 1)

    current_osds_out = 0

    # sweep through all parameters
    for osds_out, size, replica, rep in product(osds, sizes, replicas, reps):

        osds_in = total_osds_in_cluster - osds_out

        if osds_in == 0:
            # we're done
            break

        if current_osds_out != osds_out:
            # take an osd out
            ctx.manager.raw_cluster_cmd(
                'osd', 'reweight', str(osds_out-1), '0.0')
            wait_until_healthy(ctx, config)
            current_osds_out = osds_out

        if osds_in not in range(min_num_osds, (max_num_osds + 1)):
            # no need to execute with a number of osds that wasn't requested
            continue

        if osds_in < replica:
            # cannot execute with more replicas than available osds
            continue

        run_radosbench(ctx, config, f, osds_in, size, replica, rep)

    f.close()

    yield


def get_csv_header(conf):
    all_columns = [
        'rep', 'num_osd', 'num_replica', 'avg_throughput',
        'avg_latency', 'stdev_throughput', 'stdev_latency'
    ]
    given_columns = conf.get('columns', None)
    if given_columns and len(given_columns) != 0:
        for column in given_columns:
            if column not in all_columns:
                raise Exception('Unknown column ' + column)
        return ','.join(conf['columns'])
    else:
        conf['columns'] = all_columns
        return ','.join(all_columns)


def run_radosbench(ctx, config, f, num_osds, size, replica, rep):
    pool = ctx.manager.create_pool_with_unique_name()

    ctx.manager.set_pool_property(pool, 'size', replica)

    wait_until_healthy(ctx, config)

    log.info('Executing with parameters: ')
    log.info('  num_osd =' + str(num_osds))
    log.info('  size =' + str(size))
    log.info('  num_replicas =' + str(replica))
    log.info('  repetition =' + str(rep))

    for role in config.get('clients', ['client.0']):
        assert isinstance(role, str)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.keys()

        proc = remote.run(
            args=[
                'adjust-ulimits',
                'ceph-coverage',
                '{}/archive/coverage'.format(teuthology.get_testdir(ctx)),
                'rados',
                '--no-log-to-stderr',
                '--name', role,
                '-b', str(size),
                '-p', pool,
                'bench', str(config.get('time', 120)), 'write',
            ],
            logger=log.getChild('radosbench.{id}'.format(id=id_)),
            stdin=run.PIPE,
            stdout=BytesIO(),
            wait=False
        )

        # parse output to get summary and format it as CSV
        proc.wait()
        out = proc.stdout.getvalue()
        all_values = {
            'stdev_throughput': re.sub(r'Stddev Bandwidth: ', '', re.search(
                r'Stddev Bandwidth:.*', out).group(0)),
            'stdev_latency': re.sub(r'Stddev Latency: ', '', re.search(
                r'Stddev Latency:.*', out).group(0)),
            'avg_throughput': re.sub(r'Bandwidth \(MB/sec\): ', '', re.search(
                r'Bandwidth \(MB/sec\):.*', out).group(0)),
            'avg_latency': re.sub(r'Average Latency: ', '', re.search(
                r'Average Latency:.*', out).group(0)),
            'rep': str(rep),
            'num_osd': str(num_osds),
            'num_replica': str(replica)
        }
        values_to_write = []
        for column in config['columns']:
            values_to_write.extend([all_values[column]])
        f.write(','.join(values_to_write) + '\n')

    ctx.manager.remove_pool(pool)


def wait_until_healthy(ctx, config):
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.keys()
    teuthology.wait_until_healthy(ctx, mon_remote)
