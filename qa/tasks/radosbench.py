"""
Rados benchmarking
"""
import contextlib
import logging

from teuthology.orchestra import run
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def _validate_pool_config(config):
    """
    Validate pool configuration for incompatible combinations.
    
    Args:
        config: Pool configuration dictionary
        
    Raises:
        ValueError: If configuration contains incompatible settings
    """
    num_zones = config.get('num_zones', 1)
    if num_zones > 1:
        log.info(
            "num_zones=%d detected. Ensure osd_pool_default_flag_ec_optimizations "
            "is set to true in ceph configuration.", num_zones
        )
    
    if 'erasure_code_profile' in config and not config.get('ec_pool', False):
        raise ValueError(
            "erasure_code_profile specified but ec_pool is not true"
        )
    
    if config.get('erasure_code_use_overwrites', False) and not config.get('ec_pool', False):
        raise ValueError(
            "erasure_code_use_overwrites requires ec_pool to be true"
        )
    
    if 'erasure_code_crush' in config and not config.get('ec_pool', False):
        raise ValueError(
            "erasure_code_crush specified but ec_pool is not true"
        )


@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosbench

    The config should be as follows:

    radosbench:
        clients: [client list]
        time: <seconds to run>
        pool: <pool to use>
        size: write size to use
        concurrency: max number of outstanding writes (16)
        objectsize: object size to use
        unique_pool: use a unique pool, defaults to False
        ec_pool: create an ec pool, defaults to False
        create_pool: create pool, defaults to True
        erasure_code_profile:
          name: teuthologyprofile
          k: 2
          m: 1
          crush-failure-domain: osd
        erasure_code_crush:
          name: teuthologycrush
          type: erasure
          ...
        erasure_code_use_overwrites: test overwrites, default false
        fast_read: enable ec_pool's fast_read
        min_size: set the min_size of created pool
        num_zones: number of zones for stretched pools
        cleanup: false (defaults to true)
        type: <write|seq|rand> (defaults to write)
        use-pool-config: if true, read pool configuration from overrides/ceph/pool-config
                        instead of from tasks/radosbench (default: false)

    example (traditional style):

    tasks:
    - ceph:
    - radosbench:
        clients: [client.0]
        time: 360
    - interactive:

    example (new pool-config style):

    overrides:
      ceph:
        pool-config:
          ec_pool: true
          erasure_code_profile:
            name: isa42profile
            plugin: isa
            k: 4
            m: 2
            technique: reed_sol_van
            crush-failure-domain: osd
          erasure_code_crush:
            name: teuthologycrush
            type: erasure
          fast_read: true
    tasks:
    - ceph:
    - radosbench:
        use-pool-config: true
        clients: [client.0]
        time: 360
    - interactive:
    """
    log.info('Beginning radosbench...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    
    log.info("config is {config}".format(config=str(config)))
    overrides = ctx.config.get('overrides', {})
    log.info("overrides is {overrides}".format(overrides=str(overrides)))
    
    use_pool_config = config.get('use-pool-config', False)
    
    if use_pool_config:
        log.info("Using pool configuration from overrides/ceph/pool-config")
        pool_config = overrides.get('ceph', {}).get('pool-config', {})
        
        if pool_config:
            log.info("pool-config is {pool_config}".format(pool_config=str(pool_config)))

            pool_settings = [
                'ec_pool',
                'erasure_code_use_overwrites',
                'erasure_code_profile',
                'erasure_code_crush',
                'fast_read',
                'min_size',
                'num_zones',
            ]
            
            for setting in pool_settings:
                if setting in pool_config:
                    if setting not in config:
                        config[setting] = pool_config[setting]
                        log.info("Set {setting} from pool-config: {value}".format(
                            setting=setting, value=pool_config[setting]))
        else:
            log.warning("use-pool-config is true but no pool-config found in overrides/ceph")
    
    _validate_pool_config(config)
    
    radosbench = {}

    testdir = teuthology.get_testdir(ctx)
    manager = ctx.managers['ceph']
    runtype = config.get('type', 'write')

    create_pool = config.get('create_pool', True)
    for role in config.get(
            'clients',
            list(map(lambda x: 'client.' + x,
                     teuthology.all_roles_of_type(ctx.cluster, 'client')))):
        assert isinstance(role, str)
        (_, id_) = role.split('.', 1)
        (remote,) = ctx.cluster.only(role).remotes.keys()

        if config.get('ec_pool', False):
            profile = config.get('erasure_code_profile', {})
            profile_name = profile.get('name', 'teuthologyprofile')
            manager.create_erasure_code_profile(profile_name, profile)
            crush_prof = config.get('erasure_code_crush', {})
            crush_name = None
            if crush_prof:
                crush_name = crush_prof.get('name', 'teuthologycrush')
                manager.create_erasure_code_crush_rule(crush_name, crush_prof)
        else:
            profile_name = None
            crush_name = None

        cleanup = []
        if not config.get('cleanup', True):
            cleanup = ['--no-cleanup']
        write_to_omap = []
        if config.get('write-omap', False):
            write_to_omap = ['--write-omap']
            log.info('omap writes')

        pool = config.get('pool', 'data')
        if create_pool:
            if pool != 'data':
                manager.create_pool(
                    pool,
                    erasure_code_profile_name=profile_name,
                    erasure_code_crush_rule_name=crush_name,
                    erasure_code_use_overwrites=config.get('erasure_code_use_overwrites', False),
                )
            else:
                pool = manager.create_pool_with_unique_name(
                    erasure_code_profile_name=profile_name,
                    erasure_code_crush_rule_name=crush_name,
                    erasure_code_use_overwrites=config.get('erasure_code_use_overwrites', False),
                )
            
            # Apply additional pool settings
            if config.get('fast_read', False):
                manager.raw_cluster_cmd(
                    'osd', 'pool', 'set', pool, 'fast_read', 'true')
            
            min_size = config.get('min_size', None)
            if min_size is not None:
                manager.raw_cluster_cmd(
                    'osd', 'pool', 'set', pool, 'min_size', str(min_size))

        concurrency = config.get('concurrency', 16)
        osize = config.get('objectsize', 65536)
        if osize == 0:
            objectsize = []
        else:
            objectsize = ['--object-size', str(osize)]
        size = ['-b', str(config.get('size', 65536))]
        # If doing a reading run then populate data
        if runtype != "write":
            proc = remote.run(
                args=[
                    "/bin/sh", "-c",
                    " ".join(['adjust-ulimits',
                              'ceph-coverage',
                              '{tdir}/archive/coverage',
                              'rados',
                              '--no-log-to-stderr',
                              '--name', role] +
                              ['-t', str(concurrency)]
                              + size + objectsize +
                              ['-p' , pool,
                          'bench', str(60), "write", "--no-cleanup"
                          ]).format(tdir=testdir),
                ],
            logger=log.getChild('radosbench.{id}'.format(id=id_)),
            wait=True
            )
            size = []
            objectsize = []

        proc = remote.run(
            args=[
                "/bin/sh", "-c",
                " ".join(['adjust-ulimits',
                          'ceph-coverage',
                          '{tdir}/archive/coverage',
                          'rados',
			  '--no-log-to-stderr',
                          '--name', role] +
                          ['-t', str(concurrency)]
                          + size + objectsize +
                          ['-p' , pool,
                          'bench', str(config.get('time', 360)), runtype,
                          ] + write_to_omap + cleanup).format(tdir=testdir),
                ],
            logger=log.getChild('radosbench.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        radosbench[id_] = proc

    try:
        yield
    finally:
        timeout = config.get('time', 360) * 30 + 300
        log.info('joining radosbench (timing out after %ss)', timeout)
        run.wait(radosbench.values(), timeout=timeout)

        if pool != 'data' and create_pool:
            manager.remove_pool(pool)
