"""
persistent write log cache thrash task
"""
import contextlib
import logging
import random
import json
import time

from teuthology import misc as teuthology
from teuthology import contextutil

DEFAULT_NUM_ITERATIONS = 20
IO_PATTERNS = ("full-seq", "rand")
IO_SIZES = ('4K', '16K', '128K', '1024K')

log = logging.getLogger(__name__)

@contextlib.contextmanager
def thrashes_rbd_bench_on_persistent_cache(ctx, config):
    """
    thrashes rbd bench on persistent write log cache.
    It can test recovery feature of persistent write log cache.
    """
    log.info("thrashes rbd bench on persistent write log cache")

    client, client_config = list(config.items())[0]
    (remote,) = ctx.cluster.only(client).remotes.keys()
    client_config = client_config if client_config is not None else dict()
    image_name = client_config.get('image_name', 'testimage')
    num_iterations = client_config.get('num_iterations', DEFAULT_NUM_ITERATIONS)

    for i in range(num_iterations):
        log.info("start rbd bench")
        # rbd bench could not specify the run time so set a large enough test size.
        remote.run(
            args=[
                'rbd', 'bench',
                '--io-type', 'write',
                '--io-pattern', random.choice(IO_PATTERNS),
                '--io-size', random.choice(IO_SIZES),
                '--io-total', '100G',
                image_name,
                ],
            wait=False,
        )
        # Wait a few seconds for the rbd bench process to run
        # and complete the pwl cache initialization
        time.sleep(10)
        log.info("dump cache state when rbd bench running.")
        remote.sh(['rbd', 'status', image_name, '--format=json'])
        log.info("sleep...")
        time.sleep(random.randint(10, 60))
        log.info("rbd bench crash.")
        remote.run(
            args=[
                'killall', '-9', 'rbd',
                ],
            check_status=False,
        )
        log.info("wait for watch timeout.")
        time.sleep(40)
        log.info("check cache state after crash.")
        out = remote.sh(['rbd', 'status', image_name, '--format=json'])
        rbd_status = json.loads(out)
        assert len(rbd_status['watchers']) == 0
        assert rbd_status['persistent_cache']['present'] == True
        assert rbd_status['persistent_cache']['empty'] == False
        assert rbd_status['persistent_cache']['clean'] == False
        log.info("check dirty cache file.")
        remote.run(
            args=[
                'test', '-e', rbd_status['persistent_cache']['path'],
                ]
        )
    try:
        yield
    finally:
        log.info("cleanup")

@contextlib.contextmanager
def task(ctx, config):
    """
    This is task for testing persistent write log cache thrash.
    """
    assert isinstance(config, dict), \
            "task persistent_write_log_cache_thrash only supports a dictionary for configuration"

    managers = []
    config = teuthology.replace_all_with_clients(ctx.cluster, config)
    managers.append(
        lambda: thrashes_rbd_bench_on_persistent_cache(ctx=ctx, config=config)
        )

    with contextutil.nested(*managers):
        yield
