"""
Multibench testing
"""
import contextlib
import logging
import radosbench
import time
import copy
import gevent

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run multibench

    The config should be as follows:

    multibench:
        time: <seconds to run total>
        segments: <number of concurrent benches>
        radosbench: <config for radosbench>

    example:

    tasks:
    - ceph:
    - multibench:
        clients: [client.0]
        time: 360
    - interactive:
    """
    log.info('Beginning multibench...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    def run_one(num):
        """Run test spawn from gevent"""
        start = time.time()
        benchcontext = copy.copy(config.get('radosbench'))
        iterations = 0
        while time.time() - start < int(config.get('time', 600)):
            log.info("Starting iteration %s of segment %s"%(iterations, num))
            benchcontext['pool'] = str(num) + "-" + str(iterations)
            with radosbench.task(ctx, benchcontext):
                time.sleep()
            iterations += 1
    log.info("Starting %s threads"%(str(config.get('segments', 3)),))
    segments = [
        gevent.spawn(run_one, i)
        for i in range(0, int(config.get('segments', 3)))]

    try:
        yield
    finally:
        [i.get() for i in segments]
