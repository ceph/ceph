"""
Populate rbd pools
"""
import contextlib
import logging
from ceph_manager import CephManager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Populate <num_pools> pools with prefix <pool_prefix> with <num_images>
    rbd images at <num_snaps> snaps

    The config could be as follows::

        populate_rbd_pool:
          client: <client>
          pool_prefix: foo
          num_pools: 5
          num_images: 10
          num_snaps: 3
          image_size: 10737418240
    """
    if config is None:
        config = {}
    client = config.get("client", "client.0")
    pool_prefix = config.get("pool_prefix", "foo")
    num_pools = config.get("num_pools", 2)
    num_images = config.get("num_images", 20)
    num_snaps = config.get("num_snaps", 4)
    image_size = config.get("image_size", 100)
    write_size = config.get("write_size", 1024*1024)
    write_threads = config.get("write_threads", 10)
    write_total_per_snap = config.get("write_total_per_snap", 1024*1024*30)

    (remote,) = ctx.cluster.only(client).remotes.iterkeys()

    if not hasattr(ctx, 'manager'):
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
        ctx.manager = CephManager(
            mon,
            ctx=ctx,
            logger=log.getChild('ceph_manager'),
            )

    for poolid in range(num_pools):
        poolname = "%s-%s" % (pool_prefix, str(poolid))
        log.info("Creating pool %s" % (poolname,))
        ctx.manager.create_pool(poolname)
        for imageid in range(num_images):
            imagename = "rbd-%s" % (str(imageid),)
            log.info("Creating imagename %s" % (imagename,))
            remote.run(
                args = [
                    "rbd",
                    "create",
                    imagename,
                    "--image-format", "1",
                    "--size", str(image_size),
                    "--pool", str(poolname)])
            def bench_run():
                remote.run(
                    args = [
                        "rbd",
                        "bench-write",
                        imagename,
                        "--pool", poolname,
                        "--io-size", str(write_size),
                        "--io-threads", str(write_threads),
                        "--io-total", str(write_total_per_snap),
                        "--io-pattern", "rand"])
            log.info("imagename %s first bench" % (imagename,))
            bench_run()
            for snapid in range(num_snaps):
                snapname = "snap-%s" % (str(snapid),)
                log.info("imagename %s creating snap %s" % (imagename, snapname))
                remote.run(
                    args = [
                        "rbd", "snap", "create",
                        "--pool", poolname,
                        "--snap", snapname,
                        imagename
                        ])
                bench_run()
            
    try:
        yield
    finally:
        log.info('done')
