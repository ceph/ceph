
import logging
import contextlib
import time
import ceph_manager
from teuthology import misc
from teuthology.orchestra.run import CommandFailedError, Raw

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Go through filesystem creation with a synthetic failure in an MDS
    in its 'up:creating' state, to exercise the retry behaviour.
    """
    # Grab handles to the teuthology objects of interest
    mdslist = list(misc.all_roles_of_type(ctx.cluster, 'mds'))
    if len(mdslist) != 1:
        # Require exactly one MDS, the code path for creation failure when
        # a standby is available is different
        raise RuntimeError("This task requires exactly one MDS")

    mds_id = mdslist[0]
    mds_remote = misc.get_single_remote_value(ctx,
            'mds.{_id}'.format(_id=mds_id))
    manager = ceph_manager.CephManager(
        mds_remote, ctx=ctx, logger=log.getChild('ceph_manager'),
    )

    # Stop the MDS and reset the filesystem so that next start will go into CREATING
    mds = ctx.daemons.get_daemon('mds', mds_id)
    mds.stop()
    data_pool_id = manager.get_pool_num("data")
    md_pool_id = manager.get_pool_num("metadata")
    manager.raw_cluster_cmd_result('mds', 'newfs', md_pool_id.__str__(), data_pool_id.__str__(),
                                   '--yes-i-really-mean-it')

    # Start the MDS with mds_kill_create_at set, it will crash during creation
    mds.restart_with_args(["--mds_kill_create_at=1"])
    try:
        mds.wait_for_exit()
    except CommandFailedError as e:
        if e.exitstatus == 1:
            log.info("MDS creation killed as expected")
        else:
            log.error("Unexpected status code %s" % e.exitstatus)
            raise

    # Since I have intentionally caused a crash, I will clean up the resulting core
    # file to avoid task.internal.coredump seeing it as a failure.
    log.info("Removing core file from synthetic MDS failure")
    mds_remote.run(args=['rm', '-f', Raw("{archive}/coredump/*.core".format(archive=misc.get_archive_dir(ctx)))])

    # It should have left the MDS map state still in CREATING
    status = manager.get_mds_status(mds_id)
    assert status['state'] == 'up:creating'

    # Start the MDS again without the kill flag set, it should proceed with creation successfully
    mds.restart()

    # Wait for state ACTIVE
    t = 0
    create_timeout = 120
    while True:
        status = manager.get_mds_status(mds_id)
        if status['state'] == 'up:active':
            log.info("MDS creation completed successfully")
            break
        elif status['state'] == 'up:creating':
            log.info("MDS still in creating state")
            if t > create_timeout:
                log.error("Creating did not complete within %ss" % create_timeout)
                raise RuntimeError("Creating did not complete within %ss" % create_timeout)
            t += 1
            time.sleep(1)
        else:
            log.error("Unexpected MDS state: %s" % status['state'])
            assert(status['state'] in ['up:active', 'up:creating'])

    # The system should be back up in a happy healthy state, go ahead and run any further tasks
    # inside this context.
    yield
