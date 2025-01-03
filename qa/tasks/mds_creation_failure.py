# FIXME: this file has many undefined vars which are accessed!
# flake8: noqa
import logging
import contextlib
import time
from tasks import ceph_manager
from teuthology import misc
from teuthology.exceptions import CommandFailedError
from teuthology.orchestra.run import Raw

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
    (mds_remote,) = ctx.cluster.only('mds.{_id}'.format(_id=mds_id)).remotes.keys()
    manager = ceph_manager.CephManager(
        mds_remote, ctx=ctx, logger=log.getChild('ceph_manager'),
    )

    # Stop MDS
    self.fs.set_max_mds(0)
    self.fs.mds_stop(mds_id)
    self.fs.mds_fail(mds_id)

    # Reset the filesystem so that next start will go into CREATING
    manager.raw_cluster_cmd('fs', 'rm', "default", "--yes-i-really-mean-it")
    manager.raw_cluster_cmd('fs', 'new', "default", "metadata", "data")

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
    status = self.fs.status().get_mds(mds_id)
    assert status['state'] == 'up:creating'

    # Start the MDS again without the kill flag set, it should proceed with creation successfully
    mds.restart()

    # Wait for state ACTIVE
    self.fs.wait_for_state("up:active", timeout=120, mds_id=mds_id)

    # The system should be back up in a happy healthy state, go ahead and run any further tasks
    # inside this context.
    yield
