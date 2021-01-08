"""
Thrash mds by simulating failures
"""
import logging
import contextlib

from gevent import sleep, GreenletExit
from gevent.greenlet import Greenlet
from gevent.event import Event
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra.run import CommandFailedError

from tasks import ceph_manager
from tasks.cephfs.filesystem import MDSCluster, Filesystem
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)

class ForwardScrubber(Thrasher, Greenlet):
    """
    ForwardScrubber::

    The ForwardScrubber does forward scrubbing of file-systems during execution
    of other tasks (workunits, etc).
    """

    def __init__(self, fs, scrub_timeout=300, sleep_between_iterations=1):
        super(ForwardScrubber, self).__init__()

        self.logger = log.getChild('fs.[{f}]'.format(f=fs.name))
        self.fs = fs
        self.name = 'thrasher.fs.[{f}]'.format(f=fs.name)
        self.stopping = Event()
        self.scrub_timeout = scrub_timeout
        self.sleep_between_iterations = sleep_between_iterations

    def _run(self):
        try:
            self.do_scrub()
        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...

    def stop(self):
        self.stopping.set()

    def do_scrub(self):
        """
        Perform the file-system scrubbing
        """
        self.logger.info(f'start scrubbing fs: {self.fs.name}')

        try:
            while not self.stopping.is_set():
                self._scrub()
                sleep(self.sleep_between_iterations)
        except GreenletExit:
            pass

        self.logger.info(f'end scrubbing fs: {self.fs.name}')

    def _scrub(self, path="/", recursive=True):
        self.logger.info(f"scrubbing fs: {self.fs.name}")
        recopt = ["recursive", "force"] if recursive else ["force"]
        out_json = self.fs.rank_tell(["scrub", "start", path] + recopt)
        assert out_json is not None

        tag = out_json['scrub_tag']

        assert tag is not None
        assert out_json['return_code'] == 0
        assert out_json['mode'] == 'asynchronous'

        return self._wait_until_scrub_complete(tag)

    def _wait_until_scrub_complete(self, tag):
        # time out after scrub_timeout seconds and assume as done
        with contextutil.safe_while(sleep=30, tries=self.scrub_timeout//30) as proceed:
            while proceed():
                try:
                    out_json = self.fs.rank_tell(["scrub", "status"])
                    assert out_json is not None
                    if out_json['status'] == "no active scrubs running":
                        self.logger.info("all active scrubs completed")
                        return

                    status = out_json['scrubs'][tag]
                    if status is not None:
                        self.logger.info(f"scrub status for tag:{tag} - {status}")
                    else:
                        self.logger.info(f"scrub has completed for tag:{tag}")
                        return
                except CommandFailedError as e:
                    self.logger.exception(f"exception while getting scrub status: {e}")
                    self.logger.info("retrying scrub status command in a while")
                    pass

def stop_all_fwd_scrubbers(thrashers):
    for thrasher in thrashers:
        if not isinstance(thrasher, ForwardScrubber):
            continue
        thrasher.stop()
        thrasher.join()
        if thrasher.exception is not None:
            raise RuntimeError(f"error during scrub thrashing: {thrasher.exception}")


@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the mds by running scrub iterations while another task/workunit
    is running.
    Example config:

    - fwd_scrub:
      scrub_timeout: 300
      sleep_between_iterations: 1
    """

    mds_cluster = MDSCluster(ctx)

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'fwd_scrub task only accepts a dict for configuration'
    mdslist = list(teuthology.all_roles_of_type(ctx.cluster, 'mds'))
    assert len(mdslist) > 0, \
        'fwd_scrub task requires at least 1 metadata server'

    (first,) = ctx.cluster.only(f'mds.{mdslist[0]}').remotes.keys()
    manager = ceph_manager.CephManager(
        first, ctx=ctx, logger=log.getChild('ceph_manager'),
    )

    # make sure everyone is in active, standby, or standby-replay
    log.info('Wait for all MDSs to reach steady state...')
    status = mds_cluster.status()
    while True:
        steady = True
        for info in status.get_all():
            state = info['state']
            if state not in ('up:active', 'up:standby', 'up:standby-replay'):
                steady = False
                break
        if steady:
            break
        sleep(2)
        status = mds_cluster.status()

    log.info('Ready to start scrub thrashing')

    manager.wait_for_clean()
    assert manager.is_clean()

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    for fs in status.get_filesystems():
        fwd_scrubber = ForwardScrubber(Filesystem(ctx, fscid=fs['id']),
                                       config['scrub_timeout'],
                                       config['sleep_between_iterations'])
        fwd_scrubber.start()
        ctx.ceph[config['cluster']].thrashers.append(fwd_scrubber)

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining ForwardScrubbers')
        stop_all_fwd_scrubbers(ctx.ceph[config['cluster']].thrashers)
        log.info('done joining')
