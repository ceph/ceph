import logging
import contextlib
import time
from teuthology import misc
from gevent.greenlet import Greenlet
from gevent.event import Event
from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)

'''
    parameters:

    filesize: Optional Param. Unit in MB, default file size 2MB
              Size of the data file before creating snapshot.
              Not of use if workunit task running in parallel.

    runtime: Required Param for using rsync task sequentially. unit in seconds. default 0 sec.
             Don't use this param during parallel execution. if used then finally block will be delayed

    data_dir: Don't use this param if not rsyncing data/snap from workunit data
              Optional Param. Specify the client like client.0/client.1
              make sure cephfs mount and workunit task using this client according to scenario.
              Default source directory will be source/subdir for other scenarios.

    snapenable: Optional Param. value in boolean (True or False), default False(disabled).
                using this option will enable cephfs snapshot.

    waittime: Optional Param. units in seconds. default 5 sec.
              During rsync iteration each loop will wait for specified seconds.

    mountpoint: Optional param. default it will be first ceph-fuse client id.
            eg: if there are 3 cephfs clients then for client.0 use 0, client.1 use 1
            Setting this option will run rsync on this particular client, make sure cephfs mount available.

    Examples:
    rsync task parallel with another task
    tasks:
      - ceph:
      - ceph-fuse:
      - rsync:
            waittime: 10
            snapenable: True
            filesize: 8
      - cephfs_test_runner:

    rsync data from workunit directory in same client.
    tasks:
      - ceph:
      - ceph-fuse: [client.2]
      - rsync:
            waittime: 10
            mountpoint: 2
            data_dir: client.2
      - workunit:
            clients:
            client.2:
                - suites/iozone.sh

    rsync task parallel with workunit but on different client
    tasks:
      - ceph:
      - ceph-fuse:
      - rsync:
            waittime: 10
            mountpoint: 1
      - workunit:
            clients:
            client.2:
                - suites/iozone.sh

    rsync task sequentially based on runtime. default runtime is 0
    tasks:
      - ceph:
      - ceph-fuse:
      - rsync:
            runtime: 120
            mountpoint: 2
            snapenable: True

'''

class RSync(Greenlet):

    def __init__(self, ctx, config, logger):
        super(RSync, self).__init__()

        self.ctx = ctx

        self.stopping = Event()
        self.config = config
        self.logger = logger

        self.my_mnt = None
        self.work_unit = False

        self.file_size = self.config.get('filesize', 2)
        self.wait_time = self.config.get('waittime', 5)
        self.mount_point = self.config.get('mountpoint')

        self.fs = Filesystem(self.ctx)

        self.snap_enable = bool(self.config.get('snapenable', False))

        #Get CephFS mount client and mount object
        if len(self.ctx.mounts.items()):
            flag = False
            for i, j in sorted(self.ctx.mounts.items()):
                tmp = int(i)
                if tmp == self.mount_point:
                    self.mount_point = tmp
                    self.my_mnt = j
                if (not self.mount_point) and (not flag):
                    self.mount_point = tmp
                    self.my_mnt = j
                    flag = True
        else:
            assert len(self.ctx.mounts.items()) > 0, 'No mount available asserting rsync'

        #Set source directory for rsync
        if self.config.get('data_dir'):
            self.work_unit = True
            self.source_dir = misc.get_testdir(self.ctx) + '/mnt.{}'.format(self.mount_point) + \
                              '/{}/'.format(self.config.get('data_dir'))
        else:
            self.data_dir = misc.get_testdir(self.ctx) + '/mnt.{}/'.format(self.mount_point) + 'source'
            self.source_dir = self.data_dir + '/subdir'

    def _run(self):
        try:
            self.do_rsync()
        except:
            # Log exceptions here so we get the full backtrace (it's lost
            # by the time someone does a .get() on this greenlet)
            self.logger.exception("Exception in do_rsync:")
            raise

    def stop(self):
        self.stopping.set()

    #Function to check directory exists.
    def check_if_dir_exists(self, path):
        """
            Call this to check stat of directory.
            :return: True if directory exists.
                     False if not.
        """
        try:
            self.my_mnt.stat(path)
            return True
        except Exception, e :
            logging.error(e)
            return False

    def do_rsync(self):

        iteration = 0
        finished = False

        # Create destination directory
        self.my_mnt.run_shell(["mkdir", "rsyncdir"])

        if self.snap_enable:
            # Enable snapshots
            self.fs.mon_manager.raw_cluster_cmd("mds", "set", "allow_new_snaps", "true", "--yes-i-really-mean-it")

        if not self.work_unit:
            # Create a data directory, sub directory and rsync directory
            self.my_mnt.run_shell(["mkdir", "{}".format(self.data_dir)])
            self.my_mnt.run_shell(["mkdir", "{}".format(self.source_dir)])
            should_stop = False

        #Check for source directory exists
        while not self.check_if_dir_exists(self.source_dir):
            time.sleep(5) # if source dorectory not exists wait for 5s and poll
            iteration += 1
            if iteration > 5:
                assert self.check_if_dir_exists(self.source_dir), 'assert, source Directory doesnot exists'

        # Start observing the event started by workunit task.
        if self.work_unit:
            should_stop = self.ctx.workunit_state.start_observing()

        while not (should_stop or self.stopping.is_set()):

            # rsync data from snapshot. snap is created using workunit IO data
            if self.work_unit and self.snap_enable:

                snap_shot = self.source_dir + '.snap/snap' + '{}'.format(iteration)

                # Create Snapshot
                self.my_mnt.run_shell(["mkdir", "{}".format(snap_shot)])
                iteration += 1

                self.my_mnt.run_shell(["rsync", "-azvh", "{}".format(snap_shot), "rsyncdir/dir1/"])

                # Delete snapshot
                self.my_mnt.run_shell(["rmdir", "{}".format(snap_shot)])

                # Check for even handler stop message
                finished = self.ctx.workunit_state.observer_should_stop()

            # rsync data from snapshot, snap is created using written pattern data
            elif self.snap_enable:
                # Create file and add data to the file
                self.my_mnt.write_test_pattern("{}/file_a".format(self.source_dir), self.file_size * 1024 * 1024)
                snap_shot = self.data_dir + '/.snap/snap' + '{}'.format(iteration)

                # Create Snapshot
                self.my_mnt.run_shell(["mkdir", "{}".format(snap_shot)])
                iteration += 1

                self.my_mnt.run_shell(["rsync", "-azvh", "{}".format(snap_shot), "rsyncdir/dir{}/".format(iteration)])

                # Delete snapshot
                self.my_mnt.run_shell(["rmdir", "{}".format(snap_shot)])

                # Delete the file created in data directory
                self.my_mnt.run_shell(["rm", "-f", "{}/file_a".format(self.source_dir)])

            # rsync data from workunit IO data
            elif self.work_unit:
                self.my_mnt.run_shell(["rsync", "-azvh", "{}".format(self.source_dir), "rsyncdir/dir1/"])
                # Check for event handler stop message
                finished = self.ctx.workunit_state.observer_should_stop()

            # rsync data from written pattern data
            else:
                # Create file and add data to the file
                self.my_mnt.write_test_pattern("{}/file_a".format(self.source_dir), self.file_size * 1024 * 1024)

                self.my_mnt.run_shell(["rsync", "-azvh", "{}".format(self.source_dir), "rsyncdir/dir{}/".format(iteration)])
                iteration += 1

                # Delete the file created in data directory
                self.my_mnt.run_shell(["rm", "-f", "{}/file_a".format(self.source_dir)])

            # Send back stop request to event handler in workunit task.
            if finished:
                self.ctx.workunit_state.stop_observing()

            time.sleep(self.wait_time)

@contextlib.contextmanager
def task(ctx, config):

    log.info('Beginning rsync...')

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "rsync task accepts dict for running configuration"

    run_time = config.get('runtime', 0)

    log.info("Create object and start the gevent thread")
    start_rsync = RSync(ctx, config, logger=log.getChild('rsync'))
    start_rsync.start()
    start_rsync_thread = start_rsync

    try:
        log.debug('Yielding')
        yield
        time.sleep(run_time)
    finally:
        log.info('joining rsync thread')
        start_rsync_thread.stop()
        start_rsync_thread.get()
        start_rsync_thread.join()
        log.info("Done joining")
