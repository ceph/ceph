from threading import Event, Thread
import errno
try:
    import queue as Queue
except ImportError:
    import Queue

class MDSMonitor(Thread):
    MIN_STANDBYS = 1

    def __init__(self):
        Thread.__init__()
        self.abort = False

    def create_mds(self):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = vol_id
        completion = self.add_stateless_service("mds", spec)
        self._orchestrator_wait([completion])

    def check_standbys():
        # All we care about is if there are insufficient standbys
        fs_map = self.get('fs_map')
        # XXX Or maybe just check insufficient standby count on each FS?
        if len(fs_map['standbys']) < MIN_STANDBYS and len(fs_map['filesystems']) > 0:
			# TODO maybe be a *little* bit smarter and spawn enough MDS to fill all ranks
            self.create_mds()

    def run(self):
        while not self.abort:
            try:
                self.check_standbys()
            except (ImportError, orchestrator.NoOrchestrator):
                self.log.exception("Cannot create MDS daemons; quitting!")
                return
            except Exception as e:
                # Don't let detailed orchestrator exceptions (python backtraces)
                # bubble out to the user
                self.log.exception("Failed to create MDS daemons")
                return -errno.EINVAL, "", str(e)
            time.sleep(5)

class MDSMonitor(orchestrator.OrchestratorClientMixin):
    def __init__(self):
        super(orchestrator.OrchestratorClientMixin, self).__init__()

    def _get_mds_services(self):
        services = self.describe_service(service_type='mds')
 
