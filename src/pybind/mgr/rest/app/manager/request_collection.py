
from threading import RLock
from rest.app.manager.user_request import UserRequest
from rest.logger import logger
log = logger()

from rest.module import global_instance as rest_plugin

TICK_PERIOD = 20


log = log.getChild("request_collection")


class RequestCollection(object):
    """
    Manage a collection of UserRequests, indexed by
    salt JID and request ID.

    Unlike most of cthulhu, this class contains a lock, which
    is used in all entry points which may sleep (anything which
    progresses a UserRequest might involve I/O to create jobs
    in the salt master), so that they don't go to sleep and
    wake up in a different world.
    """

    def __init__(self):
        super(RequestCollection, self).__init__()

        self._by_request_id = {}
        self._lock = RLock()

    def get_by_id(self, request_id):
        return self._by_request_id[request_id]

    def get_all(self, state=None):
        if not state:
            return self._by_request_id.values()
        else:
            return [r for r in self._by_request_id.values() if r.state == state]

    # def tick(self):
    #     """
    #     For walltime-based monitoring of running requests.  Long-running requests
    #     get a periodic call to saltutil.running to verify that things really
    #     are still happening.
    #     """
    #
    #     if not self._by_tag:
    #         return
    #     else:
    #         log.debug("RequestCollection.tick: %s JIDs underway" % len(self._by_tag))
    #
    #     # Identify JIDs who haven't had a saltutil.running reponse for too long.
    #     # Kill requests in a separate phase because request:JID is not 1:1
    #     stale_jobs = set()
    #     _now = now()
    #     for request in self._by_tag.values():
    #         if _now - request.alive_at > datetime.timedelta(seconds=TICK_PERIOD * 3):
    #             log.error("Request %s JID %s stale: now=%s, alive_at=%s" % (
    #                 request.id, request.jid, _now, request.alive_at
    #             ))
    #             stale_jobs.add(request)
    #
    #     # Any identified stale jobs are errored out.
    #     for request in stale_jobs:
    #         with self._update_index(request):
    #             request.set_error("Lost contact")
    #             request.jid = None
    #             request.complete()
    #
    #     # Identify minions associated with JIDs in flight
    #     query_minions = set()
    #     for jid, request in self._by_tag.items():
    #         query_minions.add(request.minion_id)
    #
    #     # Attempt to emit a saltutil.running to ping jobs, next tick we
    #     # will see if we got updates to the alive_at attribute to indicate non-staleness
    #     if query_minions:
    #         log.info("RequestCollection.tick: sending get_running for {0}".format(query_minions))
    #         self._remote.get_running(list(query_minions))

    # def on_tick_response(self, minion_id, jobs):
    #     """
    #     Update the alive_at parameter of requests to record that they
    #     are still running remotely.
    #
    #     :param jobs: The response from a saltutil.running
    #     """
    #     log.debug("RequestCollection.on_tick_response: %s from %s" % (len(jobs), minion_id))
    #     for job in jobs:
    #         try:
    #             request = self._by_tag[job['jid']]
    #         except KeyError:
    #             # Not one of mine, ignore it
    #             pass
    #         else:
    #             request.alive_at = now()

    # def cancel(self, request_id):
    #     """
    #     Immediately mark a request as cancelled, and in the background
    #     try and cancel any outstanding JID for it.
    #     """
    #     request = self._by_request_id[request_id]
    #
    #     # Idempotent behaviour: no-op if already cancelled
    #     if request.state == request.COMPLETE:
    #         return
    #
    #     with self._update_index(request):
    #         # I will take over cancelling the JID from the request
    #         cancel_jid = request.jid
    #         request.jid = None
    #
    #         # Request is now done, no further calls
    #         request.set_error("Cancelled")
    #         request.complete()
    #
    #         # In the background, try to cancel the request's JID on a best-effort basis
    #         if cancel_jid:
    #             self._remote.cancel(request.minion_id, cancel_jid)
    #             # We don't check for completion or errors, it's a best-effort thing.  If we're
    #             # cancelling something we will do our best to kill any subprocess but can't
    #             # any guarantees because running nodes may be out of touch with the calamari server.
    #
    # @nosleep
    # def fail_all(self, failed_minion):
    #     """
    #     For use when we lose contact with the minion that was in use for running
    #     requests: assume all these requests are never going to return now.
    #     """
    #     for request in self.get_all(UserRequest.SUBMITTED):
    #         with self._update_index(request):
    #             request.set_error("Lost contact with server %s" % failed_minion)
    #             if request.jid:
    #                 log.error("Giving up on JID %s" % request.jid)
    #                 request.jid = None
    #             request.complete()

    def submit(self, request):
        """
        Submit a request and store it.  Do this in one operation
        to hold the lock over both operations, otherwise a response
        to a job could arrive before the request was filed here.
        """
        with self._lock:
            log.info("RequestCollection.submit: {0} {1}".format(
                request.id, request.headline))
            self._by_request_id[request.id] = request
            request.submit()

    def on_map(self, sync_type, sync_object):
        """
        Callback for when a new cluster map is available, in which
        we notify any interested ongoing UserRequests of the new map
        so that they can progress if they were waiting for it.
        """
        with self._lock:
            log.info("RequestCollection.on_map: {0}".format(sync_type))
            requests = self.get_all(state=UserRequest.SUBMITTED)
            for request in requests:
                try:
                    # If this is one of the types that this request
                    # is waiting for, invoke on_map.
                    for awaited_type in request.awaiting_versions.keys():
                        if awaited_type == sync_type:
                            request.on_map(sync_type, sync_object)
                except Exception as e:
                    log.error("e.__class__ = {0}".format(e.__class__))
                    log.exception("Request %s threw exception in on_map", request.id)
                    request.set_error("Internal error %s" % e)
                    request.complete()
    #
    # def _on_rados_completion(self, request, result):
    #     """
    #     Handle JID completion from a ceph.rados_commands operation
    #     """
    #     if request.state != UserRequest.SUBMITTED:
    #         # Unexpected, ignore.
    #         log.error("Received completion for request %s/%s in state %s" % (
    #             request.id, request.jid, request.state
    #         ))
    #         return
    #
    #     if result['error']:
    #         # This indicates a failure within ceph.rados_commands which was caught
    #         # by our code, like one of our Ceph commands returned an error code.
    #         # NB in future there may be UserRequest subclasses which want to receive
    #         # and handle these errors themselves, so this branch would be refactored
    #         # to allow that.
    #         log.error("Request %s experienced an error: %s" % (request.id, result['error_status']))
    #         request.jid = None
    #         request.set_error(result['error_status'])
    #         request.complete()
    #         return
    #
    #     try:
    #         request.complete_jid()
    #
    #         # After a jid completes, requests may start waiting for cluster
    #         # map updates, we ask ClusterMonitor to hurry up and get them
    #         # on behalf of the request.
    #         if request.awaiting_versions:
    #             # The request may be waiting for an epoch that we already
    #             # have, if so give it to the request right away
    #             for sync_type, want_version in request.awaiting_versions.items():
    #                 data = ceph_state.get(sync_type)
    #
    #                 if want_version and sync_type.cmp(data['epoch'],
    #                                                   want_version) >= 0:
    #                     log.info(
    #                         "Awaited %s %s is immediately available" % (
    #                         sync_type, want_version))
    #                     request.on_map(sync_type, data)
    #
    #     except Exception as e:
    #         # Ensure that a misbehaving piece of code in a UserRequest subclass
    #         # results in a terminated job, not a zombie job
    #         log.exception("Calling complete_jid for %s/%s" % (request.id, request.jid))
    #         request.jid = None
    #         request.set_error("Internal error %s" % e)
    #         request.complete()

    def on_completion(self, tag):
        """
        Callback for when a salt/job/<jid>/ret event is received, in which
        we find the UserRequest that created the job, and inform it of
        completion so that it can progress.
        """
        with self._lock:
            log.info("RequestCollection.on_completion: {0}".format(tag))

            try:
                request = self.get_by_id(tag)
            except KeyError:
                log.warning("on_completion: unknown tag {0}" % tag)
                return

            request.rados_commands.advance()
            if request.rados_commands.is_complete():
                if request.rados_commands.r == 0:
                    try:
                        request.complete_jid()
                    except Exception as e:
                        log.exception("Request %s threw exception in on_map", request.id)
                        request.set_error("Internal error %s" % e)
                        request.complete()

                    # The request may be waiting for an epoch that we already have, if so
                    # give it to the request right away
                    for sync_type, want_version in request.awaiting_versions.items():
                        sync_object = rest_plugin().get_sync_object(sync_type)
                        if want_version and sync_type.cmp(sync_object.version, want_version) >= 0:
                            log.info("Awaited %s %s is immediately available" % (sync_type, want_version))
                            request.on_map(sync_type, sync_object)
                else:
                    request.set_error(request.rados_commands.outs)
                    request.complete()
