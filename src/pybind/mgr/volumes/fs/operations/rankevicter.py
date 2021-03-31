import errno
import json
import logging
import threading
import time

from .volume import get_mds_map
from ..exception import ClusterTimeout, ClusterError

log = logging.getLogger(__name__)

class RankEvicter(threading.Thread):
    """
    Thread for evicting client(s) from a particular MDS daemon instance.

    This is more complex than simply sending a command, because we have to
    handle cases where MDS daemons might not be fully up yet, and/or might
    be transiently unresponsive to commands.
    """
    class GidGone(Exception):
        pass

    POLL_PERIOD = 5

    def __init__(self, mgr, fs, client_spec, volname, rank, gid, mds_map, ready_timeout):
        """
        :param client_spec: list of strings, used as filter arguments to "session evict"
                            pass ["id=123"] to evict a single client with session id 123.
        """
        self.volname = volname
        self.rank = rank
        self.gid = gid
        self._mds_map = mds_map
        self._client_spec = client_spec
        self._fs = fs
        self._ready_timeout = ready_timeout
        self._ready_waited = 0
        self.mgr = mgr

        self.success = False
        self.exception = None

        super(RankEvicter, self).__init__()

    def _ready_to_evict(self):
        if self._mds_map['up'].get("mds_{0}".format(self.rank), None) != self.gid:
            log.info("Evicting {0} from {1}/{2}: rank no longer associated with gid, done.".format(
                self._client_spec, self.rank, self.gid
            ))
            raise RankEvicter.GidGone()

        info = self._mds_map['info']["gid_{0}".format(self.gid)]
        log.debug("_ready_to_evict: state={0}".format(info['state']))
        return info['state'] in ["up:active", "up:clientreplay"]

    def _wait_for_ready(self):
        """
        Wait for that MDS rank to reach an active or clientreplay state, and
        not be laggy.
        """
        while not self._ready_to_evict():
            if self._ready_waited > self._ready_timeout:
                raise ClusterTimeout()

            time.sleep(self.POLL_PERIOD)
            self._ready_waited += self.POLL_PERIOD
            self._mds_map = get_mds_map(self.mgr, self.volname)

    def _evict(self):
        """
        Run the eviction procedure.  Return true on success, false on errors.
        """

        # Wait til the MDS is believed by the mon to be available for commands
        try:
            self._wait_for_ready()
        except self.GidGone:
            return True

        # Then send it an evict
        ret = -errno.ETIMEDOUT
        while ret == -errno.ETIMEDOUT:
            log.debug("mds_command: {0}, {1}".format(
                "%s" % self.gid, ["session", "evict"] + self._client_spec
            ))
            ret, outb, outs = self._fs.mds_command(
                "%s" % self.gid,
                json.dumps({
                                "prefix": "session evict",
                                "filters": self._client_spec
                }), "")
            log.debug("mds_command: complete {0} {1}".format(ret, outs))

            # If we get a clean response, great, it's gone from that rank.
            if ret == 0:
                return True
            elif ret == -errno.ETIMEDOUT:
                # Oh no, the MDS went laggy (that's how libcephfs knows to emit this error)
                self._mds_map = get_mds_map(self.mgr, self.volname)
                try:
                    self._wait_for_ready()
                except self.GidGone:
                    return True
            else:
                raise ClusterError("Sending evict to mds.{0}".format(self.gid), ret, outs)

    def run(self):
        try:
            self._evict()
        except Exception as e:
            self.success = False
            self.exception = e
        else:
            self.success = True
