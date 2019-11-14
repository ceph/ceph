"""
CephFS sub-tasks.
"""

import contextlib
import logging
import re
import time

from gevent import sleep
from gevent.greenlet import Greenlet
from gevent.event import Event

from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)

class MemoryWatchdog(Greenlet):
    def __init__(self, ctx, config):
        super(MemoryWatchdog, self).__init__()
        self.ctx = ctx
        self.config = config
        self.e = None
        self.logger = log.getChild('memory_watch')
        self.cluster = config.get('cluster', 'ceph')
        self.name = 'memorywatchdog'
        self.stopping = Event()

    def _run(self):
        try:
            self.watch()
        except Exception as e:
            self.e = e
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...

    def log(self, x):
        """Write data to logger"""
        self.logger.info(x)

    def stop(self):
        self.stopping.set()

    def watch(self):
        self.log("MemoryWatchdog starting")

        while not self.stopping.is_set():
            status = self.fs.status()
            max_mds = status.get_fsmap(self.fs.id)['mdsmap']['max_mds']
            ranks = list(status.get_ranks(self.fs.id))
            actives = filter(lambda info: "up:active" == info['state'] and "laggy_since" not in info, ranks)
            for rank in actives:
                mem_info = self.fs.rank_asok(["perf", "dump"], rank)["mds_mem"]
                self.log("Usage statistic for mds ", self.get_rank(rank=rank, status=status)["name"])
                self.log(mem_info)
                assertGreaterEqual(((int(self.fs.get_config("mds_memory_target")) * 125)/100), mem_info["rss"], "rss usage has exceeded the mds_memory_target")

        self.log("MemoryWatchdog finished")

def clients_evicted(ctx, config):
    """
    Check clients are evicted, unmount (cleanup) if so.
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'task only accepts a dict for configuration'

    clients = config.get('clients')

    if clients is None:
        clients = {("client."+client_id): True for client_id in ctx.mounts}

    log.info("clients is {}".format(str(clients)))

    fs = Filesystem(ctx)
    status = fs.status()

    has_session = set()
    mounts = {}
    for client in clients:
        client_id = re.match("^client.([0-9]+)$", client).groups(1)[0]
        mounts[client] = ctx.mounts.get(client_id)

    for rank in fs.get_ranks(status=status):
        ls = fs.rank_asok(['session', 'ls'], rank=rank['rank'], status=status)
        for session in ls:
            for client, evicted in clients.viewitems():
                mount = mounts.get(client)
                if mount is not None:
                    global_id = mount.get_global_id()
                    if session['id'] == global_id:
                        if evicted:
                            raise RuntimeError("client still has session: {}".format(str(session)))
                        else:
                            log.info("client {} has a session with MDS {}.{}".format(client, fs.id, rank['rank']))
                            has_session.add(client)

    no_session = set(clients) - has_session
    should_assert = False
    for client, evicted in clients.viewitems():
        mount = mounts.get(client)
        if mount is not None:
            if evicted:
                log.info("confirming client {} is blacklisted".format(client))
                assert mount.is_blacklisted()
            elif client in no_session:
                log.info("client {} should not be evicted but has no session with an MDS".format(client))
                mount.is_blacklisted() # for debugging
                should_assert = True
    if should_assert:
        raise RuntimeError("some clients which should not be evicted have no session with an MDS?")
