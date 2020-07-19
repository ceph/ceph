"""
CephFS sub-tasks.
"""

import logging
import re

from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)

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
            for client, evicted in clients.items():
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
    for client, evicted in clients.items():
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
