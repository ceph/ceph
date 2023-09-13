"""
CephFS sub-tasks.
"""

import logging
import re

from tasks.cephfs.filesystem import Filesystem, MDSCluster

log = logging.getLogger(__name__)

# Everything up to CEPH_MDSMAP_ALLOW_STANDBY_REPLAY
CEPH_MDSMAP_ALLOW_STANDBY_REPLAY = (1<<5)
CEPH_MDSMAP_NOT_JOINABLE = (1 << 0)
CEPH_MDSMAP_LAST = CEPH_MDSMAP_ALLOW_STANDBY_REPLAY
UPGRADE_FLAGS_MASK = ((CEPH_MDSMAP_LAST<<1) - 1)
def pre_upgrade_save(ctx, config):
    """
    That the upgrade procedure doesn't clobber state: save state.
    """

    mdsc = MDSCluster(ctx)
    status = mdsc.status()

    state = {}
    ctx['mds-upgrade-state'] = state

    for fs in list(status.get_filesystems()):
        fscid = fs['id']
        mdsmap = fs['mdsmap']
        fs_state = {}
        fs_state['epoch'] = mdsmap['epoch']
        fs_state['max_mds'] = mdsmap['max_mds']
        fs_state['flags'] = mdsmap['flags'] & UPGRADE_FLAGS_MASK
        state[fscid] = fs_state
        log.debug(f"fs fscid={fscid},name={mdsmap['fs_name']} state = {fs_state}")


def post_upgrade_checks(ctx, config):
    """
    That the upgrade procedure doesn't clobber state.
    """

    state = ctx['mds-upgrade-state']

    mdsc = MDSCluster(ctx)
    status = mdsc.status()

    for fs in list(status.get_filesystems()):
        fscid = fs['id']
        mdsmap = fs['mdsmap']
        fs_state = state[fscid]
        log.debug(f"checking fs fscid={fscid},name={mdsmap['fs_name']} state = {fs_state}")

        # check state was restored to previous values
        assert fs_state['max_mds'] == mdsmap['max_mds']
        assert fs_state['flags'] == (mdsmap['flags'] & UPGRADE_FLAGS_MASK)

        # now confirm that the upgrade procedure was followed
        epoch = mdsmap['epoch']
        pre_upgrade_epoch = fs_state['epoch']
        assert pre_upgrade_epoch < epoch
        multiple_max_mds = fs_state['max_mds'] > 1
        did_decrease_max_mds = False
        should_disable_allow_standby_replay = fs_state['flags'] & CEPH_MDSMAP_ALLOW_STANDBY_REPLAY
        did_disable_allow_standby_replay = False
        did_fail_fs = False
        for i in range(pre_upgrade_epoch+1, mdsmap['epoch']):
            old_status = mdsc.status(epoch=i)
            old_fs = old_status.get_fsmap(fscid)
            old_mdsmap = old_fs['mdsmap']
            if not multiple_max_mds \
                    and (old_mdsmap['flags'] & CEPH_MDSMAP_NOT_JOINABLE):
                raise RuntimeError('mgr is failing fs when there is only one '
                                   f'rank in epoch {i}.')
            if multiple_max_mds \
                    and (old_mdsmap['flags'] & CEPH_MDSMAP_NOT_JOINABLE) \
                    and old_mdsmap['max_mds'] == 1:
                raise RuntimeError('mgr is failing fs as well the max_mds '
                                   f'is reduced in epoch {i}')
            if old_mdsmap['flags'] & CEPH_MDSMAP_NOT_JOINABLE:
                log.debug(f"max_mds not reduced in epoch {i} as fs was failed "
                          "for carrying out rapid multi-rank mds upgrade")
                did_fail_fs = True
            if multiple_max_mds and old_mdsmap['max_mds'] == 1:
                log.debug(f"max_mds reduced in epoch {i}")
                did_decrease_max_mds = True
            if should_disable_allow_standby_replay and not (old_mdsmap['flags'] & CEPH_MDSMAP_ALLOW_STANDBY_REPLAY):
                log.debug(f"allow_standby_replay disabled in epoch {i}")
                did_disable_allow_standby_replay = True
        assert not multiple_max_mds or did_fail_fs or did_decrease_max_mds
        assert not should_disable_allow_standby_replay or did_disable_allow_standby_replay


def ready(ctx, config):
    """
    That the file system is ready for clients.
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'task only accepts a dict for configuration'

    timeout = config.get('timeout', 300)

    mdsc = MDSCluster(ctx)
    status = mdsc.status()

    for filesystem in status.get_filesystems():
        fs = Filesystem(ctx, fscid=filesystem['id'])
        fs.wait_for_daemons(timeout=timeout, status=status)

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
                log.info("confirming client {} is blocklisted".format(client))
                assert fs.is_addr_blocklisted(mount.get_global_addr())
            elif client in no_session:
                log.info("client {} should not be evicted but has no session with an MDS".format(client))
                fs.is_addr_blocklisted(mount.get_global_addr()) # for debugging
                should_assert = True
    if should_assert:
        raise RuntimeError("some clients which should not be evicted have no session with an MDS?")
