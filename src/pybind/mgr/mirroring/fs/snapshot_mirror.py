import base64
import errno
import json
import logging
import os
import pickle
import re
import stat
import threading
import uuid
from typing import Dict, Any

import cephfs
import rados

from mgr_util import RTimer, CephfsClient, open_filesystem,\
    CephfsConnectionException
from mgr_module import NotifyType
from .blocklist import blocklist
from .notify import Notifier, InstanceWatcher
from .utils import INSTANCE_ID_PREFIX, MIRROR_OBJECT_NAME, Finisher, \
    AsyncOpTracker, connect_to_filesystem, disconnect_from_filesystem
from .exception import MirrorException
from .dir_map.create import create_mirror_object
from .dir_map.load import load_dir_map, load_instances
from .dir_map.update import UpdateDirMapRequest, UpdateInstanceRequest
from .dir_map.policy import Policy
from .dir_map.state_transition import ActionType

log = logging.getLogger(__name__)

CEPHFS_IMAGE_POLICY_UPDATE_THROTTLE_INTERVAL = 1

class FSPolicy:
    class InstanceListener(InstanceWatcher.Listener):
        def __init__(self, fspolicy):
            self.fspolicy = fspolicy

        def handle_instances(self, added, removed):
            self.fspolicy.update_instances(added, removed)

    def __init__(self, mgr, ioctx):
        self.mgr = mgr
        self.ioctx = ioctx
        self.pending = []
        self.policy = Policy()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.dir_paths = []
        self.async_requests = {}
        self.finisher = Finisher()
        self.op_tracker = AsyncOpTracker()
        self.notifier = Notifier(ioctx)
        self.instance_listener = FSPolicy.InstanceListener(self)
        self.instance_watcher = None
        self.stopping = threading.Event()
        self.timer_task = RTimer(CEPHFS_IMAGE_POLICY_UPDATE_THROTTLE_INTERVAL,
                                 self.process_updates)
        self.timer_task.start()

    def schedule_action(self, dir_paths):
        self.dir_paths.extend(dir_paths)

    def init(self, dir_mapping, instances):
        with self.lock:
            self.policy.init(dir_mapping)
            # we'll schedule action for all directories, so don't bother capturing
            # directory names here.
            self.policy.add_instances(list(instances.keys()), initial_update=True)
            self.instance_watcher = InstanceWatcher(self.ioctx, instances,
                                                    self.instance_listener)
            self.schedule_action(list(dir_mapping.keys()))

    def shutdown(self):
        with self.lock:
            log.debug('FSPolicy.shutdown')
            self.stopping.set()
            log.debug('canceling update timer task')
            self.timer_task.cancel()
            log.debug('update timer task canceled')
        if self.instance_watcher:
            log.debug('stopping instance watcher')
            self.instance_watcher.wait_and_stop()
            log.debug('stopping instance watcher')
        self.op_tracker.wait_for_ops()
        log.debug('FSPolicy.shutdown done')

    def handle_update_mapping(self, updates, removals, request_id, callback, r):
        log.info(f'handle_update_mapping: {updates} {removals} {request_id} {callback} {r}')
        with self.lock:
            try:
                self.async_requests.pop(request_id)
                if callback:
                    callback(updates, removals, r)
            finally:
                self.op_tracker.finish_async_op()

    def handle_update_instances(self, instances_added, instances_removed, request_id, r):
        log.info(f'handle_update_instances: {instances_added} {instances_removed} {request_id} {r}')
        with self.lock:
            try:
                self.async_requests.pop(request_id)
                if self.stopping.is_set():
                    log.debug(f'handle_update_instances: policy shutting down')
                    return
                schedules = []
                if instances_removed:
                    schedules.extend(self.policy.remove_instances(instances_removed))
                if instances_added:
                    schedules.extend(self.policy.add_instances(instances_added))
                self.schedule_action(schedules)
            finally:
                self.op_tracker.finish_async_op()

    def update_mapping(self, update_map, removals, callback=None):
        log.info(f'updating directory map: {len(update_map)}+{len(removals)} updates')
        request_id = str(uuid.uuid4())
        def async_callback(r):
            self.finisher.queue(self.handle_update_mapping,
                                [list(update_map.keys()), removals, request_id, callback, r])
        request = UpdateDirMapRequest(self.ioctx, update_map.copy(), removals.copy(), async_callback)
        self.async_requests[request_id] = request
        self.op_tracker.start_async_op()
        log.debug(f'async request_id: {request_id}')
        request.send()

    def update_instances(self, added, removed):
        logging.debug(f'update_instances: added={added}, removed={removed}')
        for instance_id, addr in removed.items():
            log.info(f'blocklisting instance_id: {instance_id} addr: {addr}')
            blocklist(self.mgr, addr)
        with self.lock:
            instances_added = {}
            instances_removed = []
            for instance_id, addr in added.items():
                instances_added[instance_id] = {'version': 1, 'addr': addr}
            instances_removed = list(removed.keys())
            request_id = str(uuid.uuid4())
            def async_callback(r):
                self.finisher.queue(self.handle_update_instances,
                                    [list(instances_added.keys()), instances_removed, request_id, r])
            # blacklisted instances can be removed at this point. remapping directories
            # mapped to blacklisted instances on module startup is handled in policy
            # add_instances().
            request = UpdateInstanceRequest(self.ioctx, instances_added.copy(),
                                            instances_removed.copy(), async_callback)
            self.async_requests[request_id] = request
            log.debug(f'async request_id: {request_id}')
            self.op_tracker.start_async_op()
            request.send()

    def continue_action(self, updates, removals, r):
        log.debug(f'continuing action: {updates}+{removals} r={r}')
        if self.stopping.is_set():
            log.debug('continue_action: policy shutting down')
            return
        schedules = []
        for dir_path in updates:
            schedule = self.policy.finish_action(dir_path, r)
            if schedule:
                schedules.append(dir_path)
        for dir_path in removals:
            schedule = self.policy.finish_action(dir_path, r)
            if schedule:
                schedules.append(dir_path)
        self.schedule_action(schedules)

    def handle_peer_ack(self, dir_path, r):
        log.info(f'handle_peer_ack: {dir_path} r={r}')
        with self.lock:
            try:
                if self.stopping.is_set():
                    log.debug(f'handle_peer_ack: policy shutting down')
                    return
                self.continue_action([dir_path], [], r)
            finally:
                self.op_tracker.finish_async_op()

    def process_updates(self):
        def acquire_message(dir_path):
            return json.dumps({'dir_path': dir_path,
                               'mode': 'acquire'
                               })
        def release_message(dir_path):
            return json.dumps({'dir_path': dir_path,
                               'mode': 'release'
                               })
        with self.lock:
            if not self.dir_paths or self.stopping.is_set():
                return
            update_map = {}
            removals = []
            notifies = {}
            instance_purges = []
            for dir_path in self.dir_paths:
                action_type = self.policy.start_action(dir_path)
                lookup_info = self.policy.lookup(dir_path)
                log.debug(f'processing action: dir_path: {dir_path}, lookup_info: {lookup_info}, action_type: {action_type}')
                if action_type == ActionType.NONE:
                    continue
                elif action_type == ActionType.MAP_UPDATE:
                    # take care to not overwrite purge status
                    update_map[dir_path] = {'version': 1,
                                            'instance_id': lookup_info['instance_id'],
                                            'last_shuffled': lookup_info['mapped_time']
                    }
                    if lookup_info['purging']:
                        update_map[dir_path]['purging'] = 1
                elif action_type == ActionType.MAP_REMOVE:
                    removals.append(dir_path)
                elif action_type == ActionType.ACQUIRE:
                    notifies[dir_path] = (lookup_info['instance_id'], acquire_message(dir_path))
                elif action_type == ActionType.RELEASE:
                    notifies[dir_path] = (lookup_info['instance_id'], release_message(dir_path))
            if update_map or removals:
                self.update_mapping(update_map, removals, callback=self.continue_action)
            for dir_path, message in notifies.items():
                self.op_tracker.start_async_op()
                self.notifier.notify(dir_path, message, self.handle_peer_ack)
            self.dir_paths.clear()

    def add_dir(self, dir_path):
        with self.lock:
            lookup_info = self.policy.lookup(dir_path)
            if lookup_info:
                if lookup_info['purging']:
                    raise MirrorException(-errno.EAGAIN, f'remove in-progress for {dir_path}')
                else:
                    raise MirrorException(-errno.EEXIST, f'directory {dir_path} is already tracked')
            schedule = self.policy.add_dir(dir_path)
            if not schedule:
                return
            update_map = {dir_path: {'version': 1, 'instance_id': '', 'last_shuffled': 0.0}}
            updated = False
            def update_safe(updates, removals, r):
                nonlocal updated
                updated = True
                self.cond.notifyAll()
            self.update_mapping(update_map, [], callback=update_safe)
            self.cond.wait_for(lambda: updated)
            self.schedule_action([dir_path])

    def remove_dir(self, dir_path):
        with self.lock:
            lookup_info = self.policy.lookup(dir_path)
            if not lookup_info:
                raise MirrorException(-errno.ENOENT, f'directory {dir_path} id not tracked')
            if lookup_info['purging']:
                raise MirrorException(-errno.EINVAL, f'directory {dir_path} is under removal')
            update_map = {dir_path: {'version': 1,
                                     'instance_id': lookup_info['instance_id'],
                                     'last_shuffled': lookup_info['mapped_time'],
                                     'purging': 1}}
            updated = False
            sync_lock = threading.Lock()
            sync_cond = threading.Condition(sync_lock)
            def update_safe(r):
                with sync_lock:
                    nonlocal updated
                    updated = True
                    sync_cond.notifyAll()
            request = UpdateDirMapRequest(self.ioctx, update_map.copy(), [], update_safe)
            request.send()
            with sync_lock:
                sync_cond.wait_for(lambda: updated)
            schedule = self.policy.remove_dir(dir_path)
            if schedule:
                self.schedule_action([dir_path])

    def status(self, dir_path):
        with self.lock:
            res = self.policy.dir_status(dir_path)
            return 0, json.dumps(res, indent=4, sort_keys=True), ''

    def summary(self):
        with self.lock:
            res = self.policy.instance_summary()
            return 0, json.dumps(res, indent=4, sort_keys=True), ''

class FSSnapshotMirror:
    PEER_CONFIG_KEY_PREFIX = "cephfs/mirror/peer"

    def __init__(self, mgr):
        self.mgr = mgr
        self.rados = mgr.rados
        self.pool_policy = {}
        self.fs_map = self.mgr.get('fs_map')
        self.lock = threading.Lock()
        self.refresh_pool_policy()
        self.local_fs = CephfsClient(mgr)

    def notify(self, notify_type: NotifyType):
        log.debug(f'got notify type {notify_type}')
        if notify_type == NotifyType.fs_map:
            with self.lock:
                self.fs_map = self.mgr.get('fs_map')
                self.refresh_pool_policy_locked()

    @staticmethod
    def make_spec(client_name, cluster_name):
        return f'{client_name}@{cluster_name}'

    @staticmethod
    def split_spec(spec):
        try:
            client_id, cluster_name = spec.split('@')
            _, client_name = client_id.split('.')
            return client_name, cluster_name
        except ValueError:
            raise MirrorException(-errno.EINVAL, f'invalid cluster spec {spec}')

    @staticmethod
    def get_metadata_pool(filesystem, fs_map):
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return fs['mdsmap']['metadata_pool']
        return None

    @staticmethod
    def get_filesystem_id(filesystem, fs_map):
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return fs['id']
        return None

    @staticmethod
    def peer_config_key(filesystem, peer_uuid):
        return f'{FSSnapshotMirror.PEER_CONFIG_KEY_PREFIX}/{filesystem}/{peer_uuid}'

    def config_set(self, key, val=None):
        """set or remove a key from mon config store"""
        if val:
            cmd = {'prefix': 'config-key set',
                   'key': key, 'val': val}
        else:
            cmd = {'prefix': 'config-key rm',
                   'key': key}
        r, outs, err = self.mgr.mon_command(cmd)
        if r < 0:
            log.error(f'mon command to set/remove config-key {key} failed: {err}')
            raise Exception(-errno.EINVAL)

    def config_get(self, key):
        """fetch a config key value from mon config store"""
        cmd = {'prefix': 'config-key get', 'key': key}
        r, outs, err = self.mgr.mon_command(cmd)
        if r < 0 and not r == -errno.ENOENT:
            log.error(f'mon command to get config-key {key} failed: {err}')
            raise Exception(-errno.EINVAL)
        val = {}
        if r == 0:
            val = json.loads(outs)
        return val

    def filesystem_exist(self, filesystem):
        for fs in self.fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return True
        return False

    def get_mirrored_filesystems(self):
        return [fs['mdsmap']['fs_name'] for fs in self.fs_map['filesystems'] if fs.get('mirror_info', None)]

    def get_filesystem_peers(self, filesystem):
        """To be used when mirroring in enabled for the filesystem"""
        for fs in self.fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return fs['mirror_info']['peers']
        return None

    def peer_exists(self, filesystem, remote_cluster_spec, remote_fs_name):
        peers = self.get_filesystem_peers(filesystem)
        for _, rem in peers.items():
            remote = rem['remote']
            spec = FSSnapshotMirror.make_spec(remote['client_name'], remote['cluster_name'])
            if spec == remote_cluster_spec and remote['fs_name'] == remote_fs_name:
                return True
        return False

    @staticmethod
    def get_mirror_info(fs):
        try:
            val = fs.getxattr('/', 'ceph.mirror.info')
            match = re.search(r'^cluster_id=([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}) fs_id=(\d+)$',
                              val.decode('utf-8'))
            if match and len(match.groups()) == 2:
                return {'cluster_id': match.group(1),
                        'fs_id': int(match.group(2))
                        }
            raise MirrorException(-errno.EINVAL, 'invalid ceph.mirror.info value format')
        except cephfs.Error as e:
            raise MirrorException(-e.errno, 'error fetching ceph.mirror.info xattr')

    @staticmethod
    def set_mirror_info(local_cluster_id, local_fsid, remote_fs):
        log.info(f'setting {local_cluster_id}::{local_fsid} on remote')
        try:
            remote_fs.setxattr('/', 'ceph.mirror.info',
                               f'cluster_id={local_cluster_id} fs_id={local_fsid}'.encode('utf-8'), os.XATTR_CREATE)
        except cephfs.Error as e:
            if e.errno == errno.EEXIST:
                try:
                    mi = FSSnapshotMirror.get_mirror_info(remote_fs)
                    cluster_id = mi['cluster_id']
                    fs_id = mi['fs_id']
                    if not (cluster_id == local_cluster_id and fs_id == local_fsid):
                        raise MirrorException(-errno.EEXIST, f'peer mirrorred by: (cluster_id: {cluster_id}, fs_id: {fs_id})')
                except MirrorException:
                    # if mirror info cannot be fetched for some reason, let's just
                    # fail.
                    raise MirrorException(-errno.EEXIST, f'already an active peer')
            else:
                log.error(f'error setting mirrored fsid: {e}')
                raise Exception(-e.errno)

    def resolve_peer(self, fs_name, peer_uuid):
        peers = self.get_filesystem_peers(fs_name)
        for peer, rem in peers.items():
            if peer == peer_uuid:
                return rem['remote']
        return None

    def purge_mirror_info(self, local_fs_name, peer_uuid):
        log.debug(f'local fs={local_fs_name} peer_uuid={peer_uuid}')
        # resolve the peer to its spec
        rem = self.resolve_peer(local_fs_name, peer_uuid)
        if not rem:
            return
        log.debug(f'peer_uuid={peer_uuid} resolved to {rem}')
        _, client_name = rem['client_name'].split('.')

        # fetch auth details from config store
        remote_conf = self.config_get(FSSnapshotMirror.peer_config_key(local_fs_name, peer_uuid))
        remote_cluster, remote_fs = connect_to_filesystem(client_name,
                                                          rem['cluster_name'],
                                                          rem['fs_name'], 'remote', conf_dct=remote_conf)
        try:
            remote_fs.removexattr('/', 'ceph.mirror.info')
        except cephfs.Error as e:
            if not e.errno == errno.ENOENT:
                log.error('error removing mirror info')
                raise Exception(-e.errno)
        finally:
            disconnect_from_filesystem(rem['cluster_name'], rem['fs_name'], remote_cluster, remote_fs)

    def verify_and_set_mirror_info(self, local_fs_name, remote_cluster_spec, remote_fs_name, remote_conf={}):
        log.debug(f'local fs={local_fs_name} remote={remote_cluster_spec}/{remote_fs_name}')

        client_name, cluster_name = FSSnapshotMirror.split_spec(remote_cluster_spec)
        remote_cluster, remote_fs = connect_to_filesystem(client_name, cluster_name, remote_fs_name,
                                                          'remote', conf_dct=remote_conf)
        try:
            local_cluster_id = self.rados.get_fsid()
            remote_cluster_id = remote_cluster.get_fsid()
            log.debug(f'local_cluster_id={local_cluster_id} remote_cluster_id={remote_cluster_id}')
            if 'fsid' in remote_conf:
                if not remote_cluster_id == remote_conf['fsid']:
                    raise MirrorException(-errno.EINVAL, 'FSID mismatch between bootstrap token and remote cluster')

            local_fscid = remote_fscid = None
            with open_filesystem(self.local_fs, local_fs_name) as local_fsh:
                local_fscid = local_fsh.get_fscid()
                remote_fscid = remote_fs.get_fscid()
                log.debug(f'local_fscid={local_fscid} remote_fscid={remote_fscid}')
                mi = None
                try:
                    mi = FSSnapshotMirror.get_mirror_info(local_fsh)
                except MirrorException as me:
                    if me.args[0] != -errno.ENODATA:
                        raise Exception(-errno.EINVAL)
                if mi and mi['cluster_id'] == remote_cluster_id and mi['fs_id'] == remote_fscid:
                    raise MirrorException(-errno.EINVAL, f'file system is an active peer for file system: {remote_fs_name}')

            if local_cluster_id == remote_cluster_id and local_fscid == remote_fscid:
                raise MirrorException(-errno.EINVAL, "'Source and destination cluster fsid and "\
                                      "file-system name can't be the same")
            FSSnapshotMirror.set_mirror_info(local_cluster_id, local_fscid, remote_fs)
        finally:
            disconnect_from_filesystem(cluster_name, remote_fs_name, remote_cluster, remote_fs)

    def init_pool_policy(self, filesystem):
        metadata_pool_id = FSSnapshotMirror.get_metadata_pool(filesystem, self.fs_map)
        if not metadata_pool_id:
            log.error(f'cannot find metadata pool-id for filesystem {filesystem}')
            raise Exception(-errno.EINVAL)
        try:
            ioctx = self.rados.open_ioctx2(metadata_pool_id)
            # TODO: make async if required
            dir_mapping = load_dir_map(ioctx)
            instances = load_instances(ioctx)
            # init policy
            fspolicy = FSPolicy(self.mgr, ioctx)
            log.debug(f'init policy for filesystem {filesystem}: pool-id {metadata_pool_id}')
            fspolicy.init(dir_mapping, instances)
            self.pool_policy[filesystem] = fspolicy
        except rados.Error as e:
            log.error(f'failed to access pool-id {metadata_pool_id} for filesystem {filesystem}: {e}')
            raise Exception(-e.errno)

    def refresh_pool_policy_locked(self):
        filesystems = self.get_mirrored_filesystems()
        log.debug(f'refreshing policy for {filesystems}')
        for filesystem in list(self.pool_policy):
            if not filesystem in filesystems:
                log.info(f'shutdown pool policy for {filesystem}')
                fspolicy = self.pool_policy.pop(filesystem)
                fspolicy.shutdown()
        for filesystem in filesystems:
            if not filesystem in self.pool_policy:
                log.info(f'init pool policy for {filesystem}')
                self.init_pool_policy(filesystem)

    def refresh_pool_policy(self):
        with self.lock:
            self.refresh_pool_policy_locked()

    def enable_mirror(self, filesystem):
        log.info(f'enabling mirror for filesystem {filesystem}')
        with self.lock:
            try:
                metadata_pool_id = FSSnapshotMirror.get_metadata_pool(filesystem, self.fs_map)
                if not metadata_pool_id:
                    log.error(f'cannot find metadata pool-id for filesystem {filesystem}')
                    raise Exception(-errno.EINVAL)
                create_mirror_object(self.rados, metadata_pool_id)
                cmd = {'prefix': 'fs mirror enable', 'fs_name': filesystem}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to enable mirror failed: {err}')
                    raise Exception(-errno.EINVAL)
                return 0, json.dumps({}), ''
            except MirrorException as me:
                return me.args[0], '', me.args[1]
            except Exception as me:
                return me.args[0], '', 'failed to enable mirroring'

    def disable_mirror(self, filesystem):
        log.info(f'disabling mirror for filesystem {filesystem}')
        try:
            with self.lock:
                cmd = {'prefix': 'fs mirror disable', 'fs_name': filesystem}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to disable mirror failed: {err}')
                    raise Exception(-errno.EINVAL)
                return 0, json.dumps({}), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
        except Exception as e:
            return e.args[0], '', 'failed to disable mirroring'

    def peer_list(self, filesystem):
        try:
            with self.lock:
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise MirrorException(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                peers = self.get_filesystem_peers(filesystem)
                peer_res = {}
                for peer_uuid, rem in peers.items():
                    remote = rem['remote']
                    peer_res[peer_uuid] = {'client_name': remote['client_name'],
                                           'site_name': remote['cluster_name'],
                                           'fs_name': remote['fs_name']
                                           }
                return 0, json.dumps(peer_res), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
        except Exception as e:
            return e.args[0], '', 'failed to list peers'

    def peer_add(self, filesystem, remote_cluster_spec, remote_fs_name, remote_conf):
        try:
            if remote_fs_name == None:
                remote_fs_name = filesystem
            with self.lock:
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise MirrorException(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                ### peer updates for key, site-name are not yet supported
                if self.peer_exists(filesystem, remote_cluster_spec, remote_fs_name):
                    return 0, json.dumps({}), ''
                # _own_ the peer
                self.verify_and_set_mirror_info(filesystem, remote_cluster_spec, remote_fs_name, remote_conf)
                # unique peer uuid
                peer_uuid = str(uuid.uuid4())
                config_key = FSSnapshotMirror.peer_config_key(filesystem, peer_uuid)
                if remote_conf.get('mon_host') and remote_conf.get('key'):
                    self.config_set(config_key, json.dumps(remote_conf))
                cmd = {'prefix': 'fs mirror peer_add',
                       'fs_name': filesystem,
                       'uuid': peer_uuid,
                       'remote_cluster_spec': remote_cluster_spec,
                       'remote_fs_name': remote_fs_name}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to add peer failed: {err}')
                    try:
                        log.debug(f'cleaning up config-key for {peer_uuid}')
                        self.config_set(config_key)
                    except:
                        pass
                    raise Exception(-errno.EINVAL)
                return 0, json.dumps({}), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
        except Exception as e:
            return e.args[0], '', 'failed to add peer'

    def peer_remove(self, filesystem, peer_uuid):
        try:
            with self.lock:
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise MirrorException(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                # ok, this is being a bit lazy. remove mirror info from peer followed
                # by purging the peer from fsmap. if the mirror daemon fs map updates
                # are laggy, they happily continue to synchronize. ideally, we should
                # purge the peer from fsmap here and purge mirror info on fsmap update
                # (in notify()). but thats not straightforward -- before purging mirror
                # info, we would need to wait for all mirror daemons to catch up with
                # fsmap updates. this involves mirror daemons sending the fsmap epoch
                # they have seen in reply to a notify request. TODO: fix this.
                self.purge_mirror_info(filesystem, peer_uuid)
                cmd = {'prefix': 'fs mirror peer_remove',
                       'fs_name': filesystem,
                       'uuid': peer_uuid}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to remove peer failed: {err}')
                    raise Exception(-errno.EINVAL)
                self.config_set(FSSnapshotMirror.peer_config_key(filesystem, peer_uuid))
                return 0, json.dumps({}), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
        except Exception as e:
            return e.args[0], '', 'failed to remove peer'

    def peer_bootstrap_create(self, fs_name, client_name, site_name):
        """create a bootstrap token for this peer filesystem"""
        try:
            with self.lock:
                cmd = {'prefix': 'fs authorize',
                       'filesystem': fs_name,
                       'entity': client_name,
                       'caps': ['/', 'rwps']}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to create peer user failed: {err}')
                    raise Exception(-errno.EINVAL)
                cmd = {'prefix': 'auth get',
                       'entity': client_name,
                       'format': 'json'}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to fetch keyring failed: {err}')
                    raise Exception(-errno.EINVAL)
                outs = json.loads(outs)
                outs0 = outs[0]
                token_dct = {'fsid': self.mgr.rados.get_fsid(),
                             'filesystem': fs_name,
                             'user': outs0['entity'],
                             'site_name': site_name,
                             'key': outs0['key'],
                             'mon_host': self.mgr.rados.conf_get('mon_host')}
                token_str = json.dumps(token_dct).encode('utf-8')
                encoded_token = base64.b64encode(token_str)
                return 0, json.dumps({'token': encoded_token.decode('utf-8')}), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
        except Exception as e:
            return e.args[0], '', 'failed to bootstrap peer'

    def peer_bootstrap_import(self, filesystem, token):
        try:
            token_str = base64.b64decode(token)
            token_dct = json.loads(token_str.decode('utf-8'))
        except:
            return -errno.EINVAL, '', 'failed to parse token'
        client_name = token_dct.pop('user')
        cluster_name = token_dct.pop('site_name')
        remote_fs_name = token_dct.pop('filesystem')
        remote_cluster_spec = f'{client_name}@{cluster_name}'
        return self.peer_add(filesystem, remote_cluster_spec, remote_fs_name, token_dct)

    @staticmethod
    def norm_path(dir_path):
        if not os.path.isabs(dir_path):
            raise MirrorException(-errno.EINVAL, f'{dir_path} should be an absolute path')
        return os.path.normpath(dir_path)

    def add_dir(self, filesystem, dir_path):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise MirrorException(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise MirrorException(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                dir_path = FSSnapshotMirror.norm_path(dir_path)
                log.debug(f'path normalized to {dir_path}')
                fspolicy.add_dir(dir_path)
                return 0, json.dumps({}), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
        except Exception as e:
            return e.args[0], '', 'failed to add directory'

    def remove_dir(self, filesystem, dir_path):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise MirrorException(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise MirrorException(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                dir_path = FSSnapshotMirror.norm_path(dir_path)
                fspolicy.remove_dir(dir_path)
                return 0, json.dumps({}), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
        except Exception as e:
            return e.args[0], '', 'failed to remove directory'

    def status(self,filesystem, dir_path):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise MirrorException(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise MirrorException(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                dir_path = FSSnapshotMirror.norm_path(dir_path)
                return fspolicy.status(dir_path)
        except MirrorException as me:
            return me.args[0], '', me.args[1]

    def show_distribution(self, filesystem):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise MirrorException(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise MirrorException(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                return fspolicy.summary()
        except MirrorException as me:
            return me.args[0], '', me.args[1]

    def daemon_status(self):
        try:
            with self.lock:
                daemons = []
                sm = self.mgr.get('service_map')
                daemon_entry = sm['services'].get('cephfs-mirror', None)
                log.debug(f'daemon_entry: {daemon_entry}')
                if daemon_entry is not None:
                    for daemon_key in daemon_entry.get('daemons', []):
                        try:
                            daemon_id = int(daemon_key)
                        except ValueError:
                            continue
                        daemon = {
                            'daemon_id'   : daemon_id,
                            'filesystems' : []
                        } # type: Dict[str, Any]
                        daemon_status = self.mgr.get_daemon_status('cephfs-mirror', daemon_key)
                        if not daemon_status:
                            log.debug(f'daemon status not yet available for cephfs-mirror daemon: {daemon_key}')
                            continue
                        status = json.loads(daemon_status['status_json'])
                        for fs_id, fs_desc in status.items():
                            fs = {'filesystem_id'   : int(fs_id),
                                'name'            : fs_desc['name'],
                                'directory_count' : fs_desc.get('directory_count', 0),
                                'peers'           : []
                            } # type: Dict[str, Any]
                            for peer_uuid, peer_desc in fs_desc['peers'].items():
                                peer = {
                                    'uuid'   : peer_uuid,
                                    'remote' : peer_desc['remote'],
                                    'stats'  : peer_desc['stats']
                                }
                                fs['peers'].append(peer)
                            daemon['filesystems'].append(fs)
                        daemons.append(daemon)
                return 0, json.dumps(daemons), ''
        except MirrorException as me:
            return me.args[0], '', me.args[1]
