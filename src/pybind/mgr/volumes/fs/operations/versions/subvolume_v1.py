import os
import sys
import stat
import uuid
import errno
import logging
import json
from datetime import datetime
from typing import List, Dict

import cephfs

from .metadata_manager import MetadataManager
from .subvolume_attrs import SubvolumeTypes, SubvolumeStates, SubvolumeFeatures
from .op_sm import SubvolumeOpSm
from .subvolume_base import SubvolumeBase
from ..template import SubvolumeTemplate
from ..snapshot_util import mksnap, rmsnap
from ..access import allow_access, deny_access
from ...exception import IndexException, OpSmException, VolumeException, MetadataMgrException, EvictionError
from ...fs_util import listsnaps, is_inherited_snap, create_base_dir
from ..template import SubvolumeOpType
from ..group import Group
from ..rankevicter import RankEvicter
from ..volume import get_mds_map

from ..clone_index import open_clone_index, create_clone_index

log = logging.getLogger(__name__)

class SubvolumeV1(SubvolumeBase, SubvolumeTemplate):
    """
    Version 1 subvolumes creates a subvolume with path as follows,
        volumes/<group-name>/<subvolume-name>/<uuid>/

    - The directory under which user data resides is <uuid>
    - Snapshots of the subvolume are taken within the <uuid> directory
    - A meta file is maintained under the <subvolume-name> directory as a metadata store, typically storing,
        - global information about the subvolume (version, path, type, state)
        - snapshots attached to an ongoing clone operation
        - clone snapshot source if subvolume is a clone of a snapshot
    - It retains backward compatability with legacy subvolumes by creating the meta file for legacy subvolumes under
    /volumes/_legacy/ (see legacy_config_path), thus allowing cloning of older legacy volumes that lack the <uuid>
    component in the path.
    """
    VERSION = 1

    @staticmethod
    def version():
        return SubvolumeV1.VERSION

    @property
    def path(self):
        try:
            # no need to stat the path -- open() does that
            return self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_PATH).encode('utf-8')
        except MetadataMgrException as me:
            raise VolumeException(-errno.EINVAL, "error fetching subvolume metadata")

    @property
    def features(self):
        return [SubvolumeFeatures.FEATURE_SNAPSHOT_CLONE.value, SubvolumeFeatures.FEATURE_SNAPSHOT_AUTOPROTECT.value]

    def mark_subvolume(self):
        # set subvolume attr, on subvolume root, marking it as a CephFS subvolume
        # subvolume root is where snapshots would be taken, and hence is the <uuid> dir for v1 subvolumes
        try:
            # MDS treats this as a noop for already marked subvolume
            self.fs.setxattr(self.path, 'ceph.dir.subvolume', b'1', 0)
        except cephfs.InvalidValue as e:
            raise VolumeException(-errno.EINVAL, "invalid value specified for ceph.dir.subvolume")
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def snapshot_base_path(self):
        """ Base path for all snapshots """
        return os.path.join(self.path, self.vol_spec.snapshot_dir_prefix.encode('utf-8'))

    def snapshot_path(self, snapname):
        """ Path to a specific snapshot named 'snapname' """
        return os.path.join(self.snapshot_base_path(), snapname.encode('utf-8'))

    def snapshot_data_path(self, snapname):
        """ Path to user data directory within a subvolume snapshot named 'snapname' """
        return self.snapshot_path(snapname)

    def create(self, size, isolate_nspace, pool, mode, uid, gid):
        subvolume_type = SubvolumeTypes.TYPE_NORMAL
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException as oe:
            raise VolumeException(-errno.EINVAL, "subvolume creation failed: internal error")

        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            # create group directory with default mode(0o755) if it doesn't exist.
            create_base_dir(self.fs, self.group.path, self.vol_spec.DEFAULT_MODE)
            # create directory and set attributes
            self.fs.mkdirs(subvol_path, mode)
            self.mark_subvolume()
            attrs = {
                'uid': uid,
                'gid': gid,
                'data_pool': pool,
                'pool_namespace': self.namespace if isolate_nspace else None,
                'quota': size
            }
            self.set_attrs(subvol_path, attrs)

            # persist subvolume metadata
            qpath = subvol_path.decode('utf-8')
            self.init_config(SubvolumeV1.VERSION, subvolume_type, qpath, initial_state)
        except (VolumeException, MetadataMgrException, cephfs.Error) as e:
            try:
                log.info("cleaning up subvolume with path: {0}".format(self.subvolname))
                self.remove()
            except VolumeException as ve:
                log.info("failed to cleanup subvolume '{0}' ({1})".format(self.subvolname, ve))

            if isinstance(e, MetadataMgrException):
                log.error("metadata manager exception: {0}".format(e))
                e = VolumeException(-errno.EINVAL, "exception in subvolume metadata")
            elif isinstance(e, cephfs.Error):
                e = VolumeException(-e.args[0], e.args[1])
            raise e

    def add_clone_source(self, volname, subvolume, snapname, flush=False):
        self.metadata_mgr.add_section("source")
        self.metadata_mgr.update_section("source", "volume", volname)
        if not subvolume.group.is_default_group():
            self.metadata_mgr.update_section("source", "group", subvolume.group_name)
        self.metadata_mgr.update_section("source", "subvolume", subvolume.subvol_name)
        self.metadata_mgr.update_section("source", "snapshot", snapname)
        if flush:
            self.metadata_mgr.flush()

    def remove_clone_source(self, flush=False):
        self.metadata_mgr.remove_section("source")
        if flush:
            self.metadata_mgr.flush()

    def create_clone(self, pool, source_volname, source_subvolume, snapname):
        subvolume_type = SubvolumeTypes.TYPE_CLONE
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException as oe:
            raise VolumeException(-errno.EINVAL, "clone failed: internal error")

        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            # source snapshot attrs are used to create clone subvolume.
            # attributes of subvolume's content though, are synced during the cloning process.
            attrs = source_subvolume.get_attrs(source_subvolume.snapshot_data_path(snapname))

            # The source of the clone may have exceeded its quota limit as
            # CephFS quotas are imprecise. Cloning such a source may fail if
            # the quota on the destination is set before starting the clone
            # copy. So always set the quota on destination after cloning is
            # successful.
            attrs["quota"] = None

            # override snapshot pool setting, if one is provided for the clone
            if pool is not None:
                attrs["data_pool"] = pool
                attrs["pool_namespace"] = None

            # create directory and set attributes
            self.fs.mkdirs(subvol_path, attrs.get("mode"))
            self.mark_subvolume()
            self.set_attrs(subvol_path, attrs)

            # persist subvolume metadata and clone source
            qpath = subvol_path.decode('utf-8')
            self.metadata_mgr.init(SubvolumeV1.VERSION, subvolume_type.value, qpath, initial_state.value)
            self.add_clone_source(source_volname, source_subvolume, snapname)
            self.metadata_mgr.flush()
        except (VolumeException, MetadataMgrException, cephfs.Error) as e:
            try:
                log.info("cleaning up subvolume with path: {0}".format(self.subvolname))
                self.remove()
            except VolumeException as ve:
                log.info("failed to cleanup subvolume '{0}' ({1})".format(self.subvolname, ve))

            if isinstance(e, MetadataMgrException):
                log.error("metadata manager exception: {0}".format(e))
                e = VolumeException(-errno.EINVAL, "exception in subvolume metadata")
            elif isinstance(e, cephfs.Error):
                e = VolumeException(-e.args[0], e.args[1])
            raise e

    def allowed_ops_by_type(self, vol_type):
        if vol_type == SubvolumeTypes.TYPE_CLONE:
            return {op_type for op_type in SubvolumeOpType}

        if vol_type == SubvolumeTypes.TYPE_NORMAL:
            return {op_type for op_type in SubvolumeOpType} - {SubvolumeOpType.CLONE_STATUS,
                                                               SubvolumeOpType.CLONE_CANCEL,
                                                               SubvolumeOpType.CLONE_INTERNAL}

        return {}

    def allowed_ops_by_state(self, vol_state):
        if vol_state == SubvolumeStates.STATE_COMPLETE:
            return {op_type for op_type in SubvolumeOpType}

        return {SubvolumeOpType.REMOVE_FORCE,
                SubvolumeOpType.CLONE_CREATE,
                SubvolumeOpType.CLONE_STATUS,
                SubvolumeOpType.CLONE_CANCEL,
                SubvolumeOpType.CLONE_INTERNAL}

    def open(self, op_type):
        if not isinstance(op_type, SubvolumeOpType):
            raise VolumeException(-errno.ENOTSUP, "operation {0} not supported on subvolume '{1}'".format(
                                  op_type.value, self.subvolname))
        try:
            self.metadata_mgr.refresh()

            etype = self.subvol_type
            if op_type not in self.allowed_ops_by_type(etype):
                raise VolumeException(-errno.ENOTSUP, "operation '{0}' is not allowed on subvolume '{1}' of type {2}".format(
                                      op_type.value, self.subvolname, etype.value))

            estate = self.state
            if op_type not in self.allowed_ops_by_state(estate):
                raise VolumeException(-errno.EAGAIN, "subvolume '{0}' is not ready for operation {1}".format(
                                      self.subvolname, op_type.value))

            subvol_path = self.path
            log.debug("refreshed metadata, checking subvolume path '{0}'".format(subvol_path))
            st = self.fs.stat(subvol_path)
            # unconditionally mark as subvolume, to handle pre-existing subvolumes without the mark
            self.mark_subvolume()

            self.uid = int(st.st_uid)
            self.gid = int(st.st_gid)
            self.mode = int(st.st_mode & ~stat.S_IFMT(st.st_mode))
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT:
                raise VolumeException(-errno.ENOENT, "subvolume '{0}' does not exist".format(self.subvolname))
            raise VolumeException(me.args[0], me.args[1])
        except cephfs.ObjectNotFound:
            log.debug("missing subvolume path '{0}' for subvolume '{1}'".format(subvol_path, self.subvolname))
            raise VolumeException(-errno.ENOENT, "mount path missing for subvolume '{0}'".format(self.subvolname))
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def _recover_auth_meta(self, auth_id, auth_meta):
        """
        Call me after locking the auth meta file.
        """
        remove_subvolumes = []

        for subvol, subvol_data in auth_meta['subvolumes'].items():
            if not subvol_data['dirty']:
                continue

            (group_name, subvol_name) = subvol.split('/')
            group_name = group_name if group_name != 'None' else Group.NO_GROUP_NAME
            access_level = subvol_data['access_level']

            with self.auth_mdata_mgr.subvol_metadata_lock(group_name, subvol_name):
                subvol_meta = self.auth_mdata_mgr.subvol_metadata_get(group_name, subvol_name)

                # No SVMeta update indicates that there was no auth update
                # in Ceph either. So it's safe to remove corresponding
                # partial update in AMeta.
                if not subvol_meta or auth_id not in subvol_meta['auths']:
                    remove_subvolumes.append(subvol)
                    continue

                want_auth = {
                    'access_level': access_level,
                    'dirty': False,
                }
                # SVMeta update looks clean. Ceph auth update must have been
                # clean. Update the dirty flag and continue
                if subvol_meta['auths'][auth_id] == want_auth:
                    auth_meta['subvolumes'][subvol]['dirty'] = False
                    self.auth_mdata_mgr.auth_metadata_set(auth_id, auth_meta)
                    continue

                client_entity = "client.{0}".format(auth_id)
                ret, out, err = self.mgr.mon_command(
                    {
                        'prefix': 'auth get',
                        'entity': client_entity,
                        'format': 'json'
                    })
                if ret == 0:
                    existing_caps = json.loads(out)
                elif ret == -errno.ENOENT:
                    existing_caps = None
                else:
                    log.error(err)
                    raise VolumeException(ret, err)

                self._authorize_subvolume(auth_id, access_level, existing_caps)

            # Recovered from partial auth updates for the auth ID's access
            # to a subvolume.
            auth_meta['subvolumes'][subvol]['dirty'] = False
            self.auth_mdata_mgr.auth_metadata_set(auth_id, auth_meta)

        for subvol in remove_subvolumes:
            del auth_meta['subvolumes'][subvol]

        if not auth_meta['subvolumes']:
            # Clean up auth meta file
            self.fs.unlink(self.auth_mdata_mgr._auth_metadata_path(auth_id))
            return

        # Recovered from all partial auth updates for the auth ID.
        auth_meta['dirty'] = False
        self.auth_mdata_mgr.auth_metadata_set(auth_id, auth_meta)

    def authorize(self, auth_id, access_level, tenant_id=None, allow_existing_id=False):
        """
        Get-or-create a Ceph auth identity for `auth_id` and grant them access
        to
        :param auth_id:
        :param access_level:
        :param tenant_id: Optionally provide a stringizable object to
                          restrict any created cephx IDs to other callers
                          passing the same tenant ID.
        :allow_existing_id: Optionally authorize existing auth-ids not
                          created by ceph_volume_client.
        :return:
        """

        with self.auth_mdata_mgr.auth_lock(auth_id):
            client_entity = "client.{0}".format(auth_id)
            ret, out, err = self.mgr.mon_command(
                {
                    'prefix': 'auth get',
                    'entity': client_entity,
                    'format': 'json'
                })

            if ret == 0:
                existing_caps = json.loads(out)
            elif ret == -errno.ENOENT:
                existing_caps = None
            else:
                log.error(err)
                raise VolumeException(ret, err)

            # Existing meta, or None, to be updated
            auth_meta = self.auth_mdata_mgr.auth_metadata_get(auth_id)

            # subvolume data to be inserted
            group_name = self.group.groupname if self.group.groupname != Group.NO_GROUP_NAME else None
            group_subvol_id = "{0}/{1}".format(group_name, self.subvolname)
            subvolume = {
                group_subvol_id : {
                    # The access level at which the auth_id is authorized to
                    # access the volume.
                    'access_level': access_level,
                    'dirty': True,
                }
            }

            if auth_meta is None:
                if not allow_existing_id and existing_caps is not None:
                    msg = "auth ID: {0} exists and not created by mgr plugin. Not allowed to modify".format(auth_id)
                    log.error(msg)
                    raise VolumeException(-errno.EPERM, msg)

                # non-existent auth IDs
                sys.stderr.write("Creating meta for ID {0} with tenant {1}\n".format(
                    auth_id, tenant_id
                ))
                log.debug("Authorize: no existing meta")
                auth_meta = {
                    'dirty': True,
                    'tenant_id': str(tenant_id) if tenant_id else None,
                    'subvolumes': subvolume
                }
            else:
                # Update 'volumes' key (old style auth metadata file) to 'subvolumes' key
                if 'volumes' in auth_meta:
                    auth_meta['subvolumes'] = auth_meta.pop('volumes')

                # Disallow tenants to share auth IDs
                if str(auth_meta['tenant_id']) != str(tenant_id):
                    msg = "auth ID: {0} is already in use".format(auth_id)
                    log.error(msg)
                    raise VolumeException(-errno.EPERM, msg)

                if auth_meta['dirty']:
                    self._recover_auth_meta(auth_id, auth_meta)

                log.debug("Authorize: existing tenant {tenant}".format(
                    tenant=auth_meta['tenant_id']
                ))
                auth_meta['dirty'] = True
                auth_meta['subvolumes'].update(subvolume)

            self.auth_mdata_mgr.auth_metadata_set(auth_id, auth_meta)

            with self.auth_mdata_mgr.subvol_metadata_lock(self.group.groupname, self.subvolname):
                key = self._authorize_subvolume(auth_id, access_level, existing_caps)

            auth_meta['dirty'] = False
            auth_meta['subvolumes'][group_subvol_id]['dirty'] = False
            self.auth_mdata_mgr.auth_metadata_set(auth_id, auth_meta)

            if tenant_id:
                return key
            else:
                # Caller wasn't multi-tenant aware: be safe and don't give
                # them a key
                return ""

    def _authorize_subvolume(self, auth_id, access_level, existing_caps):
        subvol_meta = self.auth_mdata_mgr.subvol_metadata_get(self.group.groupname, self.subvolname)

        auth = {
            auth_id: {
                'access_level': access_level,
                'dirty': True,
            }
        }

        if subvol_meta is None:
            subvol_meta = {
                'auths': auth
            }
        else:
            subvol_meta['auths'].update(auth)
            self.auth_mdata_mgr.subvol_metadata_set(self.group.groupname, self.subvolname, subvol_meta)

        key = self._authorize(auth_id, access_level, existing_caps)

        subvol_meta['auths'][auth_id]['dirty'] = False
        self.auth_mdata_mgr.subvol_metadata_set(self.group.groupname, self.subvolname, subvol_meta)

        return key

    def _authorize(self, auth_id, access_level, existing_caps):
        subvol_path = self.path
        log.debug("Authorizing Ceph id '{0}' for path '{1}'".format(auth_id, subvol_path))

        # First I need to work out what the data pool is for this share:
        # read the layout
        try:
            pool = self.fs.getxattr(subvol_path, 'ceph.dir.layout.pool').decode('utf-8')
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

        try:
            namespace = self.fs.getxattr(subvol_path, 'ceph.dir.layout.pool_namespace').decode('utf-8')
        except cephfs.NoData:
            namespace = None

        # Now construct auth capabilities that give the guest just enough
        # permissions to access the share
        client_entity = "client.{0}".format(auth_id)
        want_mds_cap = "allow {0} path={1}".format(access_level, subvol_path.decode('utf-8'))
        want_osd_cap = "allow {0} pool={1}{2}".format(
                access_level, pool, " namespace={0}".format(namespace) if namespace else "")

        # Construct auth caps that if present might conflict with the desired
        # auth caps.
        unwanted_access_level = 'r' if access_level == 'rw' else 'rw'
        unwanted_mds_cap = 'allow {0} path={1}'.format(unwanted_access_level, subvol_path.decode('utf-8'))
        unwanted_osd_cap = "allow {0} pool={1}{2}".format(
                unwanted_access_level, pool, " namespace={0}".format(namespace) if namespace else "")

        return allow_access(self.mgr, client_entity, want_mds_cap, want_osd_cap,
                            unwanted_mds_cap, unwanted_osd_cap, existing_caps)

    def deauthorize(self, auth_id):
        with self.auth_mdata_mgr.auth_lock(auth_id):
            # Existing meta, or None, to be updated
            auth_meta = self.auth_mdata_mgr.auth_metadata_get(auth_id)

            if auth_meta is None:
                msg = "auth ID: {0} doesn't exist".format(auth_id)
                log.error(msg)
                raise VolumeException(-errno.ENOENT, msg)

            # Update 'volumes' key (old style auth metadata file) to 'subvolumes' key
            if 'volumes' in auth_meta:
                auth_meta['subvolumes'] = auth_meta.pop('volumes')

            group_name = self.group.groupname if self.group.groupname != Group.NO_GROUP_NAME else None
            group_subvol_id = "{0}/{1}".format(group_name, self.subvolname)
            if (auth_meta is None) or (not auth_meta['subvolumes']):
                log.warning("deauthorized called for already-removed auth"
                         "ID '{auth_id}' for subvolume '{subvolume}'".format(
                    auth_id=auth_id, subvolume=self.subvolname
                ))
                # Clean up the auth meta file of an auth ID
                self.fs.unlink(self.auth_mdata_mgr._auth_metadata_path(auth_id))
                return

            if group_subvol_id not in auth_meta['subvolumes']:
                log.warning("deauthorized called for already-removed auth"
                         "ID '{auth_id}' for subvolume '{subvolume}'".format(
                    auth_id=auth_id, subvolume=self.subvolname
                ))
                return

            if auth_meta['dirty']:
                self._recover_auth_meta(auth_id, auth_meta)

            auth_meta['dirty'] = True
            auth_meta['subvolumes'][group_subvol_id]['dirty'] = True
            self.auth_mdata_mgr.auth_metadata_set(auth_id, auth_meta)

            self._deauthorize_subvolume(auth_id)

            # Filter out the volume we're deauthorizing
            del auth_meta['subvolumes'][group_subvol_id]

            # Clean up auth meta file
            if not auth_meta['subvolumes']:
                self.fs.unlink(self.auth_mdata_mgr._auth_metadata_path(auth_id))
                return

            auth_meta['dirty'] = False
            self.auth_mdata_mgr.auth_metadata_set(auth_id, auth_meta)

    def _deauthorize_subvolume(self, auth_id):
        with self.auth_mdata_mgr.subvol_metadata_lock(self.group.groupname, self.subvolname):
            subvol_meta = self.auth_mdata_mgr.subvol_metadata_get(self.group.groupname, self.subvolname)

            if (subvol_meta is None) or (auth_id not in subvol_meta['auths']):
                log.warning("deauthorized called for already-removed auth"
                         "ID '{auth_id}' for subvolume '{subvolume}'".format(
                    auth_id=auth_id, subvolume=self.subvolname
                ))
                return

            subvol_meta['auths'][auth_id]['dirty'] = True
            self.auth_mdata_mgr.subvol_metadata_set(self.group.groupname, self.subvolname, subvol_meta)

            self._deauthorize(auth_id)

            # Remove the auth_id from the metadata *after* removing it
            # from ceph, so that if we crashed here, we would actually
            # recreate the auth ID during recovery (i.e. end up with
            # a consistent state).

            # Filter out the auth we're removing
            del subvol_meta['auths'][auth_id]
            self.auth_mdata_mgr.subvol_metadata_set(self.group.groupname, self.subvolname, subvol_meta)

    def _deauthorize(self, auth_id):
        """
        The volume must still exist.
        """
        client_entity = "client.{0}".format(auth_id)
        subvol_path = self.path
        try:
            pool_name = self.fs.getxattr(subvol_path, 'ceph.dir.layout.pool').decode('utf-8')
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

        try:
            namespace = self.fs.getxattr(subvol_path, 'ceph.dir.layout.pool_namespace').decode('utf-8')
        except cephfs.NoData:
            namespace = None

        # The auth_id might have read-only or read-write mount access for the
        # subvolume path.
        access_levels = ('r', 'rw')
        want_mds_caps = ['allow {0} path={1}'.format(access_level, subvol_path.decode('utf-8'))
                         for access_level in access_levels]
        want_osd_caps = ['allow {0} pool={1}{2}'.format(
                          access_level, pool_name, " namespace={0}".format(namespace) if namespace else "")
                         for access_level in access_levels]
        deny_access(self.mgr, client_entity, want_mds_caps, want_osd_caps)

    def authorized_list(self):
        """
        Expose a list of auth IDs that have access to a subvolume.

        return: a list of (auth_id, access_level) tuples, where
                the access_level can be 'r' , or 'rw'.
                None if no auth ID is given access to the subvolume.
        """
        with self.auth_mdata_mgr.subvol_metadata_lock(self.group.groupname, self.subvolname):
            meta = self.auth_mdata_mgr.subvol_metadata_get(self.group.groupname, self.subvolname)
            auths = [] # type: List[Dict[str,str]]
            if not meta or not meta['auths']:
                return auths

            for auth, auth_data in meta['auths'].items():
                # Skip partial auth updates.
                if not auth_data['dirty']:
                    auths.append({auth: auth_data['access_level']})

            return auths

    def evict(self, volname, auth_id, timeout=30):
        """
        Evict all clients based on the authorization ID and the subvolume path mounted.
        Assumes that the authorization key has been revoked prior to calling this function.

        This operation can throw an exception if the mon cluster is unresponsive, or
        any individual MDS daemon is unresponsive for longer than the timeout passed in.
        """

        client_spec = ["auth_name={0}".format(auth_id), ]
        client_spec.append("client_metadata.root={0}".
                           format(self.path.decode('utf-8')))

        log.info("evict clients with {0}".format(', '.join(client_spec)))

        mds_map = get_mds_map(self.mgr, volname)
        if not mds_map:
            raise VolumeException(-errno.ENOENT, "mdsmap for volume {0} not found".format(volname))

        up = {}
        for name, gid in mds_map['up'].items():
            # Quirk of the MDSMap JSON dump: keys in the up dict are like "mds_0"
            assert name.startswith("mds_")
            up[int(name[4:])] = gid

        # For all MDS ranks held by a daemon
        # Do the parallelism in python instead of using "tell mds.*", because
        # the latter doesn't give us per-mds output
        threads = []
        for rank, gid in up.items():
            thread = RankEvicter(self.mgr, self.fs, client_spec, volname, rank, gid, mds_map, timeout)
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

        log.info("evict: joined all")

        for t in threads:
            if not t.success:
                msg = ("Failed to evict client with {0} from mds {1}/{2}: {3}".
                       format(', '.join(client_spec), t.rank, t.gid, t.exception)
                      )
                log.error(msg)
                raise EvictionError(msg)

    def _get_clone_source(self):
        try:
            clone_source = {
                'volume'   : self.metadata_mgr.get_option("source", "volume"),
                'subvolume': self.metadata_mgr.get_option("source", "subvolume"),
                'snapshot' : self.metadata_mgr.get_option("source", "snapshot"),
            }

            try:
                clone_source["group"] = self.metadata_mgr.get_option("source", "group")
            except MetadataMgrException as me:
                if me.errno == -errno.ENOENT:
                    pass
                else:
                    raise
        except MetadataMgrException as me:
            raise VolumeException(-errno.EINVAL, "error fetching subvolume metadata")
        return clone_source

    @property
    def status(self):
        state = SubvolumeStates.from_value(self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_STATE))
        subvolume_type = self.subvol_type
        subvolume_status = {
            'state' : state.value
        }
        if not SubvolumeOpSm.is_complete_state(state) and subvolume_type == SubvolumeTypes.TYPE_CLONE:
            subvolume_status["source"] = self._get_clone_source()
        return subvolume_status

    @property
    def state(self):
        return SubvolumeStates.from_value(self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_STATE))

    @state.setter
    def state(self, val):
        state = val[0].value
        flush = val[1]
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_STATE, state)
        if flush:
            self.metadata_mgr.flush()

    def remove(self, retainsnaps=False):
        if retainsnaps:
            raise VolumeException(-errno.EINVAL, "subvolume '{0}' does not support snapshot retention on delete".format(self.subvolname))
        if self.list_snapshots():
            raise VolumeException(-errno.ENOTEMPTY, "subvolume '{0}' has snapshots".format(self.subvolname))
        self.trash_base_dir()

    def resize(self, newsize, noshrink):
        subvol_path = self.path
        return self._resize(subvol_path, newsize, noshrink)

    def create_snapshot(self, snapname):
        try:
            group_snapshot_path = os.path.join(self.group.path,
                                               self.vol_spec.snapshot_dir_prefix.encode('utf-8'),
                                               snapname.encode('utf-8'))
            self.fs.stat(group_snapshot_path)
        except cephfs.Error as e:
            if e.args[0] == errno.ENOENT:
                snappath = self.snapshot_path(snapname)
                mksnap(self.fs, snappath)
            else:
                raise VolumeException(-e.args[0], e.args[1])
        else:
            raise VolumeException(-errno.EINVAL, "subvolumegroup and subvolume snapshot name can't be same")

    def has_pending_clones(self, snapname):
        try:
            return self.metadata_mgr.section_has_item('clone snaps', snapname)
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT:
                return False
            raise

    def remove_snapshot(self, snapname):
        if self.has_pending_clones(snapname):
            raise VolumeException(-errno.EAGAIN, "snapshot '{0}' has pending clones".format(snapname))
        snappath = self.snapshot_path(snapname)
        rmsnap(self.fs, snappath)

    def snapshot_info(self, snapname):
        if is_inherited_snap(snapname):
            raise VolumeException(-errno.EINVAL,
                                  "snapshot name '{0}' is invalid".format(snapname))
        snappath = self.snapshot_data_path(snapname)
        snap_info = {}
        try:
            snap_attrs = {'created_at':'ceph.snap.btime', 'size':'ceph.dir.rbytes',
                          'data_pool':'ceph.dir.layout.pool'}
            for key, val in snap_attrs.items():
                snap_info[key] = self.fs.getxattr(snappath, val)
            return {'size': int(snap_info['size']),
                    'created_at': str(datetime.fromtimestamp(float(snap_info['created_at']))),
                    'data_pool': snap_info['data_pool'].decode('utf-8'),
                    'has_pending_clones': "yes" if self.has_pending_clones(snapname) else "no"}
        except cephfs.Error as e:
            if e.errno == errno.ENOENT:
                raise VolumeException(-errno.ENOENT,
                                      "snapshot '{0}' does not exist".format(snapname))
            raise VolumeException(-e.args[0], e.args[1])

    def list_snapshots(self):
        try:
            dirpath = self.snapshot_base_path()
            return listsnaps(self.fs, self.vol_spec, dirpath, filter_inherited_snaps=True)
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                return []
            raise

    def _add_snap_clone(self, track_id, snapname):
        self.metadata_mgr.add_section("clone snaps")
        self.metadata_mgr.update_section("clone snaps", track_id, snapname)
        self.metadata_mgr.flush()

    def _remove_snap_clone(self, track_id):
        self.metadata_mgr.remove_option("clone snaps", track_id)
        self.metadata_mgr.flush()

    def attach_snapshot(self, snapname, tgt_subvolume):
        if not snapname.encode('utf-8') in self.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
        try:
            create_clone_index(self.fs, self.vol_spec)
            with open_clone_index(self.fs, self.vol_spec) as index:
                track_idx = index.track(tgt_subvolume.base_path)
                self._add_snap_clone(track_idx, snapname)
        except (IndexException, MetadataMgrException) as e:
            log.warning("error creating clone index: {0}".format(e))
            raise VolumeException(-errno.EINVAL, "error cloning subvolume")

    def detach_snapshot(self, snapname, track_id):
        if not snapname.encode('utf-8') in self.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
        try:
            with open_clone_index(self.fs, self.vol_spec) as index:
                index.untrack(track_id)
                self._remove_snap_clone(track_id)
        except (IndexException, MetadataMgrException) as e:
            log.warning("error delining snapshot from clone: {0}".format(e))
            raise VolumeException(-errno.EINVAL, "error delinking snapshot from clone")
