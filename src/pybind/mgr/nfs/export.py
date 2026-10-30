import errno
import hashlib
import json
import logging
import threading
from contextlib import contextmanager
from typing import (
    List,
    Any,
    Dict,
    Iterator,
    Optional,
    TYPE_CHECKING,
    TypeVar,
    Callable,
    Set,
    Tuple,
    cast)
from os.path import normpath
from ceph.fs.earmarking import EarmarkTopScope
from ceph.fs.enctag import CephFSVolumeEncryptionTag, EncryptionTagException
import cephfs

from mgr_util import CephFSEarmarkResolver, open_filesystem
from rados import ObjectNotFound

from object_format import ErrorResponse
from mgr_module import NFS_POOL_NAME as POOL_NAME, NFS_GANESHA_SUPPORTED_FSALS

from .ganesha_conf import (
    CephFSFSAL,
    CephUsers,
    CephUser,
    Export,
    GaneshaConfParser,
    RGWFSAL,
    format_block)
from .qos_conf import QOS, QOSBandwidthControl, QOSOpsControl, QOSParams
from .export_utils import (
    export_qos_bw_checks,
    export_dict_qos_bw_ops_checks,
    export_qos_ops_checks,
    get_cluster_qos_config)
from .exception import NFSException, NFSInvalidOperation, FSNotFound, NFSObjectNotFound
from .utils import (
    EXPORT_PREFIX,
    NonFatalError,
    export_obj_name,
    conf_obj_name,
    ceph_users_obj_name,
    available_clusters,
    check_fs,
    get_nfs_spec_for_cluster,
    restart_nfs_service,
    cephfs_client_for_mgr,
    cephfs_path_is_dir,
    normalize_auth_entity)
from .rados_utils import NFSRados

if TYPE_CHECKING:
    from nfs.module import Module

FuncT = TypeVar('FuncT', bound=Callable)

log = logging.getLogger(__name__)


def known_cluster_ids(mgr: 'Module') -> Set[str]:
    """Return the set of known cluster IDs."""
    return set(available_clusters(mgr))


def is_root_cmount(cmount_path: Optional[str]) -> bool:
    """An unset cmount_path means the default of /, which is what pools use."""
    return not cmount_path or cmount_path == "/"


def normalize_path(path: str) -> str:
    if path:
        path = normpath(path.strip())
        if path[:2] == "//":
            path = path[1:]
    return path


def validate_cephfs_path(mgr: 'Module', fs_name: str, path: str) -> None:
    try:
        cephfs_path_is_dir(mgr, fs_name, path)
    except NotADirectoryError:
        raise NFSException(f"path {path} is not a dir", -errno.ENOTDIR)
    except cephfs.ObjectNotFound:
        raise NFSObjectNotFound(f"path {path} does not exist")
    except cephfs.Error as e:
        raise NFSException(e.args[1], -e.args[0])


def get_enctag_from_path(mgr: 'Module', fs_name: str, path: str) -> Optional[str]:
    """
    Retrieve the encryption tag (enctag) from a CephFS path.
    """
    try:
        cephfs_client = cephfs_client_for_mgr(mgr)
        with open_filesystem(cephfs_client, fs_name) as fs_handle:
            fs_enctag = CephFSVolumeEncryptionTag(fs_handle, path)
            enctag = fs_enctag.get_tag()
            return enctag if enctag else ""
    except EncryptionTagException as e:
        log.warning("Failed to get enctag for path %s: %s", path, e)
        if e.errno == -errno.ENODATA:
            return ""
        raise NFSException(f"Failed to get enctag for path {path}: {e.error_str}", e.errno)
    except Exception as e:
        log.exception("Unexpected error getting enctag for path %s: %s", path, e)
        raise NFSException(f"Failed to get enctag for path {path}: {e}", -errno.EIO)


def _validate_cmount_path(cmount_path: str, path: str) -> None:
    if cmount_path not in path:
        raise ValueError(
            f"Invalid cmount_path: '{cmount_path}'. The path '{path}' is not within the mount path. "
            f"Please ensure that the cmount_path includes the specified path '{path}'. "
            "It is allowed to be any complete path hierarchy between / and the EXPORT {path}."
        )


class AppliedExportResults:
    """Gathers the results of multiple changed exports.
    Returned by apply_export.
    """

    def __init__(self) -> None:
        self.changes: List[Dict[str, str]] = []
        self.has_error = False
        self.exceptions: List[Exception] = []
        self.faulty_export_block_indices = ""
        self.num_errors = 0
        self.status = ""

    def append(self, value: Dict[str, Any]) -> None:
        if value.get("state", "") == "error":
            self.num_errors += 1
            # If there is an error then there must be an exception in the dict.
            self.exceptions.append(value.pop("exception"))
            # Index is for indicating at which export block in the conf/json
            # file did the export creation/update failed.
            if len(self.faulty_export_block_indices) == 0:
                self.faulty_export_block_indices = str(value.pop("index"))
            else:
                self.faulty_export_block_indices += f", {value.pop('index')}"
            self.has_error = True
        self.changes.append(value)

    def to_simplified(self) -> List[Dict[str, str]]:
        return self.changes

    def mgr_return_value(self) -> int:
        if self.has_error:
            if len(self.exceptions) == 1:
                ex = self.exceptions[0]
                if isinstance(ex, NFSException):
                    return ex.errno
                # Some non-nfs exception occurred, this can be anything
                # therefore return EAGAIN as a generalised errno.
                return -errno.EAGAIN
            # There are multiple failures so returning EIO as a generalised
            # errno.
            return -errno.EIO
        return 0

    def mgr_status_value(self) -> str:
        if self.has_error:
            if len(self.faulty_export_block_indices) == 1:
                self.status = f"{str(self.exceptions[0])} for export block" \
                              f" at index {self.faulty_export_block_indices}"
            elif len(self.faulty_export_block_indices) > 1:
                self.status = f"{self.num_errors} export blocks (at index" \
                              f" {self.faulty_export_block_indices}) failed" \
                              " to be created/updated"
        return self.status


class ExportMgr:
    def __init__(
            self,
            mgr: 'Module',
            export_ls: Optional[Dict[str, List[Export]]] = None
    ) -> None:
        self.mgr = mgr
        self.rados_pool = POOL_NAME
        self._exports: Optional[Dict[str, List[Export]]] = export_ls
        self.skip_notify_nfs_server = False
        # One lock per cluster, created on demand, see _ceph_users_locked().
        self._ceph_users_locks: Dict[str, Any] = {}
        self._ceph_users_locks_guard = threading.Lock()

    def _get_cluster_protocols(self, cluster_id: str) -> List[int]:
        """Get the list of supported NFS protocols for a cluster.
        """
        try:
            import orchestrator
            from ceph.deployment.service_spec import NFSServiceSpec

            completion = self.mgr.describe_service(service_type='nfs', service_name=f'nfs.{cluster_id}')
            services = orchestrator.raise_if_exception(completion)
            for service in services:
                if service.spec:
                    spec = cast(NFSServiceSpec, service.spec)
                    if getattr(spec, 'enable_nfsv3', False):
                        return [3, 4]
                    return [4]
        except Exception as e:
            log.debug(f"Failed to get cluster protocols for {cluster_id}: {e}, defaulting to v4 only")
        return [4]

    @property
    def exports(self) -> Dict[str, List[Export]]:
        if self._exports is None:
            self._exports = {}
            log.info("Begin export parsing")
            for cluster_id in known_cluster_ids(self.mgr):
                self._exports[cluster_id] = self._read_raw_config(cluster_id)
                log.info("Exports parsed successfully %s", self._exports.items())
        return self._exports

    def _fetch_export(
            self,
            cluster_id: str,
            pseudo_path: str
    ) -> Optional[Export]:
        try:
            for ex in self.exports[cluster_id]:
                if ex.pseudo == pseudo_path:
                    return ex
            return None
        except KeyError:
            log.info('no exports for cluster %s', cluster_id)
            return None

    def _fetch_export_id(
            self,
            cluster_id: str,
            export_id: int
    ) -> Optional[Export]:
        try:
            for ex in self.exports[cluster_id]:
                if ex.export_id == export_id:
                    return ex
            return None
        except KeyError:
            log.info(f'no exports for cluster {cluster_id}')
            return None

    def _delete_export_user(self, export: Export) -> None:
        if isinstance(export.fsal, CephFSFSAL):
            assert export.fsal.user_id
            self.mgr.check_mon_command({
                'prefix': 'auth rm',
                'entity': 'client.{}'.format(export.fsal.user_id),
            })
            log.info("Deleted export user %s", export.fsal.user_id)
        elif isinstance(export.fsal, RGWFSAL):
            # do nothing; we're using the bucket owner creds.
            pass

    def _create_rgw_export_user(self, export: Export) -> None:
        rgwfsal = cast(RGWFSAL, export.fsal)
        if not rgwfsal.user_id:
            assert export.path
            ret, out, err = self.mgr.tool_exec(
                ['radosgw-admin', 'bucket', 'stats', '--bucket', export.path]
            )
            if ret:
                raise NFSException(f'Failed to fetch owner for bucket {export.path}')
            j = json.loads(out)
            owner = j.get('owner', '')
            rgwfsal.user_id = owner
        assert rgwfsal.user_id
        ret, out, err = self.mgr.tool_exec([
            'radosgw-admin', 'user', 'info', '--uid', rgwfsal.user_id
        ])
        if ret:
            raise NFSException(
                f'Failed to fetch key for bucket {export.path} owner {rgwfsal.user_id}'
            )
        j = json.loads(out)

        # FIXME: make this more tolerate of unexpected output?
        rgwfsal.access_key_id = j['keys'][0]['access_key']
        rgwfsal.secret_access_key = j['keys'][0]['secret_key']
        log.debug("Successfully fetched user %s for RGW path %s", rgwfsal.user_id, export.path)

    def _ensure_cephfs_export_user(self, export: Export) -> None:
        fsal = cast(CephFSFSAL, export.fsal)
        assert fsal.fs_name
        assert fsal.cmount_path

        fsal.user_id = f"nfs.{get_user_id(export.cluster_id, fsal.fs_name, fsal.cmount_path)}"
        fsal.cephx_key = self._create_user_key(
            export.cluster_id, fsal.user_id, fsal.cmount_path, fsal.fs_name
        )
        log.debug(f"Established user {fsal.user_id} for cephfs {fsal.fs_name}")

    @staticmethod
    def _clients_per_pool_of_spec(spec: Optional[Any]) -> Optional[int]:
        """Client pool size an already fetched cluster spec asks for, if any."""
        if spec is None:
            return None
        n = getattr(spec, 'clients_per_pool', None)
        if n is None or n < 2:
            return None
        return int(n)

    def _get_cephfs_clients_per_pool(self, cluster_id: str) -> Optional[int]:
        """Same, for callers that have no spec at hand. Costs an orch call."""
        return self._clients_per_pool_of_spec(
            get_nfs_spec_for_cluster(self.mgr, cluster_id))

    def _uses_cephfs_client_pool(self, cluster_id: str, cmount_path: Optional[str]) -> bool:
        if not is_root_cmount(cmount_path):
            return False
        return self._get_cephfs_clients_per_pool(cluster_id) is not None

    def _pool_fs_of_export(self, cluster_id: str, fsal: CephFSFSAL) -> Optional[str]:
        """The filesystem whose client pool this export use"""
        if not fsal.fs_name or not is_root_cmount(fsal.cmount_path):
            return None
        ceph_users = self._read_ceph_users(cluster_id)
        if not ceph_users or not ceph_users.users_for_fs(fsal.fs_name):
            return None
        return fsal.fs_name

    def _setup_cephfs_export_credentials(
            self,
            export: Export,
            clients_per_pool: Optional[int],
    ) -> None:
        """Give the export its CephX identity.

        ``clients_per_pool`` is the size the cluster spec asks for, already
        resolved by the caller, so creating an export does not fetch the spec
        again here.
        """
        fsal = cast(CephFSFSAL, export.fsal)
        if clients_per_pool and is_root_cmount(fsal.cmount_path):
            assert fsal.fs_name
            pool_users = self._ensure_pool_users_for_fs(
                export.cluster_id, fsal.fs_name, clients_per_pool)
            if pool_users:
                # The FSAL block carries the first identity of the pool, so the
                # export stays usable on its own, Ganesha picks a handle out of
                # the CEPH_USERS block for the actual client pool.
                primary = pool_users[0]
                fsal.user_id = primary.user_id
                fsal.cephx_key = primary.secret_access_key
                log.debug("Pool mode: export for cephfs %s uses pool user %s",
                          fsal.fs_name, fsal.user_id)
                return
        self._ensure_cephfs_export_user(export)

    @contextmanager
    def _ceph_users_locked(self, cluster_id: str) -> Iterator[None]:
        with self._ceph_users_locks_guard:
            lock = self._ceph_users_locks.setdefault(cluster_id, threading.RLock())
        with lock:
            yield

    def _read_ceph_users(self, cluster_id: str) -> Optional[CephUsers]:
        raw = self._rados(cluster_id).read_obj(ceph_users_obj_name(cluster_id))
        if raw is None:
            return None
        return CephUsers.from_raw(raw)

    def _write_ceph_users(
            self,
            cluster_id: str,
            ceph_users: CephUsers,
            link: bool = False,
            should_notify: bool = False
    ) -> None:
        rados_obj = self._rados(cluster_id)
        block = format_block(ceph_users.to_block())
        obj = ceph_users_obj_name(cluster_id)
        conf = conf_obj_name(cluster_id)
        if link:
            rados_obj.write_obj(block, obj, conf, should_notify=should_notify)
        else:
            rados_obj.update_obj(block, obj, conf, should_notify=should_notify)

    @staticmethod
    def _pool_slot_ids(cluster_id: str, fs_name: str, clients_per_pool: int) -> List[str]:
        """The CephX ids a pool of this filesystem is made of, in slot order.

        Derived from the naming scheme alone, so it says what the pool should
        hold rather than what CEPH_USERS happens to record.
        """
        return [get_pool_user_id(cluster_id, fs_name, i)
                for i in range(clients_per_pool)]

    def _remove_pool_users(self, users: List[CephUser]) -> None:
        for user in users:
            self.mgr.check_mon_command({
                'prefix': 'auth rm',
                'entity': f'client.{user.user_id}',
            })
            log.info("Deleted pool user %s", user.user_id)

    def _missing_pool_slots(
            self,
            cluster_id: str,
            fs_name: str,
            existing: List[CephUser],
            clients_per_pool: int,
    ) -> List[str]:
        have = {u.user_id for u in existing}
        return [
            user_id
            for user_id in self._pool_slot_ids(cluster_id, fs_name, clients_per_pool)
            if user_id not in have
        ]

    def _create_pool_users(
            self,
            cluster_id: str,
            fs_name: str,
            user_ids: List[str],
    ) -> List[CephUser]:
        return [
            CephUser(
                user_id=user_id,
                secret_access_key=self._create_user_key(cluster_id, user_id, "/", fs_name),
                filesystem=fs_name,
            )
            for user_id in user_ids
        ]

    def reconcile_client_pool(self, cluster_id: str, clients_per_pool: int) -> List[str]:
        """Size the client pool of every CephFS the cluster serves to match.

        Pool users are otherwise provisioned by the first ``cmount_path=/``
        export of a filesystem. A cluster that enables or grows
        ``clients_per_pool`` once such exports already exist has no such
        trigger, so the pools are topped up from here instead.
        """
        if clients_per_pool < 2:
            return []
        with self._ceph_users_locked(cluster_id):
            fs_names: List[str] = []
            for export in self._read_raw_config(cluster_id):
                fsal = export.fsal
                if not isinstance(fsal, CephFSFSAL) or not fsal.fs_name:
                    continue
                if fsal.cmount_path and fsal.cmount_path != "/":
                    continue
                if fsal.fs_name not in fs_names:
                    fs_names.append(fsal.fs_name)
            if not fs_names:
                return []

            current = self._read_ceph_users(cluster_id)
            ceph_users = current or CephUsers()

            created: List[str] = []
            retired: List[str] = []
            to_revoke: List[CephUser] = []
            for fs_name in fs_names:
                wanted = set(get_pool_user_id(cluster_id, fs_name, i)
                             for i in range(clients_per_pool))
                extra = [u for u in ceph_users.users_for_fs(fs_name)
                         if u.user_id not in wanted]
                if extra:
                    log.warning(
                        "Pool of fs %s on cluster %s holds %d users the "
                        "cluster no longer asks for, retiring them",
                        fs_name, cluster_id, len(extra))
                    ceph_users.remove_users(fs_name, {u.user_id for u in extra})
                    retired.extend(u.user_id for u in extra)
                    to_revoke.extend(extra)
                missing = self._missing_pool_slots(
                    cluster_id, fs_name,
                    ceph_users.users_for_fs(fs_name), clients_per_pool)
                if not missing:
                    continue
                # Past here cephx has been written to, so the pool is only
                # consistent again once the block below lands.
                new_users = self._create_pool_users(cluster_id, fs_name, missing)
                ceph_users.users.extend(new_users)
                created.extend(u.user_id for u in new_users)

            if not created and not retired:
                log.debug("Client pool of cluster %s is already %s users wide",
                          cluster_id, clients_per_pool)
                return []
            # A cluster only now turning client-pool mode on has no CEPH_USERS
            # object yet, so this is also where it gets linked from the common
            # config.
            # The write is quiet. Redeploy due to dependency will notify nfs
            self._write_ceph_users(cluster_id, ceph_users, link=current is None)
            if to_revoke:
                self._remove_pool_users(to_revoke)
            log.info("Sized client pool of cluster %s to %s users per "
                     "filesystem, added %s, retired %s",
                     cluster_id, clients_per_pool, created, retired)
            return created

    def _ensure_pool_users_for_fs(
            self,
            cluster_id: str,
            fs_name: str,
            clients_per_pool: int,
    ) -> List[CephUser]:
        if clients_per_pool < 2:
            return []
        with self._ceph_users_locked(cluster_id):
            current = self._read_ceph_users(cluster_id)
            ceph_users = current or CephUsers()
            wanted = self._pool_slot_ids(cluster_id, fs_name, clients_per_pool)
            existing = ceph_users.users_for_fs(fs_name)
            if [u.user_id for u in existing] == wanted:
                log.debug("Pool users already exist for fs %s on cluster %s", fs_name, cluster_id)
                return existing
            to_revoke: List[CephUser] = []
            if existing:
                # The pool on disk is not the size the cluster now asks for.
                # A cluster removed with `orch rm` and recreated with another
                # clients_per_pool leaves its CEPH_USERS object behind, and
                # ganesha refuses a USERS block that does not hold exactly
                # clients_per_pool identities, so the pool is resized to match
                # rather than handed back as it is.
                log.warning(
                    "Pool of fs %s on cluster %s holds %d users but "
                    "clients_per_pool is %d, resizing it",
                    fs_name, cluster_id, len(existing), clients_per_pool
                )
                to_revoke = [u for u in existing if u.user_id not in set(wanted)]
                ceph_users.remove_fs(fs_name)
            # Every slot is re-read from cephx here rather than topped up like
            # reconcile_client_pool does, this is the path that meets a stale
            # CEPH_USERS object, whose recorded keys may belong to users the
            # removed cluster took with it. get-or-create returns the key a
            # slot that survived the resize already had, so only the slots
            # that are really new change.
            new_users = self._create_pool_users(cluster_id, fs_name, wanted)
            ceph_users.users.extend(new_users)
            self._write_ceph_users(cluster_id, ceph_users, link=current is None)
            if to_revoke:
                self._remove_pool_users(to_revoke)
            log.info("Created %s pool users for fs %s on cluster %s",
                     clients_per_pool, fs_name, cluster_id)
            return new_users

    def _cleanup_pool_users_for_fs(self, cluster_id: str, fs_name: str) -> None:
        with self._ceph_users_locked(cluster_id):
            ceph_users = self._read_ceph_users(cluster_id)
            if ceph_users is None:
                log.warning(
                    "No CEPH_USERS object for cluster %s, leaving the pool "
                    "users of fs %s alone", cluster_id, fs_name)
                return
            users_to_rm = ceph_users.users_for_fs(fs_name)
            ceph_users.remove_fs(fs_name)
            if ceph_users.users:
                self._write_ceph_users(cluster_id, ceph_users)
            else:
                # The last pool of the cluster is gone, so is the object.
                self._rados(cluster_id).remove_obj(
                    ceph_users_obj_name(cluster_id),
                    conf_obj_name(cluster_id),
                    should_notify=False,
                )
            self._remove_pool_users(users_to_rm)

    def _gen_export_id(self, cluster_id: str) -> int:
        exports = sorted([ex.export_id for ex in self.exports[cluster_id]])
        nid = 1
        for e_id in exports:
            if e_id == nid:
                nid += 1
            else:
                break
        return nid

    def _read_raw_config(self, rados_namespace: str) -> List[Export]:
        """Parse the exports of one cluster straight out of RADOS.
        """
        exports: List[Export] = []
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            ioctx.set_namespace(rados_namespace)
            for obj in ioctx.list_objects():
                if obj.key.startswith(EXPORT_PREFIX):
                    size, _ = obj.stat()
                    raw_config = obj.read(size)
                    raw_config = raw_config.decode("utf-8")
                    log.debug("read export configuration from rados "
                              "object %s/%s/%s", self.rados_pool,
                              rados_namespace, obj.key)
                    exports.append(Export.from_export_block(
                        GaneshaConfParser(raw_config).parse()[0], rados_namespace))
        return exports

    def _save_export(self, cluster_id: str, export: Export) -> None:
        self.exports[cluster_id].append(export)
        self._rados(cluster_id).write_obj(
            format_block(export.to_export_block()),
            export_obj_name(export.export_id),
            conf_obj_name(export.cluster_id),
            (not self.skip_notify_nfs_server)
        )

    def _delete_export(
            self,
            cluster_id: str,
            pseudo_path: Optional[str],
            export_obj: Optional[Export] = None
    ) -> None:
        try:
            if export_obj:
                export: Optional[Export] = export_obj
            else:
                assert pseudo_path
                export = self._fetch_export(cluster_id, pseudo_path)

            if export:
                if export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[0]:
                    fsal = cast(CephFSFSAL, export.fsal)
                    pool_fs = self._pool_fs_of_export(cluster_id, fsal)
                    if pool_fs:
                        if self.get_export_count_with_same_fsal(
                                "/", cluster_id, pool_fs) == 1:
                            self._cleanup_pool_users_for_fs(cluster_id, pool_fs)
                    else:
                        exports_count = self.get_export_count_with_same_fsal(
                            fsal.cmount_path, cluster_id, fsal.fs_name)  # type: ignore
                        if exports_count == 1:
                            self._delete_export_user(export)
                if pseudo_path:
                    self._rados(cluster_id).remove_obj(
                        export_obj_name(export.export_id), conf_obj_name(cluster_id),
                        (not self.skip_notify_nfs_server))

                self.exports[cluster_id].remove(export)
                if export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[1]:
                    self._delete_export_user(export)
                if not self.exports[cluster_id]:
                    del self.exports[cluster_id]
                    log.debug("Deleted all exports for cluster %s", cluster_id)
                return None
            raise NonFatalError("Export does not exist")
        except Exception as e:
            log.exception(f"Failed to delete {pseudo_path} export for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def _fetch_export_obj(self, cluster_id: str, ex_id: int) -> Optional[Export]:
        try:
            with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
                ioctx.set_namespace(cluster_id)
                export = Export.from_export_block(
                    GaneshaConfParser(
                        ioctx.read(export_obj_name(ex_id)).decode("utf-8")
                    ).parse()[0],
                    cluster_id
                )
                return export
        except ObjectNotFound:
            log.exception("Export ID: %s not found", ex_id)
        return None

    def _update_export(self, cluster_id: str, export: Export,
                       need_nfs_service_restart: bool,
                       defer_reload: bool = False) -> None:
        self.exports[cluster_id].append(export)
        self._rados(cluster_id).update_obj(
            format_block(export.to_export_block()),
            export_obj_name(export.export_id), conf_obj_name(export.cluster_id),
            should_notify=(not defer_reload and not need_nfs_service_restart
                           and not self.skip_notify_nfs_server))
        if need_nfs_service_restart and not defer_reload:
            restart_nfs_service(self.mgr, export.cluster_id)

    def _reload_nfs_service(self, cluster_id: str, need_nfs_service_restart: bool) -> None:
        """Make ganesha pick up configuration that is already persisted."""
        if need_nfs_service_restart:
            restart_nfs_service(self.mgr, cluster_id)
        elif not self.skip_notify_nfs_server:
            self._rados(cluster_id).notify_conf(conf_obj_name(cluster_id))

    def _validate_cluster_id(self, cluster_id: str) -> None:
        """Raise an exception if cluster_id is not valid."""
        clusters = known_cluster_ids(self.mgr)
        log.debug("checking for %r in known nfs clusters: %r",
                  cluster_id, clusters)
        if cluster_id not in clusters:
            raise ErrorResponse(f"Cluster {cluster_id!r} does not exist",
                                return_value=-errno.ENOENT)

    def create_export(self, addr: Optional[List[str]] = None, **kwargs: Any) -> Dict[str, Any]:
        self._validate_cluster_id(kwargs['cluster_id'])
        # if addr(s) are provided, construct client list and adjust outer block
        clients = []
        if addr:
            clients = [{
                'addresses': addr,
                'access_type': 'ro' if kwargs['read_only'] else 'rw',
                'squash': kwargs['squash'],
            }]
            kwargs['squash'] = 'none'
        kwargs['clients'] = clients

        if clients:
            kwargs['access_type'] = "none"
        elif kwargs['read_only']:
            kwargs['access_type'] = "RO"
        else:
            kwargs['access_type'] = "RW"

        if kwargs['cluster_id'] not in self.exports:
            self.exports[kwargs['cluster_id']] = []

        try:
            fsal_type = kwargs.pop('fsal_type')
            if fsal_type == 'cephfs':
                return self.create_cephfs_export(**kwargs)
            if fsal_type == 'rgw':
                return self.create_rgw_export(**kwargs)
            raise NotImplementedError()
        except Exception as e:
            log.exception(
                f"Failed to create {kwargs['pseudo_path']} export for {kwargs['cluster_id']}")
            raise ErrorResponse.wrap(e)

    def delete_export(self,
                      cluster_id: str,
                      pseudo_path: str) -> None:
        self._validate_cluster_id(cluster_id)
        return self._delete_export(cluster_id, pseudo_path)

    def delete_all_exports(self, cluster_id: str) -> None:
        try:
            export_list = list(self.exports[cluster_id])
        except KeyError:
            log.info("No exports to delete")
            return
        for export in export_list:
            try:
                self._delete_export(cluster_id=cluster_id, pseudo_path=None,
                                    export_obj=export)
            except Exception as e:
                raise NFSException(f"Failed to delete export {export.export_id}: {e}")
        log.info("All exports successfully deleted for cluster id: %s", cluster_id)

    def list_all_exports(self) -> List[Dict[str, Any]]:
        r = []
        for cluster_id, ls in self.exports.items():
            r.extend([e.to_dict() for e in ls])
        return r

    def list_exports(self,
                     cluster_id: str,
                     detailed: bool = False) -> List[Any]:
        self._validate_cluster_id(cluster_id)
        try:
            if detailed:
                result_d = [export.to_dict() for export in self.exports[cluster_id]]
                return result_d
            else:
                result_ps = [export.pseudo for export in self.exports[cluster_id]]
                return result_ps

        except KeyError:
            log.warning("No exports to list for %s", cluster_id)
            return []
        except Exception as e:
            log.exception(f"Failed to list exports for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def _client_pool_info(self, cluster_id: str, export: Export) -> Optional[Dict[str, Any]]:
        """The client pool an export draws its CephFS handles from, if any.

        The FSAL block of such an export only names slot 0, so the rest of the
        pool is invisible in the export itself. Report the ids the way
        CEPH_USERS holds them, in slot order. The keys stay out of it, like
        the FSAL cephx_key does.
        """
        fsal = export.fsal
        if not isinstance(fsal, CephFSFSAL) or not fsal.fs_name:
            return None
        if not self._uses_cephfs_client_pool(cluster_id, fsal.cmount_path):
            return None
        ceph_users = self._read_ceph_users(cluster_id)
        if not ceph_users:
            return None
        users = ceph_users.users_for_fs(fsal.fs_name)
        if not users:
            return None
        return {
            'fs_name': fsal.fs_name,
            'users': [u.user_id for u in users],
        }

    def _get_export_dict(self, cluster_id: str, pseudo_path: str) -> Optional[Dict[str, Any]]:
        export = self._fetch_export(cluster_id, pseudo_path)
        if export:
            export_dict = export.to_dict()
            client_pool = self._client_pool_info(cluster_id, export)
            if client_pool:
                export_dict['client_pool'] = client_pool
            return export_dict
        log.warning(f"No {pseudo_path} export to show for {cluster_id}")
        return None

    def get_export(
            self,
            cluster_id: str,
            pseudo_path: str,
    ) -> Dict[str, Any]:
        self._validate_cluster_id(cluster_id)
        try:
            export_dict = self._get_export_dict(cluster_id, pseudo_path)
            log.info(f"Fetched {export_dict!r} for {cluster_id!r}, {pseudo_path!r}")
            return export_dict if export_dict else {}
        except Exception as e:
            log.exception(f"Failed to get {pseudo_path} export for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def get_export_by_id(
            self,
            cluster_id: str,
            export_id: int
    ) -> Optional[Dict[str, Any]]:
        export = self._fetch_export_id(cluster_id, export_id)
        return export.to_dict() if export else None

    def get_export_by_pseudo(
            self,
            cluster_id: str,
            pseudo_path: str
    ) -> Optional[Dict[str, Any]]:
        export = self._fetch_export(cluster_id, pseudo_path)
        return export.to_dict() if export else None

    def reload_exports(self) -> None:
        """Drop the cached exports so that they are re-read from RADOS."""
        self._exports = None

    def get_cephfs_export_user_ids(self, cluster_id: str) -> Set[str]:
        """Return CephFS export user_ids for the cluster."""
        user_ids = {
            export.fsal.user_id
            for export in self.exports.get(cluster_id, [])
            if isinstance(export.fsal, CephFSFSAL)
            and export.fsal.user_id
        }
        ceph_users = self._read_ceph_users(cluster_id)
        if ceph_users:
            user_ids.update(u.user_id for u in ceph_users.users if u.user_id)
        return user_ids

    def _refresh_pool_user_keys(
            self,
            cluster_id: str,
            entity_set: Set[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Re-fetch rotated CephX keys into CEPH_USERS.

        Returns the pool user ids that were rewritten and the ones that could
        not be. The write is quiet: the caller knows whether an export write
        follows that will make ganesha reread anyway.
        """
        with self._ceph_users_locked(cluster_id):
            ceph_users = self._read_ceph_users(cluster_id)
            if not ceph_users or not ceph_users.users:
                return [], []

            updated: List[str] = []
            stale: List[str] = []

            for user in ceph_users.users:
                entity = f'client.{user.user_id}'
                if entity not in entity_set:
                    continue
                old_key = user.secret_access_key
                try:
                    key = self._create_user_key(
                        cluster_id, user.user_id, "/", user.filesystem)
                    if key == old_key:
                        log.warning(
                            "Pool user %s key did not change after rotate",
                            user.user_id
                        )
                    user.secret_access_key = key
                    updated.append(user.user_id)
                except Exception:
                    stale.append(user.user_id)
                    log.exception(
                        "Failed to update pool user %s after key rotation",
                        user.user_id
                    )

            if updated:
                try:
                    self._write_ceph_users(cluster_id, ceph_users)
                except Exception:
                    log.exception(
                        "Failed to write CEPH_USERS for cluster %s after key rotation",
                        cluster_id
                    )
                    # Nothing was persisted, so none of them made it.
                    return [], stale + updated
                log.info(
                    "Updated CEPH_USERS for cluster %s after rotating pool users %s",
                    cluster_id, updated
                )
            return updated, stale

    def refresh_export_keys(
            self,
            cluster_id: str,
            entities: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Re-fetch CephX keys for the given export entities and update matching
        CephFS exports so Ganesha receives the new keyrings.

        Returns the pseudo paths that were updated and the ones that could not
        be. One failure must not stop the others: their keys are already
        rotated, so an export left behind is one Ganesha cannot authenticate.
        """
        entity_set = {normalize_auth_entity(e) for e in entities}
        matching = [
            export for export in self.exports.get(cluster_id, [])
            if isinstance(export.fsal, CephFSFSAL) and export.fsal.user_id
            and f'client.{export.fsal.user_id}' in entity_set
        ]
        # The CEPH_USERS write is quiet, every export write that follows
        # notifies ganesha on its own, as it does outside key rotation.
        pool_updated, stale = self._refresh_pool_user_keys(cluster_id, entity_set)

        updated_pseudos: List[str] = []
        stale_pseudos: List[str] = []

        for export in matching:
            fsal = cast(CephFSFSAL, export.fsal)
            old_key = fsal.cephx_key
            try:
                self._ensure_cephfs_export_user(export)
                if fsal.cephx_key == old_key:
                    log.warning(
                        "Export %s key for user %s did not change after rotate",
                        export.pseudo, fsal.user_id
                    )
                self.exports[cluster_id].remove(export)
                self._update_export(cluster_id, export, need_nfs_service_restart=False)
            except Exception:
                stale_pseudos.append(export.pseudo)
                log.exception("Failed to update export %s after key rotation", export.pseudo)
                continue

            updated_pseudos.append(export.pseudo)
            log.info(
                "Updated export %s with rotated key for user %s",
                export.pseudo, fsal.user_id
            )

        if pool_updated and not updated_pseudos and not self.skip_notify_nfs_server:
            # No export write happened, or every one of them failed, but the
            # pool's new keys are persisted: notify here so ganesha does not
            # keep using a credential that no longer exists.
            log.info(
                "Notifying ganesha for cluster %s: pool users %s were rotated "
                "but no export write succeeded to announce it",
                cluster_id, pool_updated
            )
            try:
                self._rados(cluster_id).notify_conf(conf_obj_name(cluster_id))
            except Exception as e:
                log.exception(
                    "Failed to notify ganesha for cluster %s after rotating "
                    "pool users %s", cluster_id, pool_updated
                )
                raise ErrorResponse.wrap(e)
        return updated_pseudos, stale + stale_pseudos

    # This method is used by the dashboard module (../dashboard/controllers/nfs.py)
    # Do not change interface without updating the Dashboard code
    def apply_export(self, cluster_id: str, export_config: str,
                     earmark_resolver: Optional[CephFSEarmarkResolver] = None) -> AppliedExportResults:
        try:
            exports = self._read_export_config(cluster_id, export_config)
        except Exception as e:
            log.exception(f'Failed to update export: {e}')
            raise ErrorResponse.wrap(e)

        aeresults = AppliedExportResults()
        for export in exports:
            changed_export = self._change_export(cluster_id, export, earmark_resolver)
            # This will help figure out which export blocks in conf/json file
            # are problematic.
            if changed_export.get("state", "") == "error":
                changed_export.update({"index": exports.index(export) + 1})
            aeresults.append(changed_export)
        return aeresults

    def _read_export_config(self, cluster_id: str, export_config: str) -> List[Dict]:
        if not export_config:
            raise NFSInvalidOperation("Empty Config!!")
        try:
            j = json.loads(export_config)
        except ValueError:
            # okay, not JSON.  is it an EXPORT block?
            try:
                blocks = GaneshaConfParser(export_config).parse()
                exports = [
                    Export.from_export_block(block, cluster_id)
                    for block in blocks
                ]
                j = [export.to_dict() for export in exports]
            except Exception as ex:
                raise NFSInvalidOperation(f"Input must be JSON or a ganesha EXPORT block: {ex}")
        # check export type - always return a list
        if isinstance(j, list):
            return j  # j is already a list object
        return [j]  # return a single object list, with j as the only item

    def _change_export(self, cluster_id: str, export: Dict,
                       earmark_resolver: Optional[CephFSEarmarkResolver] = None) -> Dict[str, Any]:
        try:
            return self._apply_export(cluster_id, export, earmark_resolver)
        except NotImplementedError:
            # in theory, the NotImplementedError here may be raised by a hook back to
            # an orchestration module. If the orchestration module supports it the NFS
            # servers may be restarted. If not supported the expectation is that an
            # (unfortunately generic) NotImplementedError will be raised. We then
            # indicate to the user that manual intervention may be needed now that the
            # configuration changes have been applied.
            return {
                "pseudo": export['pseudo'],
                "state": "warning",
                "msg": "changes applied (Manual restart of NFS Pods required)",
            }
        except Exception as ex:
            msg = f'Failed to apply export: {ex}'
            log.exception(msg)
            return {"state": "error", "msg": msg, "exception": ex,
                    "pseudo": export['pseudo']}

    def _update_user_id(
            self,
            cluster_id: str,
            path: str,
            fs_name: str,
            user_id: str
    ) -> None:
        osd_cap = 'allow rw pool={} namespace={}, allow rw tag cephfs data={}'.format(
            self.rados_pool, cluster_id, fs_name)
        # NFS-Ganesha can dynamically enforce an export's access type changes, but Ceph server
        # daemons can't dynamically enforce changes in Ceph user caps of the Ceph clients. To
        # allow dynamic updates of CephFS NFS exports, always set FSAL Ceph user's MDS caps with
        # path restricted read-write access. Rely on the ganesha servers to enforce the export
        # access type requested for the NFS clients.
        self.mgr.check_mon_command({
            'prefix': 'auth caps',
            'entity': f'client.{user_id}',
            'caps': ['mon', 'allow r', 'osd', osd_cap, 'mds', 'allow rw path={}'.format(path)],
        })

        log.info("Export user updated %s", user_id)

    def _create_user_key(self, cluster_id: str, entity: str, path: str, fs_name: str) -> str:
        osd_cap = f'allow rw pool={self.rados_pool} namespace={cluster_id}, allow rw tag cephfs data={fs_name}'
        nfs_caps = [
            'mon', 'allow r',
            'osd', osd_cap,
            'mds', f'allow rw path={path}'
        ]

        ret, out, err = self.mgr.mon_command({
            'prefix': 'auth get-or-create',
            'entity': f'client.{entity}',
            'caps': nfs_caps,
            'format': 'json',
        })
        if ret == -errno.EINVAL and 'does not match' in err:
            ret, out, err = self.mgr.mon_command({
                'prefix': 'auth caps',
                'entity': f'client.{entity}',
                'caps': nfs_caps,
                'format': 'json',
            })
            if ret:
                raise NFSException(f'Failed to update caps for {entity}: {err}', ret)
            ret, out, err = self.mgr.mon_command({
                'prefix': 'auth get',
                'entity': f'client.{entity}',
                'format': 'json',
            })
            if ret:
                raise NFSException(f'Failed to fetch caps for {entity}: {err}', ret)
        elif ret:
            raise NFSException(f'Failed to create user {entity}: {err}', ret)

        json_res = json.loads(out)
        log.info(f"Export user created is {json_res[0]['entity']}")
        return json_res[0]['key']

    def _check_earmark(self, earmark_resolver: CephFSEarmarkResolver, path: str,
                       fs_name: str) -> None:
        earmark = earmark_resolver.get_earmark(
            path,
            fs_name,
        )
        if not earmark:
            earmark_resolver.set_earmark(
                path,
                fs_name,
                EarmarkTopScope.NFS.value,
            )
        else:
            if not earmark_resolver.check_earmark(
                earmark, EarmarkTopScope.NFS
            ):
                raise NFSException(
                    'earmark has already been set by ' + earmark.split('.')[0],
                    -errno.EAGAIN
                )
        return None

    def _validate_kmip_key_id_with_enctag(
        self,
        kmip_key_id: str,
        fs_name: Optional[str],
        path: Optional[str]
    ) -> None:
        """
        Validate that kmip_key_id matches the subvolume's enctag for CephFS exports.
        """
        if not fs_name or not path:
            log.debug(f"Skipping enctag validation for kmip_key_id '{kmip_key_id}': "
                      f"fs_name={fs_name}, path={path} (likely not a CephFS export)")
            return

        enctag = get_enctag_from_path(self.mgr, fs_name, path)
        if not enctag:
            raise NFSInvalidOperation(
                f"Cannot create export with kmip_key_id '{kmip_key_id}': "
                f"subvolume at path '{path}' does not have an enctag set. "
                "Please set the enctag on the subvolume using "
                "'ceph fs subvolume enctag set' command."
            )
        if enctag != kmip_key_id:
            raise NFSInvalidOperation(
                f"Cannot create export: kmip_key_id '{kmip_key_id}' does not match "
                f"subvolume enctag '{enctag}' at path '{path}'. "
                "The kmip_key_id must match the enctag set on the subvolume."
            )
        log.info(f"Validated kmip_key_id '{kmip_key_id}' matches subvolume enctag at path '{path}'")

    def create_export_from_dict(self,
                                cluster_id: str,
                                ex_id: int,
                                ex_dict: Dict[str, Any],
                                earmark_resolver: Optional[CephFSEarmarkResolver] = None
                                ) -> Export:
        pseudo_path = ex_dict.get("pseudo")
        if not pseudo_path:
            raise NFSInvalidOperation("export must specify pseudo path")

        path = ex_dict.get("path")
        if path is None:
            raise NFSInvalidOperation("export must specify path")
        path = normalize_path(path)

        fsal = ex_dict.get("fsal", {})
        fsal_type = fsal.get("name")

        if fsal_type == NFS_GANESHA_SUPPORTED_FSALS[1]:
            if '/' in path and path != '/':
                raise NFSInvalidOperation('"/" is not allowed in path with bucket name')

            # BYOK encryption is only supported by the CEPH FSAL
            if ex_dict.get("kmip_key_id"):
                raise NFSInvalidOperation(
                    "kmip_key_id is only supported for CephFS exports, "
                    f"not for {NFS_GANESHA_SUPPORTED_FSALS[1]} exports")

        elif fsal_type == NFS_GANESHA_SUPPORTED_FSALS[0]:
            fs_name = fsal.get("fs_name")
            if not fs_name:
                raise NFSInvalidOperation("export FSAL must specify fs_name")
            if not check_fs(self.mgr, fs_name):
                raise FSNotFound(fs_name)

            validate_cephfs_path(self.mgr, fs_name, path)

            # Validate kmip_key_id against subvolume enctag for CephFS
            kmip_key_id = ex_dict.get("kmip_key_id")
            if kmip_key_id:
                self._validate_kmip_key_id_with_enctag(kmip_key_id, fs_name, path)

            # Check if earmark is set for the path, given path is of subvolume
            if earmark_resolver:
                self._check_earmark(earmark_resolver, path, fs_name)

            cmount_path = fsal.get("cmount_path", "/")
            if cmount_path != "/":
                _validate_cmount_path(cmount_path, path)

            # In client-pool mode the FSAL still carries slot 0 of the pool,
            # which is this very id, so the rule is the same either way.
            user_id = f"nfs.{get_user_id(cluster_id, fs_name, cmount_path)}"
            if "user_id" in fsal and fsal["user_id"] != user_id:
                raise NFSInvalidOperation(f"export FSAL user_id must be '{user_id}'")
        else:
            raise NFSInvalidOperation(f"NFS Ganesha supported FSALs are {NFS_GANESHA_SUPPORTED_FSALS}."
                                      "Export must specify any one of it.")

        ex_dict["fsal"] = fsal
        ex_dict["cluster_id"] = cluster_id
        # When RDMA is enabled at cluster level, default export transports to tcp, RDMA
        nfs_spec = get_nfs_spec_for_cluster(self.mgr, cluster_id)
        rdma_status = getattr(nfs_spec, "enable_rdma", False)
        if "transports" not in ex_dict and rdma_status:
            ex_dict["transports"] = ["TCP", "RDMA"]
        elif "RDMA" in ex_dict.get("transports", []) and not rdma_status:
            raise NFSInvalidOperation("Can't set Transport as RDMA as RDMA not enabled at NFS cluster level")

        export = Export.from_dict(ex_id, ex_dict)
        export.validate(self.mgr)

        if export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[0]:
            self._setup_cephfs_export_credentials(
                export, self._clients_per_pool_of_spec(nfs_spec))
        log.debug("Successfully created %s export-%s from dict for cluster %s",
                  fsal_type, ex_id, cluster_id)
        return export

    def create_cephfs_export(self,
                             fs_name: str,
                             cluster_id: str,
                             pseudo_path: str,
                             read_only: bool,
                             path: str,
                             squash: str,
                             access_type: str,
                             clients: list = [],
                             sectype: Optional[List[str]] = None,
                             xprtsec: Optional[str] = None,
                             cmount_path: Optional[str] = "/",
                             transports: Optional[List[str]] = None,
                             earmark_resolver: Optional[CephFSEarmarkResolver] = None,
                             kmip_key_id: Optional[str] = None
                             ) -> Dict[str, Any]:

        validate_cephfs_path(self.mgr, fs_name, path)
        if cmount_path != "/":
            _validate_cmount_path(cmount_path, path)  # type: ignore

        pseudo_path = normalize_path(pseudo_path)
        # Get the protocols based on cluster's enable_nfsv3 setting
        protocols = self._get_cluster_protocols(cluster_id)

        export_dict = {
            "pseudo": pseudo_path,
            "path": path,
            "access_type": access_type,
            "squash": squash,
            "fsal": {
                "name": NFS_GANESHA_SUPPORTED_FSALS[0],
                "cmount_path": cmount_path,
                "fs_name": fs_name,
            },
            "clients": clients,
            "sectype": sectype,
            "XprtSec": xprtsec,
            "protocols": protocols,
            "kmip_key_id": kmip_key_id
        }
        if transports is not None:
            export_dict["transports"] = transports
        if not self._fetch_export(cluster_id, pseudo_path):
            export = self.create_export_from_dict(
                cluster_id,
                self._gen_export_id(cluster_id),
                export_dict,
                earmark_resolver
            )
            log.debug("creating cephfs export %s", export)
            self._save_export(cluster_id, export)
            result = {
                "bind": export.pseudo,
                "fs": fs_name,
                "path": export.path,
                "cluster": cluster_id,
                "mode": export.access_type,
            }
            return result
        raise NonFatalError("Export already exists")

    def create_rgw_export(self,
                          cluster_id: str,
                          pseudo_path: str,
                          access_type: str,
                          read_only: bool,
                          squash: str,
                          bucket: Optional[str] = None,
                          user_id: Optional[str] = None,
                          clients: list = [],
                          sectype: Optional[List[str]] = None,
                          xprtsec: Optional[str] = None,
                          transports: Optional[List[str]] = None) -> Dict[str, Any]:
        pseudo_path = normalize_path(pseudo_path)

        if not bucket and not user_id:
            raise ErrorResponse("Must specify either bucket or user_id")

        # Get the protocols based on cluster's enable_nfsv3 setting
        protocols = self._get_cluster_protocols(cluster_id)

        export_dict = {
            "pseudo": pseudo_path,
            "path": bucket or '/',
            "access_type": access_type,
            "squash": squash,
            "fsal": {
                "name": NFS_GANESHA_SUPPORTED_FSALS[1],
                "user_id": user_id,
            },
            "clients": clients,
            "sectype": sectype,
            "XprtSec": xprtsec,
            "protocols": protocols
        }
        if transports is not None:
            export_dict["transports"] = transports
        if not self._fetch_export(cluster_id, pseudo_path):
            export = self.create_export_from_dict(
                cluster_id,
                self._gen_export_id(cluster_id),
                export_dict
            )
            log.debug("creating rgw export %s", export)
            self._create_rgw_export_user(export)
            self._save_export(cluster_id, export)
            result = {
                "bind": export.pseudo,
                "path": export.path,
                "cluster": cluster_id,
                "mode": export.access_type,
                "squash": export.squash,
            }
            return result
        raise NonFatalError("Export already exists")

    def _apply_export(
            self,
            cluster_id: str,
            new_export_dict: Dict,
            earmark_resolver: Optional[CephFSEarmarkResolver] = None
    ) -> Dict[str, str]:
        for k in ['path', 'pseudo']:
            if k not in new_export_dict:
                raise NFSInvalidOperation(f'Export missing required field {k}')
        if cluster_id not in self.exports:
            self.exports[cluster_id] = []

        new_export_dict['path'] = normalize_path(new_export_dict['path'])
        new_export_dict['pseudo'] = normalize_path(new_export_dict['pseudo'])

        old_export = self._fetch_export(cluster_id, new_export_dict['pseudo'])
        if old_export:
            # Check if export id matches
            if new_export_dict.get('export_id'):
                if old_export.export_id != new_export_dict.get('export_id'):
                    raise NFSInvalidOperation('Export ID changed, Cannot update export')
            else:
                new_export_dict['export_id'] = old_export.export_id
        elif new_export_dict.get('export_id'):
            old_export = self._fetch_export_obj(cluster_id, new_export_dict['export_id'])
            if old_export:
                # re-fetch via old pseudo
                old_export = self._fetch_export(cluster_id, old_export.pseudo)
                assert old_export
                log.debug("export %s pseudo %s -> %s",
                          old_export.export_id, old_export.pseudo, new_export_dict['pseudo'])

        fsal_dict = new_export_dict.get('fsal')
        if fsal_dict and fsal_dict['name'] == NFS_GANESHA_SUPPORTED_FSALS[0]:
            # Ensure cmount_path is present in CephFS FSAL block
            if not fsal_dict.get('cmount_path'):
                if old_export:
                    new_export_dict['fsal']['cmount_path'] = old_export.fsal.cmount_path
                else:
                    new_export_dict['fsal']['cmount_path'] = '/'

        # Set protocols based on cluster's enable_nfsv3 setting if not explicitly specified
        if 'protocols' not in new_export_dict:
            new_export_dict['protocols'] = self._get_cluster_protocols(cluster_id)

        if old_export:
            if fsal_dict and fsal_dict.get('name') != old_export.fsal.name:
                raise NFSInvalidOperation('FSAL change not allowed')
            if new_export_dict.get('qos_block'):
                old_qos = {}
                if old_export.qos_block:
                    old_qos = old_export.qos_block.to_dict()
                export_dict_qos_bw_ops_checks(
                    cluster_id, self.mgr,
                    dict(new_export_dict.get('qos_block', {})), old_qos)

        new_export = self.create_export_from_dict(
            cluster_id,
            new_export_dict.get('export_id', self._gen_export_id(cluster_id)),
            new_export_dict,
            earmark_resolver
        )

        if not old_export:
            if new_export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[1]:  # only for RGW
                self._create_rgw_export_user(new_export)
            self._save_export(cluster_id, new_export)
            return {"pseudo": new_export.pseudo, "state": "added"}

        need_nfs_service_restart = True
        if old_export.pseudo != new_export.pseudo:
            log.debug('export %s pseudo %s -> %s',
                      new_export.export_id, old_export.pseudo, new_export.pseudo)

        if old_export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[0]:
            old_fsal = cast(CephFSFSAL, old_export.fsal)
            new_fsal = cast(CephFSFSAL, new_export.fsal)
            need_nfs_service_restart = not (old_fsal.user_id == new_fsal.user_id
                                            and old_fsal.fs_name == new_fsal.fs_name
                                            and old_export.path == new_export.path
                                            and old_export.pseudo == new_export.pseudo)

        if old_export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[1]:
            old_rgw_fsal = cast(RGWFSAL, old_export.fsal)
            new_rgw_fsal = cast(RGWFSAL, new_export.fsal)
            if old_rgw_fsal.user_id != new_rgw_fsal.user_id:
                self._delete_export_user(old_export)
                self._create_rgw_export_user(new_export)
            elif old_rgw_fsal.access_key_id != new_rgw_fsal.access_key_id:
                raise NFSInvalidOperation('access_key_id change is not allowed')
            elif old_rgw_fsal.secret_access_key != new_rgw_fsal.secret_access_key:
                raise NFSInvalidOperation('secret_access_key change is not allowed')

        self.exports[cluster_id].remove(old_export)

        orphaned_pool_fs: Optional[str] = None
        if (old_export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[0]
                and new_export.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[0]):
            old_fsal = cast(CephFSFSAL, old_export.fsal)
            new_fsal = cast(CephFSFSAL, new_export.fsal)
            old_pool_fs = self._pool_fs_of_export(cluster_id, old_fsal)
            # The replacement keeps the old pool if it still names that
            # filesystem at the default cmount_path: it then carries slot 0 of
            # the pool whether or not the cluster still asks for one.
            new_keeps_pool = (new_fsal.fs_name == old_pool_fs
                              and is_root_cmount(new_fsal.cmount_path))
            if (old_pool_fs
                    and not new_keeps_pool
                    and self.get_export_count_with_same_fsal(
                        "/", cluster_id, old_pool_fs) == 0):
                orphaned_pool_fs = old_pool_fs

        if orphaned_pool_fs is None:
            self._update_export(cluster_id, new_export, need_nfs_service_restart)
            return {"pseudo": new_export.pseudo, "state": "updated"}

        # Write the replacement export before revoking the pool it is moving
        # away from. The export object on disk still carries the old pool's
        # credentials until that write lands, so tearing the pool down first
        # would leave a working export unable to authenticate if the write
        # failed. Ganesha is told once, after both changes are persisted, so it
        # never reloads a configuration that is half way through the move.
        self._update_export(cluster_id, new_export, need_nfs_service_restart,
                            defer_reload=True)
        try:
            self._cleanup_pool_users_for_fs(cluster_id, orphaned_pool_fs)
        finally:
            self._reload_nfs_service(cluster_id, need_nfs_service_restart)

        return {"pseudo": new_export.pseudo, "state": "updated"}

    def _rados(self, cluster_id: str) -> NFSRados:
        """Return a new NFSRados object for the given cluster id."""
        return NFSRados(self.mgr.rados, cluster_id)

    def get_export_count_with_same_fsal(self, cmount_path: str, cluster_id: str, fs_name: str) -> int:
        exports = self.list_exports(cluster_id, detailed=True)
        exports_count = 0
        for export in exports:
            if (export['fsal']['name'] == 'CEPH'
                    and export['fsal'].get('cmount_path', '/') == cmount_path
                    and export['fsal']['fs_name'] == fs_name):
                exports_count += 1
        return exports_count

    def update_export_qos(self,
                          cluster_id: str,
                          pseudo_path: str,
                          export_obj: Export,
                          enable_qos: bool,
                          bw_obj: Optional[QOSBandwidthControl] = None,
                          ops_obj: Optional[QOSOpsControl] = None) -> None:
        """Update Export QoS block"""
        # if qos_block does not exists in export create one else update existing block
        if not export_obj.qos_block:
            log.debug(f"Creating new QoS block for export {pseudo_path} of cluster {cluster_id}")
            export_obj.qos_block = QOS(enable_qos=enable_qos, bw_obj=bw_obj, ops_obj=ops_obj)
        else:
            log.debug(f"Updating existing QoS block of export {pseudo_path} of cluster {cluster_id}")
            export_obj.qos_block.enable_qos = enable_qos
            if bw_obj:
                export_obj.qos_block.bw_obj = bw_obj
            if ops_obj:
                export_obj.qos_block.ops_obj = ops_obj

        self.exports[cluster_id].remove(export_obj)
        self._update_export(cluster_id, export_obj, False)
        log.debug(f"Successfully updated QoS control config for export {pseudo_path} of cluster {cluster_id}")

    def get_export_obj(self, cluster_id: str, pseudo_path: str) -> Export:
        self._validate_cluster_id(cluster_id)
        export = self._fetch_export(cluster_id, pseudo_path)
        if not export:
            raise NFSObjectNotFound(f"Export {pseudo_path} not found in NFS cluster {cluster_id}")
        return export

    def enable_export_qos_bw(self,
                             cluster_id: str,
                             pseudo_path: str,
                             bw_obj: QOSBandwidthControl
                             ) -> None:
        """
        There are 2 cases to consider, based on QOS type set on cluster level
        1. If combined bandwith control is disabled
            a. If qos_type is pershare, then export_writebw and export_readbw parameters are compulsory
            b. If qos_type is perclient, then can't enable export level qos
            c. If qos_type is pershare_perclient then export_writebw, export_readbw, client_writebw and
               client_readbw are compulsory parameters
        2. If combined bandwidth control is enabled
            a. If qos_type is pershare, then export_rw_bw parameter is compulsory
            b. If qos_type is perclient, then can't enable export level qos
            c. If qos_type is pershare_perclient, then export_rw_bw and client_rw_bw parameters are compulsory
        """
        try:
            export_obj = self.get_export_obj(cluster_id, pseudo_path)
            export_qos_bw_checks(cluster_id, self.mgr, bw_obj=bw_obj)
            self.update_export_qos(cluster_id, pseudo_path, export_obj, True, bw_obj=bw_obj)
            log.debug(f"Successfully enabled QoS bandwidth control for export {pseudo_path} of cluster {cluster_id}")
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QoS bandwidth control failed for {pseudo_path} of {cluster_id}")
            raise ErrorResponse.wrap(e)

    def get_export_qos(self, cluster_id: str, pseudo_path: str, ret_bw_in_bytes: bool = False) -> Dict[str, int]:
        try:
            clust_qos_obj = get_cluster_qos_config(cluster_id, self.mgr)
            clust_qos_conf = {}
            if clust_qos_obj:
                clust_qos_conf[f'global_{QOSParams.enable_qos.value}'] = clust_qos_obj.enable_qos
                if clust_qos_obj.bw_obj:
                    clust_qos_conf[f'global_{QOSParams.enable_bw_ctrl.value}'] = clust_qos_obj.bw_obj.enable_bw_ctrl
                if clust_qos_obj.ops_obj:
                    clust_qos_conf[f'global_{QOSParams.enable_iops_ctrl.value}'] = clust_qos_obj.ops_obj.enable_iops_ctrl

            export_obj = self.get_export_obj(cluster_id, pseudo_path)
            if export_obj.qos_block:
                qos_block = export_obj.qos_block.to_dict(ret_bw_in_bytes)
                qos_block.update(clust_qos_conf)
                return qos_block
            return {}
        except Exception as e:
            log.exception(f"Failed to get QoS configuration for {pseudo_path} of {cluster_id}")
            raise ErrorResponse.wrap(e)

    def disable_export_qos_bw(self, cluster_id: str, pseudo_path: str) -> None:
        try:
            export_obj = self.get_export_obj(cluster_id, pseudo_path)
            if export_obj.qos_block:
                status = export_obj.qos_block.get_enable_qos_val(disable_bw=True)
            self.update_export_qos(cluster_id, pseudo_path, export_obj, status, bw_obj=QOSBandwidthControl())
            log.debug(f"Successfully disabled QoS bandwidth control for export {pseudo_path} of cluster {cluster_id}")
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QoS bandwidth control failed for {pseudo_path} of {cluster_id}")
            raise ErrorResponse.wrap(e)

    def enable_export_qos_ops(self, cluster_id: str, pseudo_path: str, ops_obj: QOSOpsControl) -> None:
        try:
            export_obj = self.get_export_obj(cluster_id, pseudo_path)
            export_qos_ops_checks(cluster_id, self.mgr, ops_obj=ops_obj)
            self.update_export_qos(cluster_id, pseudo_path, export_obj, True, ops_obj=ops_obj)
            log.debug(f"Successfully enabled QoS IOPS control for export {pseudo_path} of cluster {cluster_id}")
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QoS IOPS control failed for {pseudo_path} of {cluster_id}")
            raise ErrorResponse.wrap(e)

    def disable_export_qos_ops(self, cluster_id: str, pseudo_path: str) -> None:
        try:
            export_obj = self.get_export_obj(cluster_id, pseudo_path)
            if not export_obj:
                raise NFSObjectNotFound(f"Export {pseudo_path} not found in NFS cluster {cluster_id}")
            if export_obj.qos_block:
                status = export_obj.qos_block.get_enable_qos_val(disable_ops=True)
            self.update_export_qos(cluster_id, pseudo_path, export_obj, status, ops_obj=QOSOpsControl())
            log.debug(f"Successfully updated QoS IOPS control for export {pseudo_path} of cluster {cluster_id}")
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QoS IOPS control failed for {pseudo_path} of {cluster_id}")
            raise ErrorResponse.wrap(e)


def get_user_id(cluster_id: str, fs_name: str, cmount_path: str) -> str:
    """
    Generates a unique ID based on the input parameters using SHA-1.

    :param cluster_id: String representing the cluster ID.
    :param fs_name: String representing the file system name.
    :param cmount_path: String representing the complicated mount path.
    :return: A unique ID in the format 'cluster_id.fs_name.<hash>'.
    """
    input_string = f"{cluster_id}:{fs_name}:{cmount_path}"
    hash_hex = hashlib.sha1(input_string.encode('utf-8')).hexdigest()
    unique_id = f"{cluster_id}.{fs_name}.{hash_hex[:8]}"  # Use the first 8 characters of the hash

    return unique_id


def get_pool_user_id(cluster_id: str, fs_name: str, index: int) -> str:
    base = get_user_id(cluster_id, fs_name, '/')
    if index == 0:
        return f"nfs.{base}"
    return f"nfs.{base}.pool.{index}"
