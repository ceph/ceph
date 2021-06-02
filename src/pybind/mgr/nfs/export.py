import errno
import json
import logging
from typing import List, Any, Dict, Tuple, Optional, TYPE_CHECKING, TypeVar, Callable, cast
from os.path import normpath

from rados import TimedOut, ObjectNotFound

from .export_utils import GaneshaConfParser, Export, RawBlock
from .exception import NFSException, NFSInvalidOperation, NFSObjectNotFound, FSNotFound, \
    ClusterNotFound
from .utils import POOL_NAME, available_clusters, check_fs

if TYPE_CHECKING:
    from nfs.module import Module

FuncT = TypeVar('FuncT', bound=Callable)

log = logging.getLogger(__name__)


def export_cluster_checker(func: FuncT) -> FuncT:
    def cluster_check(fs_export: 'ExportMgr', *args: Any, **kwargs: Any) -> Tuple[int, str, str]:
        """
        This method checks if cluster exists and sets rados namespace.
        """
        if kwargs['cluster_id'] not in available_clusters(fs_export.mgr):
            return -errno.ENOENT, "", "Cluster does not exists"
        fs_export.rados_namespace = kwargs['cluster_id']
        return func(fs_export, *args, **kwargs)
    return cast(FuncT, cluster_check)


def exception_handler(exception_obj: Exception, log_msg: str = "") -> Tuple[int, str, str]:
    if log_msg:
        log.exception(log_msg)
    return getattr(exception_obj, 'errno', -1), "", str(exception_obj)


class NFSRados:
    def __init__(self, mgr: 'Module', namespace: str) -> None:
        self.mgr = mgr
        self.pool = POOL_NAME
        self.namespace = namespace

    def _make_rados_url(self, obj: str) -> str:
        return "rados://{}/{}/{}".format(self.pool, self.namespace, obj)

    def _create_url_block(self, obj_name: str) -> RawBlock:
        return RawBlock('%url', values={'value': self._make_rados_url(obj_name)})

    def write_obj(self, conf_block: str, obj: str, config_obj: str = '') -> None:
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            ioctx.write_full(obj, conf_block.encode('utf-8'))
            if not config_obj:
                # Return after creating empty common config object
                return
            log.debug("write configuration into rados object "
                      f"{self.pool}/{self.namespace}/{obj}:\n{conf_block}")

            # Add created obj url to common config obj
            ioctx.append(config_obj, GaneshaConfParser.write_block(
                         self._create_url_block(obj)).encode('utf-8'))
            ExportMgr._check_rados_notify(ioctx, config_obj)
            log.debug(f"Added {obj} url to {config_obj}")

    def update_obj(self, conf_block: str, obj: str, config_obj: str) -> None:
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            ioctx.write_full(obj, conf_block.encode('utf-8'))
            log.debug("write configuration into rados object "
                      f"{self.pool}/{self.namespace}/{obj}:\n{conf_block}")
            ExportMgr._check_rados_notify(ioctx, config_obj)
            log.debug(f"Update export {obj} in {config_obj}")

    def remove_obj(self, obj: str, config_obj: str) -> None:
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            export_urls = ioctx.read(config_obj)
            url = '%url "{}"\n\n'.format(self._make_rados_url(obj))
            export_urls = export_urls.replace(url.encode('utf-8'), b'')
            ioctx.remove_object(obj)
            ioctx.write_full(config_obj, export_urls)
            ExportMgr._check_rados_notify(ioctx, config_obj)
            log.debug("Object deleted: {}".format(url))

    def remove_all_obj(self) -> None:
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            for obj in ioctx.list_objects():
                obj.remove()

    def check_user_config(self) -> bool:
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            for obj in ioctx.list_objects():
                if obj.key.startswith("userconf-nfs"):
                    return True
        return False


class ExportMgr:
    def __init__(self, mgr: 'Module', namespace: Optional[str] = None, export_ls: Optional[Dict[str, List[Export]]] = None) -> None:
        self.mgr = mgr
        self.rados_pool = POOL_NAME
        self.rados_namespace = namespace
        self._exports: Optional[Dict[str, List[Export]]] = export_ls

    @staticmethod
    def _check_rados_notify(ioctx: Any, obj: str) -> None:
        try:
            ioctx.notify(obj)
        except TimedOut:
            log.exception("Ganesha timed out")

    @property
    def exports(self) -> Dict[str, List[Export]]:
        if self._exports is None:
            self._exports = {}
            log.info("Begin export parsing")
            for cluster_id in available_clusters(self.mgr):
                self.export_conf_objs = []  # type: List[Export]
                self._read_raw_config(cluster_id)
                self.exports[cluster_id] = self.export_conf_objs
                log.info(f"Exports parsed successfully {self.exports.items()}")
        return self._exports

    def _fetch_export(self, cluster_id: str, pseudo_path: Optional[str]) -> Optional[Export]:
        try:
            for ex in self.exports[cluster_id]:
                if ex.pseudo == pseudo_path:
                    return ex
            return None
        except KeyError:
            log.info(f'unable to fetch f{cluster_id}')
            return None

    def _delete_user(self, entity: str) -> None:
        self.mgr.check_mon_command({
            'prefix': 'auth rm',
            'entity': 'client.{}'.format(entity),
        })
        log.info(f"Export user deleted is {entity}")

    def _gen_export_id(self) -> int:
        assert self.rados_namespace
        exports = sorted([ex.export_id for ex in self.exports[self.rados_namespace]])
        nid = 1
        for e_id in exports:
            if e_id == nid:
                nid += 1
            else:
                break
        return nid

    def _read_raw_config(self, rados_namespace: str) -> None:
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            ioctx.set_namespace(rados_namespace)
            for obj in ioctx.list_objects():
                if obj.key.startswith("export-"):
                    size, _ = obj.stat()
                    raw_config = obj.read(size)
                    raw_config = raw_config.decode("utf-8")
                    log.debug("read export configuration from rados "
                              "object %s/%s/%s:\n%s", self.rados_pool,
                              rados_namespace, obj.key, raw_config)
                    self.export_conf_objs.append(Export.from_export_block(
                        GaneshaConfParser(raw_config).parse()[0], rados_namespace))

    def _save_export(self, export: Export) -> None:
        assert self.rados_namespace
        self.exports[self.rados_namespace].append(export)
        NFSRados(self.mgr, self.rados_namespace).write_obj(
            GaneshaConfParser.write_block(export.to_export_block()),
            f'export-{export.export_id}',
            f'conf-nfs.{export.cluster_id}'
        )

    def _delete_export(self, cluster_id: str, pseudo_path: Optional[str], export_obj: Optional[Any] = None) -> Tuple[int, str, str]:
        assert self.rados_namespace
        try:
            if export_obj:
                export = export_obj
            else:
                export = self._fetch_export(cluster_id, pseudo_path)

            if export:
                if pseudo_path:
                    NFSRados(self.mgr, self.rados_namespace).remove_obj(
                        f'export-{export.export_id}', f'conf-nfs.{cluster_id}')
                self.exports[cluster_id].remove(export)
                self._delete_user(export.fsal.user_id)
                if not self.exports[cluster_id]:
                    del self.exports[cluster_id]
                return 0, "Successfully deleted export", ""
            return 0, "", "Export does not exist"
        except Exception as e:
            return exception_handler(e, f"Failed to delete {pseudo_path} export for {cluster_id}")

    def _fetch_export_obj(self, ex_id: int) -> Optional[Export]:
        assert self.rados_namespace
        try:
            with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
                ioctx.set_namespace(self.rados_namespace)
                export = Export.from_export_block(GaneshaConfParser(ioctx.read(f"export-{ex_id}"
                                                                               ).decode("utf-8")).parse()[0], self.rados_namespace)
                return export
        except ObjectNotFound:
            log.exception(f"Export ID: {ex_id} not found")
        return None

    def _update_export(self, export: Export) -> None:
        assert self.rados_namespace
        self.exports[self.rados_namespace].append(export)
        NFSRados(self.mgr, self.rados_namespace).update_obj(
            GaneshaConfParser.write_block(export.to_export_block()),
            f'export-{export.export_id}', f'conf-nfs.{export.cluster_id}')

    def format_path(self, path: str) -> str:
        if path:
            path = normpath(path.strip())
            if path[:2] == "//":
                path = path[1:]
        return path

    @export_cluster_checker
    def create_export(self, cluster_id: str, **kwargs: Any) -> Tuple[int, str, str]:
        try:
            fsal_type = kwargs.pop('fsal_type')
            if fsal_type == 'cephfs':
                return FSExport(self).create_cephfs_export(**kwargs)
            if fsal_type == 'rgw':
                return FSExport(self).create_rgw_export(**kwargs)
            raise NotImplementedError()
        except Exception as e:
            return exception_handler(e, f"Failed to create {kwargs['pseudo_path']} export for {kwargs['cluster_id']}")

    @export_cluster_checker
    def delete_export(self, cluster_id: str, pseudo_path: str) -> Tuple[int, str, str]:
        return self._delete_export(cluster_id, pseudo_path)

    def delete_all_exports(self, cluster_id: str) -> None:
        try:
            export_list = list(self.exports[cluster_id])
        except KeyError:
            log.info("No exports to delete")
            return
        self.rados_namespace = cluster_id
        for export in export_list:
            ret, out, err = self._delete_export(cluster_id=cluster_id, pseudo_path=None,
                                                export_obj=export)
            if ret != 0:
                raise NFSException(-1, f"Failed to delete exports: {err} and {ret}")
        log.info(f"All exports successfully deleted for cluster id: {cluster_id}")

    @export_cluster_checker
    def list_exports(self, cluster_id: str, detailed: bool) -> Tuple[int, str, str]:
        try:
            if detailed:
                result_d = [export.to_dict() for export in self.exports[cluster_id]]
                return 0, json.dumps(result_d, indent=2), ''
            else:
                result_ps = [export.pseudo for export in self.exports[cluster_id]]
                return 0, json.dumps(result_ps, indent=2), ''

        except KeyError:
            log.warning(f"No exports to list for {cluster_id}")
            return 0, '', ''
        except Exception as e:
            return exception_handler(e, f"Failed to list exports for {cluster_id}")

    @export_cluster_checker
    def get_export(self, cluster_id: str, pseudo_path: Optional[str]) -> Tuple[int, str, str]:
        try:
            export = self._fetch_export(cluster_id, pseudo_path)
            if export:
                return 0, json.dumps(export.to_dict(), indent=2), ''
            log.warning(f"No {pseudo_path} export to show for {cluster_id}")
            return 0, '', ''
        except Exception as e:
            return exception_handler(e, f"Failed to get {pseudo_path} export for {cluster_id}")

    def update_export(self, export_config: str) -> Tuple[int, str, str]:
        try:
            new_export = json.loads(export_config)
            # check export type
            return FSExport(self).update_export_1(new_export, False)
        except NotImplementedError:
            return 0, " Manual Restart of NFS PODS required for successful update of exports", ""
        except Exception as e:
            return exception_handler(e, f'Failed to update export: {e}')

    def import_export(self, export_config: str) -> Tuple[int, str, str]:
        try:
            if not export_config:
                raise NFSInvalidOperation("Empty Config!!")
            new_export = json.loads(export_config)
            # check export type
            return FSExport(self).update_export_1(new_export, True)
        except NotImplementedError:
            return 0, " Manual Restart of NFS PODS required for successful update of exports", ""
        except Exception as e:
            return exception_handler(e, f'Failed to import export: {e}')


class FSExport(ExportMgr):
    def __init__(self, export_mgr_obj: 'ExportMgr') -> None:
        super().__init__(export_mgr_obj.mgr, export_mgr_obj.rados_namespace,
                         export_mgr_obj._exports)

    def _update_user_id(self, path: str, access_type: str, fs_name: str, user_id: str) -> None:
        osd_cap = 'allow rw pool={} namespace={}, allow rw tag cephfs data={}'.format(
            self.rados_pool, self.rados_namespace, fs_name)
        access_type = 'r' if access_type == 'RO' else 'rw'

        self.mgr.check_mon_command({
            'prefix': 'auth caps',
            'entity': f'client.{user_id}',
            'caps': ['mon', 'allow r', 'osd', osd_cap, 'mds', 'allow {} path={}'.format(
                access_type, path)],
        })

        log.info(f"Export user updated {user_id}")

    def _create_user_key(self, entity: str, path: str, fs_name: str, fs_ro: bool) -> Tuple[str, str]:
        osd_cap = 'allow rw pool={} namespace={}, allow rw tag cephfs data={}'.format(
            self.rados_pool, self.rados_namespace, fs_name)
        access_type = 'r' if fs_ro else 'rw'

        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.{}'.format(entity),
            'caps': ['mon', 'allow r', 'osd', osd_cap, 'mds', 'allow {} path={}'.format(
                access_type, path)],
            'format': 'json',
        })

        json_res = json.loads(out)
        log.info("Export user created is {}".format(json_res[0]['entity']))
        return json_res[0]['entity'], json_res[0]['key']

    def create_cephfs_export(self,
                             fs_name: str,
                             cluster_id: str,
                             pseudo_path: str,
                             read_only: bool,
                             path: str,
                             squash: str,
                             clients: list = []) -> Tuple[int, str, str]:
        if not check_fs(self.mgr, fs_name):
            raise FSNotFound(fs_name)

        pseudo_path = self.format_path(pseudo_path)

        if cluster_id not in self.exports:
            self.exports[cluster_id] = []

        if not self._fetch_export(cluster_id, pseudo_path):
            ex_id = self._gen_export_id()
            user_id = f"nfs.{cluster_id}.{ex_id}"
            user_out, key = self._create_user_key(user_id, path, fs_name, read_only)
            if clients:
                access_type = "none"
            elif read_only:
                access_type = "RO"
            else:
                access_type = "RW"
            ex_dict = {
                'path': self.format_path(path),
                'pseudo': pseudo_path,
                'cluster_id': cluster_id,
                'access_type': access_type,
                'squash': squash,
                'fsal': {"name": "CEPH", "user_id": user_id,
                         "fs_name": fs_name, "sec_label_xattr": "",
                         "cephx_key": key},
                'clients': clients
            }
            export = Export.from_dict(ex_id, ex_dict)
            self._save_export(export)
            result = {
                "bind": pseudo_path,
                "fs": fs_name,
                "path": path,
                "cluster": cluster_id,
                "mode": access_type,
            }
            return (0, json.dumps(result, indent=4), '')
        return 0, "", "Export already exists"

    def create_rgw_export(self,
                          bucket: str,
                          cluster_id: str,
                          path: str,
                          pseudo_path: str,
                          read_only: bool,
                          squash: str,
                          clients: list = []) -> Tuple[int, str, str]:
        pseudo_path = self.format_path(pseudo_path)

        if cluster_id not in self.exports:
            self.exports[cluster_id] = []

        if not self._fetch_export(cluster_id, pseudo_path):
            # generate access+secret keys

            ex_id = self._gen_export_id()
            if clients:
                access_type = "none"
            elif read_only:
                access_type = "RO"
            else:
                access_type = "RW"
            ex_dict = {
                'path': self.format_path(path),
                'pseudo': pseudo_path,
                'cluster_id': cluster_id,
                'access_type': access_type,
                'squash': squash,
                'fsal': {
                    "name": "RGW",
                    # "user_id": user_id,
                    # "access_key_id": access_key_id,
                    # "secret_access_key": secret_access_key,
                },
                'clients': clients
            }
            export = Export.from_dict(ex_id, ex_dict)
            self._save_export(export)
            result = {
                "bind": pseudo_path,
                "path": path,
                "cluster": cluster_id,
                "mode": access_type,
            }
            return (0, json.dumps(result, indent=4), '')
        return 0, "", "Export already exists"

    def update_export_1(self, new_export: Dict, can_create: bool) -> Tuple[int, str, str]:

        for k in ['cluster_id', 'path', 'pseudo']:
            if k not in new_export:
                raise NFSInvalidOperation(f'Export missing required field {k}')
        if new_export['cluster_id'] not in available_clusters(self.mgr):
            raise ClusterNotFound()
        self.rados_namespace = new_export['cluster_id']
        assert self.rados_namespace

        new_export['path'] = self.format_path(new_export['path'])
        new_export['pseudo'] = self.format_path(new_export['pseudo'])

        old_export = self._fetch_export(new_export['cluster_id'],
                                        new_export['pseudo'])
        if old_export:
            # Check if export id matches
            if old_export.export_id != new_export.get('export_id'):
                raise NFSInvalidOperation('Export ID changed, Cannot update export')
        elif new_export.get('export_id'):
            old_export = self._fetch_export_obj(new_export['export_id'])

        if not old_export and not can_create:
            raise NFSObjectNotFound('Export does not exist')

        new_export = Export.from_dict(
            new_export.get('export_id', self._gen_export_id()),
            new_export
        )
        new_export.validate(self.mgr)

        if not old_export:
            self._save_export(new_export)
            return 0, f'Added export {new_export.pseudo}', ''

        if old_export.fsal.name != new_export.fsal.name:
            raise NFSInvalidOperation('FSAL change not allowed')
        """
        if old_export.fsal.user_id != new_export.fsal.user_id:
            raise NFSInvalidOperation('user_id change is not allowed')

        if old_export.fsal.name == 'CEPH':
            if (
                old_export_path != new_export_path
                or cast(old_export.fsal, CephFSFSAL).fs_name != cast(new_export.fsal, CephFSFSAL).fs_name
            ):
                self._update_user_id(new_export.path, new_export.access_type,
                                     new_export.fsal.fs_name, new_export.fsal.user_id)
            cast(new_export.fsal, CephFSFSAL).cephx_key = cast(old_export.fsal, CephFSFSAL).cephx_key
        self._update_export(new_export)
        export_ls = self.exports[self.rados_namespace]
        if old_export not in export_ls:
            # This happens when export is fetched by ID
            old_export = self._fetch_export(old_export.cluster_id, old_export.pseudo)
        export_ls.remove(old_export)
        restart_nfs_service(self.mgr, new_export.cluster_id)
        """
        return 0, f"Updated export {new_export.pseudo}", ""
