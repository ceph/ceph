import errno
import json
import logging
import subprocess
from typing import List, Any, Dict, Tuple, Optional, TYPE_CHECKING, TypeVar, Callable, cast
from os.path import normpath

from rados import TimedOut, ObjectNotFound

from .export_utils import GaneshaConfParser, Export, RawBlock, CephFSFSAL, RGWFSAL
from .exception import NFSException, NFSInvalidOperation, FSNotFound, \
    ClusterNotFound
from .utils import POOL_NAME, available_clusters, check_fs, restart_nfs_service

if TYPE_CHECKING:
    from nfs.module import Module

FuncT = TypeVar('FuncT', bound=Callable)

log = logging.getLogger(__name__)


def export_cluster_checker(func: FuncT) -> FuncT:
    def cluster_check(
            export: 'ExportMgr',
            *args: Any,
            **kwargs: Any
    ) -> Tuple[int, str, str]:
        """
        This method checks if cluster exists
        """
        if kwargs['cluster_id'] not in available_clusters(export.mgr):
            return -errno.ENOENT, "", "Cluster does not exists"
        return func(export, *args, **kwargs)
    return cast(FuncT, cluster_check)


def exception_handler(
        exception_obj: Exception,
        log_msg: str = ""
) -> Tuple[int, str, str]:
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
    def __init__(
            self,
            mgr: 'Module',
            export_ls: Optional[Dict[str, List[Export]]] = None
    ) -> None:
        self.mgr = mgr
        self.rados_pool = POOL_NAME
        self._exports: Optional[Dict[str, List[Export]]] = export_ls

    @staticmethod
    def _check_rados_notify(ioctx: Any, obj: str) -> None:
        try:
            ioctx.notify(obj)
        except TimedOut:
            log.exception("Ganesha timed out")

    def _exec(self, args: List[str]) -> Tuple[int, str, str]:
        try:
            util = args.pop(0)
            cmd = [
                util,
                '-k', str(self.mgr.get_ceph_option('keyring')),
                '-n', f'mgr.{self.mgr.get_mgr_id()}',
            ] + args
            log.debug('exec: ' + ' '.join(cmd))
            p = subprocess.run(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=10,
            )
        except subprocess.CalledProcessError as ex:
            log.error(f'Error executing <<{ex.cmd}>>: {ex.output}')
        except subprocess.TimeoutExpired:
            log.error(f'timeout (10s) executing <<{cmd}>>')
        return p.returncode, p.stdout.decode(), p.stderr.decode()

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
            log.info(f'no exports for cluster {cluster_id}')
            return None

    def _delete_export_user(self, export: Export) -> None:
        if isinstance(export.fsal, CephFSFSAL):
            assert export.fsal.user_id
            self.mgr.check_mon_command({
                'prefix': 'auth rm',
                'entity': 'client.{}'.format(export.fsal.user_id),
            })
            log.info(f"Deleted export user {export.fsal.user_id}")
        elif isinstance(export.fsal, RGWFSAL):
            assert export.fsal.user_id
            uid = f'nfs.{export.cluster_id}.{export.path}'
            self._exec(['radosgw-admin', 'user', 'rm', '--uid', uid])
            log.info(f"Deleted export RGW user {uid}")

    def _create_export_user(self, export: Export) -> None:
        if isinstance(export.fsal, CephFSFSAL):
            fsal = cast(CephFSFSAL, export.fsal)
            assert fsal.fs_name

            # is top-level or any client rw?
            rw = export.access_type.lower() == 'rw'
            for c in export.clients:
                if c.access_type.lower() == 'rw':
                    rw = True
                    break

            fsal.user_id = f"nfs.{export.cluster_id}.{export.export_id}"
            fsal.cephx_key = self._create_user_key(
                export.cluster_id, fsal.user_id, export.path, fsal.fs_name, not rw
            )

        elif isinstance(export.fsal, RGWFSAL):
            rgwfsal = cast(RGWFSAL, export.fsal)
            rgwfsal.user_id = f'nfs.{export.cluster_id}.{export.path}'
            ret, out, err = self._exec(['radosgw-admin', 'user', 'info', '--uid',
                                        rgwfsal.user_id])
            if ret:
                ret, out, err = self._exec(['radosgw-admin', 'user', 'create',
                                            '--uid', rgwfsal.user_id,
                                            '--display-name', rgwfsal.user_id])
                if ret:
                    raise NFSException(f'Failed to create user {rgwfsal.user_id}')
            j = json.loads(out)
            # FIXME: make this more tolerate of unexpected output?
            rgwfsal.access_key_id = j['keys'][0]['access_key']
            rgwfsal.secret_access_key = j['keys'][0]['secret_key']

    def _gen_export_id(self, cluster_id: str) -> int:
        exports = sorted([ex.export_id for ex in self.exports[cluster_id]])
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

    def _save_export(self, cluster_id: str, export: Export) -> None:
        self.exports[cluster_id].append(export)
        NFSRados(self.mgr, cluster_id).write_obj(
            GaneshaConfParser.write_block(export.to_export_block()),
            f'export-{export.export_id}',
            f'conf-nfs.{export.cluster_id}'
        )

    def _delete_export(
            self,
            cluster_id: str,
            pseudo_path: Optional[str],
            export_obj: Optional[Export] = None
    ) -> Tuple[int, str, str]:
        try:
            if export_obj:
                export: Optional[Export] = export_obj
            else:
                assert pseudo_path
                export = self._fetch_export(cluster_id, pseudo_path)

            if export:
                if pseudo_path:
                    NFSRados(self.mgr, cluster_id).remove_obj(
                        f'export-{export.export_id}', f'conf-nfs.{cluster_id}')
                self.exports[cluster_id].remove(export)
                self._delete_export_user(export)
                if not self.exports[cluster_id]:
                    del self.exports[cluster_id]
                return 0, "Successfully deleted export", ""
            return 0, "", "Export does not exist"
        except Exception as e:
            return exception_handler(e, f"Failed to delete {pseudo_path} export for {cluster_id}")

    def _fetch_export_obj(self, cluster_id: str, ex_id: int) -> Optional[Export]:
        try:
            with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
                ioctx.set_namespace(cluster_id)
                export = Export.from_export_block(
                    GaneshaConfParser(
                        ioctx.read(f"export-{ex_id}").decode("utf-8")
                    ).parse()[0],
                    cluster_id
                )
                return export
        except ObjectNotFound:
            log.exception(f"Export ID: {ex_id} not found")
        return None

    def _update_export(self, cluster_id: str, export: Export) -> None:
        self.exports[cluster_id].append(export)
        NFSRados(self.mgr, cluster_id).update_obj(
            GaneshaConfParser.write_block(export.to_export_block()),
            f'export-{export.export_id}', f'conf-nfs.{export.cluster_id}')

    def format_path(self, path: str) -> str:
        if path:
            path = normpath(path.strip())
            if path[:2] == "//":
                path = path[1:]
        return path

    @export_cluster_checker
    def create_export(self, addr: Optional[List[str]] = None, **kwargs: Any) -> Tuple[int, str, str]:
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
            return exception_handler(e, f"Failed to create {kwargs['pseudo_path']} export for {kwargs['cluster_id']}")

    @export_cluster_checker
    def delete_export(self,
                      cluster_id: str,
                      pseudo_path: str) -> Tuple[int, str, str]:
        return self._delete_export(cluster_id, pseudo_path)

    def delete_all_exports(self, cluster_id: str) -> None:
        try:
            export_list = list(self.exports[cluster_id])
        except KeyError:
            log.info("No exports to delete")
            return
        for export in export_list:
            ret, out, err = self._delete_export(cluster_id=cluster_id, pseudo_path=None,
                                                export_obj=export)
            if ret != 0:
                raise NFSException(f"Failed to delete exports: {err} and {ret}")
        log.info(f"All exports successfully deleted for cluster id: {cluster_id}")

    @export_cluster_checker
    def list_exports(self,
                     cluster_id: str,
                     detailed: bool = False) -> Tuple[int, str, str]:
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
    def get_export(
            self,
            cluster_id: str,
            pseudo_path: str,
    ) -> Tuple[int, str, str]:
        try:
            export = self._fetch_export(cluster_id, pseudo_path)
            if export:
                return 0, json.dumps(export.to_dict(), indent=2), ''
            log.warning(f"No {pseudo_path} export to show for {cluster_id}")
            return 0, '', ''
        except Exception as e:
            return exception_handler(e, f"Failed to get {pseudo_path} export for {cluster_id}")

    def apply_export(self, cluster_id: str, export_config: str) -> Tuple[int, str, str]:
        try:
            if not export_config:
                raise NFSInvalidOperation("Empty Config!!")
            new_export = json.loads(export_config)
            # check export type
            return self._apply_export(cluster_id, new_export)
        except NotImplementedError:
            return 0, " Manual Restart of NFS PODS required for successful update of exports", ""
        except Exception as e:
            return exception_handler(e, f'Failed to update export: {e}')

    def _update_user_id(
            self,
            cluster_id: str,
            path: str,
            access_type: str,
            fs_name: str,
            user_id: str
    ) -> None:
        osd_cap = 'allow rw pool={} namespace={}, allow rw tag cephfs data={}'.format(
            self.rados_pool, cluster_id, fs_name)
        access_type = 'r' if access_type == 'RO' else 'rw'

        self.mgr.check_mon_command({
            'prefix': 'auth caps',
            'entity': f'client.{user_id}',
            'caps': ['mon', 'allow r', 'osd', osd_cap, 'mds', 'allow {} path={}'.format(
                access_type, path)],
        })

        log.info(f"Export user updated {user_id}")

    def _create_user_key(
            self,
            cluster_id: str,
            entity: str,
            path: str,
            fs_name: str,
            fs_ro: bool
    ) -> str:
        osd_cap = 'allow rw pool={} namespace={}, allow rw tag cephfs data={}'.format(
            self.rados_pool, cluster_id, fs_name)
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
        return json_res[0]['key']

    def create_export_from_dict(self,
                                cluster_id: str,
                                ex_id: int,
                                ex_dict: Dict[str, Any]) -> Export:
        pseudo_path = ex_dict.get("pseudo")
        if not pseudo_path:
            raise NFSInvalidOperation("export must specify pseudo path")

        path = ex_dict.get("path")
        if not path:
            raise NFSInvalidOperation("export must specify path")
        path = self.format_path(path)

        fsal = ex_dict.get("fsal", {})
        fsal_type = fsal.get("name")
        if fsal_type == 'RGW':
            if '/' in path:
                raise NFSInvalidOperation('"/" is not allowed in path (bucket name)')
            uid = f'nfs.{cluster_id}.{path}'
            if "user_id" in fsal and fsal["user_id"] != uid:
                raise NFSInvalidOperation(f"export FSAL user_id must be '{uid}'")
        elif fsal_type == 'CEPH':
            fs_name = fsal.get("fs_name")
            if not fs_name:
                raise NFSInvalidOperation("export FSAL must specify fs_name")
            if not check_fs(self.mgr, fs_name):
                raise FSNotFound(fs_name)

            user_id = f"nfs.{cluster_id}.{ex_id}"
            if "user_id" in fsal and fsal["user_id"] != user_id:
                raise NFSInvalidOperation(f"export FSAL user_id must be '{user_id}'")
        else:
            raise NFSInvalidOperation("export must specify FSAL name of 'CEPH' or 'RGW'")

        ex_dict["fsal"] = fsal
        ex_dict["cluster_id"] = cluster_id
        export = Export.from_dict(ex_id, ex_dict)
        export.validate(self.mgr)
        return export

    def create_cephfs_export(self,
                             fs_name: str,
                             cluster_id: str,
                             pseudo_path: str,
                             read_only: bool,
                             path: str,
                             squash: str,
                             access_type: str,
                             clients: list = []) -> Tuple[int, str, str]:
        pseudo_path = self.format_path(pseudo_path)

        if not self._fetch_export(cluster_id, pseudo_path):
            export = self.create_export_from_dict(
                cluster_id,
                self._gen_export_id(cluster_id),
                {
                    "pseudo": pseudo_path,
                    "path": path,
                    "access_type": access_type,
                    "squash": squash,
                    "fsal": {
                        "name": "CEPH",
                        "fs_name": fs_name,
                    },
                    "clients": clients,
                }
            )
            self._create_export_user(export)
            self._save_export(cluster_id, export)
            result = {
                "bind": export.pseudo,
                "fs": fs_name,
                "path": export.path,
                "cluster": cluster_id,
                "mode": export.access_type,
            }
            return (0, json.dumps(result, indent=4), '')
        return 0, "", "Export already exists"

    def create_rgw_export(self,
                          bucket: str,
                          cluster_id: str,
                          pseudo_path: str,
                          access_type: str,
                          read_only: bool,
                          squash: str,
                          realm: Optional[str] = None,
                          clients: list = []) -> Tuple[int, str, str]:
        pseudo_path = self.format_path(pseudo_path)

        if not self._fetch_export(cluster_id, pseudo_path):
            export = self.create_export_from_dict(
                cluster_id,
                self._gen_export_id(cluster_id),
                {
                    "pseudo": pseudo_path,
                    "path": bucket,
                    "access_type": access_type,
                    "squash": squash,
                    "fsal": {"name": "RGW"},
                    "clients": clients,
                }
            )
            self._create_export_user(export)
            self._save_export(cluster_id, export)
            result = {
                "bind": export.pseudo,
                "path": export.path,
                "cluster": cluster_id,
                "mode": export.access_type,
                "squash": export.squash,
            }
            return (0, json.dumps(result, indent=4), '')
        return 0, "", "Export already exists"

    def _apply_export(
            self,
            cluster_id: str,
            new_export_dict: Dict,
    ) -> Tuple[int, str, str]:
        for k in ['path', 'pseudo']:
            if k not in new_export_dict:
                raise NFSInvalidOperation(f'Export missing required field {k}')
        if cluster_id not in available_clusters(self.mgr):
            raise ClusterNotFound()
        if cluster_id not in self.exports:
            self.exports[cluster_id] = []

        new_export_dict['path'] = self.format_path(new_export_dict['path'])
        new_export_dict['pseudo'] = self.format_path(new_export_dict['pseudo'])

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
                self.mgr.log.debug(f"export {old_export.export_id} pseudo {old_export.pseudo} -> {new_export_dict['pseudo']}")

        new_export = self.create_export_from_dict(
            cluster_id,
            new_export_dict.get('export_id', self._gen_export_id(cluster_id)),
            new_export_dict
        )

        if not old_export:
            self._create_export_user(new_export)
            self._save_export(cluster_id, new_export)
            return 0, f'Added export {new_export.pseudo}', ''

        if old_export.fsal.name != new_export.fsal.name:
            raise NFSInvalidOperation('FSAL change not allowed')
        if old_export.pseudo != new_export.pseudo:
            self.mgr.log.debug(
                f'export {new_export.export_id} pseudo {old_export.pseudo} -> {new_export.pseudo}'
            )

        if old_export.fsal.name == 'CEPH':
            old_fsal = cast(CephFSFSAL, old_export.fsal)
            new_fsal = cast(CephFSFSAL, new_export.fsal)
            if old_fsal.user_id != new_fsal.user_id:
                self._delete_export_user(old_export)
                self._create_export_user(new_export)
            elif (
                old_export.path != new_export.path
                or old_fsal.fs_name != new_fsal.fs_name
            ):
                self._update_user_id(
                    cluster_id,
                    new_export.path,
                    new_export.access_type,
                    cast(str, new_fsal.fs_name),
                    cast(str, new_fsal.user_id)
                )
                new_fsal.cephx_key = old_fsal.cephx_key
            else:
                new_fsal.cephx_key = old_fsal.cephx_key
        if old_export.fsal.name == 'RGW':
            old_rgw_fsal = cast(RGWFSAL, old_export.fsal)
            new_rgw_fsal = cast(RGWFSAL, new_export.fsal)
            if old_rgw_fsal.user_id != new_rgw_fsal.user_id:
                self._delete_export_user(old_export)
                self._create_export_user(new_export)
            elif old_rgw_fsal.access_key_id != new_rgw_fsal.access_key_id:
                raise NFSInvalidOperation('access_key_id change is not allowed')
            elif old_rgw_fsal.secret_access_key != new_rgw_fsal.secret_access_key:
                raise NFSInvalidOperation('secret_access_key change is not allowed')

        self.exports[cluster_id].remove(old_export)
        self._update_export(cluster_id, new_export)

        # TODO: detect whether the update is such that a reload is sufficient
        restart_nfs_service(self.mgr, new_export.cluster_id)

        return 0, f"Updated export {new_export.pseudo}", ""
