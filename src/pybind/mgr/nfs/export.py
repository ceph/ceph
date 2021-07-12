import errno
import json
import logging
from typing import List
from os.path import isabs, normpath

from rados import TimedOut, ObjectNotFound

from .export_utils import GaneshaConfParser, Export
from .exception import NFSException, NFSInvalidOperation, NFSObjectNotFound, FSNotFound, \
        ClusterNotFound
from .utils import POOL_NAME, available_clusters, restart_nfs_service, check_fs

log = logging.getLogger(__name__)


def export_cluster_checker(func):
    def cluster_check(fs_export, *args, **kwargs):
        """
        This method checks if cluster exists and sets rados namespace.
        """
        if kwargs['cluster_id'] not in available_clusters(fs_export.mgr):
            return -errno.ENOENT, "", "Cluster does not exists"
        fs_export.rados_namespace = kwargs['cluster_id']
        return func(fs_export, *args, **kwargs)
    return cluster_check


def exception_handler(exception_obj, log_msg=""):
    if log_msg:
        log.exception(log_msg)
    return getattr(exception_obj, 'errno', -1), "", str(exception_obj)


class NFSRados:
    def __init__(self, mgr, namespace):
        self.mgr = mgr
        self.pool = POOL_NAME
        self.namespace = namespace

    def _make_rados_url(self, obj):
        return "rados://{}/{}/{}".format(self.pool, self.namespace, obj)

    def _create_url_block(self, obj_name):
        return {'block_name': '%url', 'value': self._make_rados_url(obj_name)}

    def write_obj(self, conf_block, obj, config_obj=''):
        if 'export-' in obj:
            conf_block = GaneshaConfParser.write_block(conf_block)

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

    def update_obj(self, conf_block, obj, config_obj):
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            ioctx.write_full(obj, conf_block.encode('utf-8'))
            log.debug("write configuration into rados object "
                      f"{self.pool}/{self.namespace}/{obj}:\n{conf_block}")
            ExportMgr._check_rados_notify(ioctx, config_obj)
            log.debug(f"Update export {obj} in {config_obj}")

    def remove_obj(self, obj, config_obj):
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            export_urls = ioctx.read(config_obj)
            url = '%url "{}"\n\n'.format(self._make_rados_url(obj))
            export_urls = export_urls.replace(url.encode('utf-8'), b'')
            ioctx.remove_object(obj)
            ioctx.write_full(config_obj, export_urls)
            ExportMgr._check_rados_notify(ioctx, config_obj)
            log.debug("Object deleted: {}".format(url))

    def remove_all_obj(self):
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            for obj in ioctx.list_objects():
                obj.remove()

    def check_user_config(self):
        with self.mgr.rados.open_ioctx(self.pool) as ioctx:
            ioctx.set_namespace(self.namespace)
            for obj in ioctx.list_objects():
                if obj.key.startswith("userconf-nfs"):
                    return True
        return False


class ValidateExport:
    @staticmethod
    def pseudo_path(path):
        if not isabs(path) or path == "/":
            raise NFSInvalidOperation(f"pseudo path {path} is invalid. It should be an absolute "
                                      "path and it cannot be just '/'.")

    @staticmethod
    def squash(squash):
        valid_squash_ls = ["root", "root_squash", "rootsquash", "rootid", "root_id_squash",
                           "rootidsquash", "all", "all_squash", "allsquash", "all_anomnymous",
                           "allanonymous", "no_root_squash", "none", "noidsquash"]
        if squash not in valid_squash_ls:
            raise NFSInvalidOperation(f"squash {squash} not in valid list {valid_squash_ls}")

    @staticmethod
    def security_label(label):
        if not isinstance(label, bool):
            raise NFSInvalidOperation('Only boolean values allowed')

    @staticmethod
    def protocols(proto):
        for p in proto:
            if p not in [3, 4]:
                raise NFSInvalidOperation(f"Invalid protocol {p}")
        if 3 in proto:
            log.warning("NFS V3 is an old version, it might not work")

    @staticmethod
    def transport(transport):
        valid_transport = ["UDP", "TCP"]
        for trans in transport:
            if trans.upper() not in valid_transport:
                raise NFSInvalidOperation(f'{trans} is not a valid transport protocol')

    @staticmethod
    def access_type(access_type):
        valid_ones = ['RW', 'RO']
        if access_type not in valid_ones:
            raise NFSInvalidOperation(f'{access_type} is invalid, valid access type are'
                                      f'{valid_ones}')

    @staticmethod
    def fsal(mgr, old, new):
        if old.name != new['name']:
            raise NFSInvalidOperation('FSAL name change not allowed')
        if old.user_id != new['user_id']:
            raise NFSInvalidOperation('User ID modification is not allowed')
        if new['sec_label_xattr']:
            raise NFSInvalidOperation('Security label xattr cannot be changed')
        if old.fs_name != new['fs_name']:
            if not check_fs(mgr, new['fs_name']):
                raise FSNotFound(new['fs_name'])
            return 1

    @staticmethod
    def _client(client):
        ValidateExport.access_type(client['access_type'])
        ValidateExport.squash(client['squash'])

    @staticmethod
    def clients(clients_ls):
        for client in clients_ls:
            ValidateExport._client(client)


class ExportMgr:
    def __init__(self, mgr, namespace=None, export_ls=None):
        self.mgr = mgr
        self.rados_pool = POOL_NAME
        self.rados_namespace = namespace
        self._exports = export_ls

    @staticmethod
    def _check_rados_notify(ioctx, obj):
        try:
            ioctx.notify(obj)
        except TimedOut:
            log.exception(f"Ganesha timed out")

    @property
    def exports(self):
        if self._exports is None:
            self._exports = {}
            log.info("Begin export parsing")
            for cluster_id in available_clusters(self.mgr):
                self.export_conf_objs = []  # type: List[Export]
                self._read_raw_config(cluster_id)
                self.exports[cluster_id] = self.export_conf_objs
                log.info(f"Exports parsed successfully {self.exports.items()}")
        return self._exports

    def _fetch_export(self, pseudo_path):
        try:
            for ex in self.exports[self.rados_namespace]:
                if ex.pseudo == pseudo_path:
                    return ex
        except KeyError:
            pass

    def _delete_user(self, entity):
        self.mgr.check_mon_command({
            'prefix': 'auth rm',
            'entity': 'client.{}'.format(entity),
            })
        log.info(f"Export user deleted is {entity}")

    def _gen_export_id(self):
        exports = sorted([ex.export_id for ex in self.exports[self.rados_namespace]])
        nid = 1
        for e_id in exports:
            if e_id == nid:
                nid += 1
            else:
                break
        return nid

    def _read_raw_config(self, rados_namespace):
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

    def _save_export(self, export):
        self.exports[self.rados_namespace].append(export)
        NFSRados(self.mgr, self.rados_namespace).write_obj(export.to_export_block(),
                 f'export-{export.export_id}', f'conf-nfs.{export.cluster_id}')

    def _delete_export(self, cluster_id, pseudo_path, export_obj=None):
        try:
            if export_obj:
                export = export_obj
            else:
                export = self._fetch_export(pseudo_path)

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

    def _fetch_export_obj(self, ex_id):
        try:
            with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
                ioctx.set_namespace(self.rados_namespace)
                export = Export.from_export_block(GaneshaConfParser(ioctx.read(f"export-{ex_id}"
                    ).decode("utf-8")).parse()[0], self.rados_namespace)
                return export
        except ObjectNotFound:
            log.exception(f"Export ID: {ex_id} not found")

    def _update_export(self, export):
        self.exports[self.rados_namespace].append(export)
        NFSRados(self.mgr, self.rados_namespace).update_obj(
                GaneshaConfParser.write_block(export.to_export_block()),
                f'export-{export.export_id}', f'conf-nfs.{export.cluster_id}')

    def format_path(self, path):
        if path:
            path = normpath(path.strip())
            if path[:2] == "//":
                path = path[1:]
        return path

    @export_cluster_checker
    def create_export(self, **kwargs):
        try:
            fsal_type = kwargs.pop('fsal_type')
            if fsal_type == 'cephfs':
                return FSExport(self).create_export(**kwargs)
            raise NotImplementedError()
        except Exception as e:
            return exception_handler(e, f"Failed to create {kwargs['pseudo_path']} export for {kwargs['cluster_id']}")

    @export_cluster_checker
    def delete_export(self, cluster_id, pseudo_path):
        return self._delete_export(cluster_id, pseudo_path)

    def delete_all_exports(self, cluster_id):
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
    def list_exports(self, cluster_id, detailed):
        try:
            if detailed:
                result = [export.to_dict() for export in self.exports[cluster_id]]
            else:
                result = [export.pseudo for export in self.exports[cluster_id]]
            return 0, json.dumps(result, indent=2), ''
        except KeyError:
            log.warning(f"No exports to list for {cluster_id}")
            return 0, '', ''
        except Exception as e:
            return exception_handler(e, f"Failed to list exports for {cluster_id}")

    @export_cluster_checker
    def get_export(self, cluster_id, pseudo_path):
        try:
            export = self._fetch_export(pseudo_path)
            if export:
                return 0, json.dumps(export.to_dict(), indent=2), ''
            log.warning(f"No {pseudo_path} export to show for {cluster_id}")
            return 0, '', ''
        except Exception as e:
            return exception_handler(e, f"Failed to get {pseudo_path} export for {cluster_id}")

    def update_export(self, export_config):
        try:
            new_export = json.loads(export_config)
            # check export type
            return FSExport(self).update_export(new_export)
        except NotImplementedError:
            return 0, " Manual Restart of NFS PODS required for successful update of exports", ""
        except Exception as e:
            return exception_handler(e, f'Failed to update export: {e}')


class FSExport(ExportMgr):
    def __init__(self, export_mgr_obj):
        super().__init__(export_mgr_obj.mgr, export_mgr_obj.rados_namespace,
                         export_mgr_obj._exports)

    def _validate_export(self, new_export_dict):
        if new_export_dict['cluster_id'] not in available_clusters(self.mgr):
            raise ClusterNotFound()

        export = self._fetch_export(new_export_dict['pseudo'])
        out_msg = ''
        if export:
            # Check if export id matches
            if export.export_id != new_export_dict['export_id']:
                raise NFSInvalidOperation('Export ID changed, Cannot update export')
        else:
            # Fetch export based on export id object
            export = self._fetch_export_obj(new_export_dict['export_id'])
            if not export:
                raise NFSObjectNotFound('Export does not exist')
            else:
                new_export_dict['pseudo'] = self.format_path(new_export_dict['pseudo'])
                ValidateExport.pseudo_path(new_export_dict['pseudo'])
                log.debug(f"Pseudo path has changed from {export.pseudo} to "
                          f"{new_export_dict['pseudo']}")
        # Check if squash changed
        if export.squash != new_export_dict['squash']:
            if new_export_dict['squash']:
                new_export_dict['squash'] = new_export_dict['squash'].lower()
                ValidateExport.squash(new_export_dict['squash'])
            log.debug(f"squash has changed from {export.squash} to {new_export_dict['squash']}")
        # Security label check
        if export.security_label != new_export_dict['security_label']:
            ValidateExport.security_label(new_export_dict['security_label'])
        # Protocol Checking
        if export.protocols != new_export_dict['protocols']:
            ValidateExport.protocols(new_export_dict['protocols'])
        # Transport checking
        if export.transports != new_export_dict['transports']:
            ValidateExport.transport(new_export_dict['transports'])
        # Path check
        if export.path != new_export_dict['path']:
            new_export_dict['path'] = self.format_path(new_export_dict['path'])
            out_msg = 'update caps'
        # Check Access Type
        if export.access_type != new_export_dict['access_type']:
            ValidateExport.access_type(new_export_dict['access_type'])
        # Fsal block check
        if export.fsal != new_export_dict['fsal']:
            ret = ValidateExport.fsal(self.mgr, export.fsal, new_export_dict['fsal'])
            if ret == 1 and not out_msg:
                out_msg = 'update caps'
        # Check client block
        if export.clients != new_export_dict['clients']:
            ValidateExport.clients(new_export_dict['clients'])
        log.debug(f'Validation succeeded for Export {export.pseudo}')
        return export, out_msg

    def _update_user_id(self, path, access_type, fs_name, user_id):
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

    def _create_user_key(self, entity, path, fs_name, fs_ro):
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

    def create_export(self, fs_name, cluster_id, pseudo_path, read_only, path):
        if not check_fs(self.mgr, fs_name):
            raise FSNotFound(fs_name)

        pseudo_path = self.format_path(pseudo_path)
        ValidateExport.pseudo_path(pseudo_path)

        if cluster_id not in self.exports:
            self.exports[cluster_id] = []

        if not self._fetch_export(pseudo_path):
            ex_id = self._gen_export_id()
            user_id = f"{cluster_id}{ex_id}"
            user_out, key = self._create_user_key(user_id, path, fs_name, read_only)
            access_type = "RW"
            if read_only:
                access_type = "RO"
            ex_dict = {
                    'path': self.format_path(path),
                    'pseudo': pseudo_path,
                    'cluster_id': cluster_id,
                    'access_type': access_type,
                    'fsal': {"name": "CEPH", "user_id": user_id,
                             "fs_name": fs_name, "sec_label_xattr": ""},
                    'clients': []
                    }
            export = Export.from_dict(ex_id, ex_dict)
            export.fsal.cephx_key = key
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

    def update_export(self, new_export):
        old_export, update_user_caps = self._validate_export(new_export)
        if update_user_caps:
            self._update_user_id(new_export['path'], new_export['access_type'],
                                 new_export['fsal']['fs_name'], new_export['fsal']['user_id'])
        new_export = Export.from_dict(new_export['export_id'], new_export)
        new_export.fsal.cephx_key = old_export.fsal.cephx_key
        self._update_export(new_export)
        export_ls = self.exports[self.rados_namespace]
        if old_export not in export_ls:
            # This happens when export is fetched by ID
            old_export = self._fetch_export(old_export.pseudo)
        export_ls.remove(old_export)
        restart_nfs_service(self.mgr, new_export.cluster_id)
        return 0, "Successfully updated export", ""
