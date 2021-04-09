import errno
import json
import logging
from typing import List
import socket
from os.path import isabs, normpath

from ceph.deployment.service_spec import NFSServiceSpec, PlacementSpec
from rados import TimedOut, ObjectNotFound

import orchestrator

from .export_utils import GaneshaConfParser, Export
from .exception import NFSException, NFSInvalidOperation, NFSObjectNotFound, FSNotFound, \
        ClusterNotFound

log = logging.getLogger(__name__)
POOL_NAME = 'nfs-ganesha'


def available_clusters(mgr):
    '''
    This method returns list of available cluster ids.
    Service name is service_type.service_id
    Example:
    completion.result value:
    <ServiceDescription of <NFSServiceSpec for service_name=nfs.vstart>>
    return value: ['vstart']
    '''
    # TODO check cephadm cluster list with rados pool conf objects
    completion = mgr.describe_service(service_type='nfs')
    orchestrator.raise_if_exception(completion)
    return [cluster.spec.service_id for cluster in completion.result
            if cluster.spec.service_id]


def restart_nfs_service(mgr, cluster_id):
    '''
    This methods restarts the nfs daemons
    '''
    completion = mgr.service_action(action='restart',
            service_name='nfs.'+cluster_id)
    orchestrator.raise_if_exception(completion)


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


def cluster_setter(func):
    def set_pool_ns_clusterid(nfs, *args, **kwargs):
        nfs._set_pool_namespace(kwargs['cluster_id'])
        nfs._set_cluster_id(kwargs['cluster_id'])
        return func(nfs, *args, **kwargs)
    return set_pool_ns_clusterid


def exception_handler(exception_obj, log_msg=""):
    if log_msg:
        log.exception(log_msg)
    return getattr(exception_obj, 'errno', -1), "", str(exception_obj)


def check_fs(mgr, fs_name):
    fs_map = mgr.get('fs_map')
    return fs_name in [fs['mdsmap']['fs_name'] for fs in fs_map['filesystems']]


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
            if not export_config:
                raise NFSInvalidOperation("Empty Config!!")
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


class NFSCluster:
    def __init__(self, mgr):
        self.pool_name = POOL_NAME
        self.pool_ns = ''
        self.mgr = mgr

    def _set_cluster_id(self, cluster_id):
        self.cluster_id = cluster_id

    def _set_pool_namespace(self, cluster_id):
        self.pool_ns = cluster_id

    def _get_common_conf_obj_name(self):
        return f'conf-nfs.{self.cluster_id}'

    def _get_user_conf_obj_name(self):
        return f'userconf-nfs.{self.cluster_id}'

    def _call_orch_apply_nfs(self, placement):
        spec = NFSServiceSpec(service_type='nfs', service_id=self.cluster_id,
                              pool=self.pool_name, namespace=self.pool_ns,
                              placement=PlacementSpec.from_string(placement))
        completion = self.mgr.apply_nfs(spec)
        orchestrator.raise_if_exception(completion)

    def create_empty_rados_obj(self):
        common_conf = self._get_common_conf_obj_name()
        NFSRados(self.mgr, self.pool_ns).write_obj('', self._get_common_conf_obj_name())
        log.info(f"Created empty object:{common_conf}")

    def delete_config_obj(self):
        NFSRados(self.mgr, self.pool_ns).remove_all_obj()
        log.info(f"Deleted {self._get_common_conf_obj_name()} object and all objects in "
                 f"{self.pool_ns}")

    @cluster_setter
    def create_nfs_cluster(self, cluster_id, placement):
        try:
            pool_list = [p['pool_name'] for p in self.mgr.get_osdmap().dump().get('pools', [])]

            if self.pool_name not in pool_list:
                self.mgr.check_mon_command({'prefix': 'osd pool create', 'pool': self.pool_name})
                self.mgr.check_mon_command({'prefix': 'osd pool application enable',
                                            'pool': self.pool_name, 'app': 'nfs'})

            self.create_empty_rados_obj()

            if cluster_id not in available_clusters(self.mgr):
                self._call_orch_apply_nfs(placement)
                return 0, "NFS Cluster Created Successfully", ""
            return 0, "", f"{cluster_id} cluster already exists"
        except Exception as e:
            return exception_handler(e, f"NFS Cluster {cluster_id} could not be created")

    @cluster_setter
    def update_nfs_cluster(self, cluster_id, placement):
        try:
            if cluster_id in available_clusters(self.mgr):
                self._call_orch_apply_nfs(placement)
                return 0, "NFS Cluster Updated Successfully", ""
            raise ClusterNotFound()
        except Exception as e:
            return exception_handler(e, f"NFS Cluster {cluster_id} could not be updated")

    @cluster_setter
    def delete_nfs_cluster(self, cluster_id):
        try:
            cluster_list = available_clusters(self.mgr)
            if cluster_id in cluster_list:
                self.mgr.fs_export.delete_all_exports(cluster_id)
                completion = self.mgr.remove_service('nfs.' + self.cluster_id)
                orchestrator.raise_if_exception(completion)
                self.delete_config_obj()
                return 0, "NFS Cluster Deleted Successfully", ""
            return 0, "", "Cluster does not exist"
        except Exception as e:
            return exception_handler(e, f"Failed to delete NFS Cluster {cluster_id}")

    def list_nfs_cluster(self):
        try:
            return 0, '\n'.join(available_clusters(self.mgr)), ""
        except Exception as e:
            return exception_handler(e, "Failed to list NFS Cluster")

    def _show_nfs_cluster_info(self, cluster_id):
        self._set_cluster_id(cluster_id)
        completion = self.mgr.list_daemons(daemon_type='nfs')
        orchestrator.raise_if_exception(completion)
        host_ip = []
        # Here completion.result is a list DaemonDescription objects
        for cluster in completion.result:
            if self.cluster_id == cluster.service_id():
                """
                getaddrinfo sample output: [(<AddressFamily.AF_INET: 2>,
                <SocketKind.SOCK_STREAM: 1>, 6, 'xyz', ('172.217.166.98',2049)),
                (<AddressFamily.AF_INET6: 10>, <SocketKind.SOCK_STREAM: 1>, 6, '',
                ('2404:6800:4009:80d::200e', 2049, 0, 0))]
                """
                try:
                    host_ip.append({
                            "hostname": cluster.hostname,
                            "ip": list(set([ip[4][0] for ip in socket.getaddrinfo(
                                cluster.hostname, 2049, flags=socket.AI_CANONNAME,
                                type=socket.SOCK_STREAM)])),
                            "port": 2049  # Default ganesha port
                            })
                except socket.gaierror:
                    continue
        return host_ip

    def show_nfs_cluster_info(self, cluster_id=None):
        try:
            cluster_ls = []
            info_res = {}
            if cluster_id:
                cluster_ls = [cluster_id]
            else:
                cluster_ls = available_clusters(self.mgr)

            for cluster_id in cluster_ls:
                res = self._show_nfs_cluster_info(cluster_id)
                if res:
                    info_res[cluster_id] = res
            return (0, json.dumps(info_res, indent=4), '')
        except Exception as e:
            return exception_handler(e, "Failed to show info for cluster")

    @cluster_setter
    def set_nfs_cluster_config(self, cluster_id, nfs_config):
        try:
            if not nfs_config:
                raise NFSInvalidOperation("Empty Config!!")
            if cluster_id in available_clusters(self.mgr):
                rados_obj = NFSRados(self.mgr, self.pool_ns)
                if rados_obj.check_user_config():
                    return 0, "", "NFS-Ganesha User Config already exists"
                rados_obj.write_obj(nfs_config, self._get_user_conf_obj_name(),
                                    self._get_common_conf_obj_name())
                restart_nfs_service(self.mgr, cluster_id)
                return 0, "NFS-Ganesha Config Set Successfully", ""
            raise ClusterNotFound()
        except NotImplementedError:
            return 0, "NFS-Ganesha Config Added Successfully (Manual Restart of NFS PODS required)", ""
        except Exception as e:
            return exception_handler(e, f"Setting NFS-Ganesha Config failed for {cluster_id}")

    @cluster_setter
    def reset_nfs_cluster_config(self, cluster_id):
        try:
            if cluster_id in available_clusters(self.mgr):
                rados_obj = NFSRados(self.mgr, self.pool_ns)
                if not rados_obj.check_user_config():
                    return 0, "", "NFS-Ganesha User Config does not exist"
                rados_obj.remove_obj(self._get_user_conf_obj_name(),
                                     self._get_common_conf_obj_name())
                restart_nfs_service(self.mgr, cluster_id)
                return 0, "NFS-Ganesha Config Reset Successfully", ""
            raise ClusterNotFound()
        except NotImplementedError:
            return 0, "NFS-Ganesha Config Removed Successfully (Manual Restart of NFS PODS required)", ""
        except Exception as e:
            return exception_handler(e, f"Resetting NFS-Ganesha Config failed for {cluster_id}")
