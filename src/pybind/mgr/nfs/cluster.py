import logging
import socket
import json
import re

from ceph.deployment.service_spec import NFSServiceSpec, PlacementSpec

import orchestrator

from .exception import NFSInvalidOperation, ClusterNotFound
from .utils import POOL_NAME, available_clusters, restart_nfs_service
from .export import NFSRados, exception_handler

log = logging.getLogger(__name__)


def cluster_setter(func):
    def set_pool_ns_clusterid(nfs, *args, **kwargs):
        nfs._set_pool_namespace(kwargs['cluster_id'])
        nfs._set_cluster_id(kwargs['cluster_id'])
        return func(nfs, *args, **kwargs)
    return set_pool_ns_clusterid


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
            invalid_str = re.search('[^A-Za-z0-9-_.]', cluster_id)
            if invalid_str:
                raise NFSInvalidOperation(f"cluster id {cluster_id} is invalid. "
                                          f"{invalid_str.group()} is char not permitted")

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
                self.mgr.export_mgr.delete_all_exports(cluster_id)
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
            return 0, "NFS-Ganesha Config Added Successfully "\
                    "(Manual Restart of NFS PODS required)", ""
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
            return 0, "NFS-Ganesha Config Removed Successfully "\
                    "(Manual Restart of NFS PODS required)", ""
        except Exception as e:
            return exception_handler(e, f"Resetting NFS-Ganesha Config failed for {cluster_id}")
