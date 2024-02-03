import ipaddress
import logging
import re
import socket
from typing import cast, Dict, List, Any, Union, Optional, TYPE_CHECKING

from mgr_module import NFS_POOL_NAME as POOL_NAME
from ceph.deployment.service_spec import NFSServiceSpec, PlacementSpec, IngressSpec
from object_format import ErrorResponse

import orchestrator
from orchestrator.module import IngressType

from .exception import NFSInvalidOperation, ClusterNotFound
from .utils import (
    ManualRestartRequired,
    NonFatalError,
    available_clusters,
    conf_obj_name,
    restart_nfs_service,
    user_conf_obj_name)
from .export import NFSRados

if TYPE_CHECKING:
    from nfs.module import Module
    from mgr_module import MgrModule


log = logging.getLogger(__name__)


def resolve_ip(hostname: str) -> str:
    try:
        r = socket.getaddrinfo(hostname, None, flags=socket.AI_CANONNAME,
                               type=socket.SOCK_STREAM)
        # pick first v4 IP, if present
        for a in r:
            if a[0] == socket.AF_INET:
                return a[4][0]
        return r[0][4][0]
    except socket.gaierror as e:
        raise NFSInvalidOperation(f"Cannot resolve IP for host {hostname}: {e}")


def create_ganesha_pool(mgr: 'MgrModule') -> None:
    pool_list = [p['pool_name'] for p in mgr.get_osdmap().dump().get('pools', [])]
    if POOL_NAME not in pool_list:
        mgr.check_mon_command({'prefix': 'osd pool create',
                               'pool': POOL_NAME,
                               'yes_i_really_mean_it': True})
        mgr.check_mon_command({'prefix': 'osd pool application enable',
                               'pool': POOL_NAME,
                               'app': 'nfs'})
        log.debug("Successfully created nfs-ganesha pool %s", POOL_NAME)


class NFSCluster:
    def __init__(self, mgr: 'Module') -> None:
        self.mgr = mgr

    def _call_orch_apply_nfs(
            self,
            cluster_id: str,
            placement: Optional[str] = None,
            virtual_ip: Optional[str] = None,
            ingress_mode: Optional[IngressType] = None,
            port: Optional[int] = None,
    ) -> None:
        if not port:
            port = 2049   # default nfs port
        if virtual_ip:
            # nfs + ingress
            # run NFS on non-standard port
            if not ingress_mode:
                ingress_mode = IngressType.default
            ingress_mode = ingress_mode.canonicalize()
            pspec = PlacementSpec.from_string(placement)
            if ingress_mode == IngressType.keepalive_only:
                # enforce count=1 for nfs over keepalive only
                pspec.count = 1

            ganesha_port = 10000 + port  # semi-arbitrary, fix me someday
            frontend_port: Optional[int] = port
            virtual_ip_for_ganesha: Optional[str] = None
            keepalive_only: bool = False
            enable_haproxy_protocol: bool = False
            if ingress_mode == IngressType.haproxy_protocol:
                enable_haproxy_protocol = True
            elif ingress_mode == IngressType.keepalive_only:
                keepalive_only = True
                virtual_ip_for_ganesha = virtual_ip.split('/')[0]
                ganesha_port = port
                frontend_port = None

            spec = NFSServiceSpec(service_type='nfs', service_id=cluster_id,
                                  placement=pspec,
                                  # use non-default port so we don't conflict with ingress
                                  port=ganesha_port,
                                  virtual_ip=virtual_ip_for_ganesha,
                                  enable_haproxy_protocol=enable_haproxy_protocol)
            completion = self.mgr.apply_nfs(spec)
            orchestrator.raise_if_exception(completion)
            ispec = IngressSpec(service_type='ingress',
                                service_id='nfs.' + cluster_id,
                                backend_service='nfs.' + cluster_id,
                                placement=pspec,
                                frontend_port=frontend_port,
                                monitor_port=7000 + port,   # semi-arbitrary, fix me someday
                                virtual_ip=virtual_ip,
                                keepalive_only=keepalive_only,
                                enable_haproxy_protocol=enable_haproxy_protocol)
            completion = self.mgr.apply_ingress(ispec)
            orchestrator.raise_if_exception(completion)
        else:
            # standalone nfs
            spec = NFSServiceSpec(service_type='nfs', service_id=cluster_id,
                                  placement=PlacementSpec.from_string(placement),
                                  port=port)
            completion = self.mgr.apply_nfs(spec)
            orchestrator.raise_if_exception(completion)
        log.debug("Successfully deployed nfs daemons with cluster id %s and placement %s",
                  cluster_id, placement)

    def create_empty_rados_obj(self, cluster_id: str) -> None:
        common_conf = conf_obj_name(cluster_id)
        self._rados(cluster_id).write_obj('', conf_obj_name(cluster_id))
        log.info("Created empty object:%s", common_conf)

    def delete_config_obj(self, cluster_id: str) -> None:
        self._rados(cluster_id).remove_all_obj()
        log.info("Deleted %s object and all objects in %s",
                 conf_obj_name(cluster_id), cluster_id)

    def create_nfs_cluster(
            self,
            cluster_id: str,
            placement: Optional[str],
            virtual_ip: Optional[str],
            ingress: Optional[bool] = None,
            ingress_mode: Optional[IngressType] = None,
            port: Optional[int] = None,
    ) -> None:
        try:
            if virtual_ip:
                # validate virtual_ip value: ip_address throws a ValueError
                # exception in case it's not a valid ipv4 or ipv6 address
                ip = virtual_ip.split('/')[0]
                ipaddress.ip_address(ip)
            if virtual_ip and not ingress:
                raise NFSInvalidOperation('virtual_ip can only be provided with ingress enabled')
            if not virtual_ip and ingress:
                raise NFSInvalidOperation('ingress currently requires a virtual_ip')
            if ingress_mode and not ingress:
                raise NFSInvalidOperation('--ingress-mode must be passed along with --ingress')
            invalid_str = re.search('[^A-Za-z0-9-_.]', cluster_id)
            if invalid_str:
                raise NFSInvalidOperation(f"cluster id {cluster_id} is invalid. "
                                          f"{invalid_str.group()} is char not permitted")

            create_ganesha_pool(self.mgr)

            self.create_empty_rados_obj(cluster_id)

            if cluster_id not in available_clusters(self.mgr):
                self._call_orch_apply_nfs(cluster_id, placement, virtual_ip, ingress_mode, port)
                return
            raise NonFatalError(f"{cluster_id} cluster already exists")
        except Exception as e:
            log.exception(f"NFS Cluster {cluster_id} could not be created")
            raise ErrorResponse.wrap(e)

    def delete_nfs_cluster(self, cluster_id: str) -> None:
        try:
            cluster_list = available_clusters(self.mgr)
            if cluster_id in cluster_list:
                self.mgr.export_mgr.delete_all_exports(cluster_id)
                completion = self.mgr.remove_service('ingress.nfs.' + cluster_id)
                orchestrator.raise_if_exception(completion)
                completion = self.mgr.remove_service('nfs.' + cluster_id)
                orchestrator.raise_if_exception(completion)
                self.delete_config_obj(cluster_id)
                return
            raise NonFatalError("Cluster does not exist")
        except Exception as e:
            log.exception(f"Failed to delete NFS Cluster {cluster_id}")
            raise ErrorResponse.wrap(e)

    def list_nfs_cluster(self) -> List[str]:
        try:
            return available_clusters(self.mgr)
        except Exception as e:
            log.exception("Failed to list NFS Cluster")
            raise ErrorResponse.wrap(e)

    def _show_nfs_cluster_info(self, cluster_id: str) -> Dict[str, Any]:
        completion = self.mgr.list_daemons(daemon_type='nfs')
        # Here completion.result is a list DaemonDescription objects
        clusters = orchestrator.raise_if_exception(completion)
        backends: List[Dict[str, Union[Any]]] = []

        for cluster in clusters:
            if cluster_id == cluster.service_id():
                assert cluster.hostname
                try:
                    if cluster.ip:
                        ip = cluster.ip
                    else:
                        c = self.mgr.get_hosts()
                        orchestrator.raise_if_exception(c)
                        hosts = [h for h in c.result or []
                                 if h.hostname == cluster.hostname]
                        if hosts:
                            ip = resolve_ip(hosts[0].addr)
                        else:
                            # sigh
                            ip = resolve_ip(cluster.hostname)
                    backends.append({
                        "hostname": cluster.hostname,
                        "ip": ip,
                        "port": cluster.ports[0] if cluster.ports else None
                    })
                except orchestrator.OrchestratorError:
                    continue

        r: Dict[str, Any] = {
            'virtual_ip': None,
            'backend': backends,
        }
        sc = self.mgr.describe_service(service_type='ingress')
        services = orchestrator.raise_if_exception(sc)
        for i in services:
            spec = cast(IngressSpec, i.spec)
            if spec.backend_service == f'nfs.{cluster_id}':
                r['virtual_ip'] = i.virtual_ip.split('/')[0] if i.virtual_ip else None
                if i.ports:
                    r['port'] = i.ports[0]
                    if len(i.ports) > 1:
                        r['monitor_port'] = i.ports[1]
        log.debug("Successfully fetched %s info: %s", cluster_id, r)
        return r

    def show_nfs_cluster_info(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            if cluster_id and cluster_id not in available_clusters(self.mgr):
                raise ClusterNotFound()
            info_res = {}
            if cluster_id:
                cluster_ls = [cluster_id]
            else:
                cluster_ls = available_clusters(self.mgr)

            for cluster_id in cluster_ls:
                res = self._show_nfs_cluster_info(cluster_id)
                if res:
                    info_res[cluster_id] = res
            return info_res
        except Exception as e:
            log.exception("Failed to show info for cluster")
            raise ErrorResponse.wrap(e)

    def get_nfs_cluster_config(self, cluster_id: str) -> str:
        try:
            if cluster_id in available_clusters(self.mgr):
                rados_obj = self._rados(cluster_id)
                conf = rados_obj.read_obj(user_conf_obj_name(cluster_id))
                return conf or ""
            raise ClusterNotFound()
        except Exception as e:
            log.exception(f"Fetching NFS-Ganesha Config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def set_nfs_cluster_config(self, cluster_id: str, nfs_config: str) -> None:
        try:
            if cluster_id in available_clusters(self.mgr):
                rados_obj = self._rados(cluster_id)
                if rados_obj.check_user_config():
                    raise NonFatalError("NFS-Ganesha User Config already exists")
                rados_obj.write_obj(nfs_config, user_conf_obj_name(cluster_id),
                                    conf_obj_name(cluster_id))
                log.debug("Successfully saved %s's user config: \n %s", cluster_id, nfs_config)
                restart_nfs_service(self.mgr, cluster_id)
                return
            raise ClusterNotFound()
        except NotImplementedError:
            raise ManualRestartRequired("NFS-Ganesha Config Added Successfully")
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha Config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def reset_nfs_cluster_config(self, cluster_id: str) -> None:
        try:
            if cluster_id in available_clusters(self.mgr):
                rados_obj = self._rados(cluster_id)
                if not rados_obj.check_user_config():
                    raise NonFatalError("NFS-Ganesha User Config does not exist")
                rados_obj.remove_obj(user_conf_obj_name(cluster_id),
                                     conf_obj_name(cluster_id))
                restart_nfs_service(self.mgr, cluster_id)
                return
            raise ClusterNotFound()
        except NotImplementedError:
            raise ManualRestartRequired("NFS-Ganesha Config Removed Successfully")
        except Exception as e:
            log.exception(f"Resetting NFS-Ganesha Config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def _rados(self, cluster_id: str) -> NFSRados:
        """Return a new NFSRados object for the given cluster id."""
        return NFSRados(self.mgr.rados, cluster_id)
