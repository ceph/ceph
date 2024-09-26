# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, Optional

from orchestrator import OrchestratorError

from .. import mgr
from ..model import nvmeof as model
from ..security import Scope
from ..services.orchestrator import OrchClient
from ..tools import str_to_bool
from . import APIDoc, APIRouter, BaseController, CreatePermission, \
    DeletePermission, Endpoint, EndpointDoc, Param, ReadPermission, \
    RESTController, UIRouter

logger = logging.getLogger(__name__)

NVME_SCHEMA = {
    "available": (bool, "Is NVMe/TCP available?"),
    "message": (str, "Descriptions")
}

try:
    from ..services.nvmeof_client import NVMeoFClient, empty_response, \
        handle_nvmeof_error, map_collection, map_model
except ImportError as e:
    logger.error("Failed to import NVMeoFClient and related components: %s", e)
else:
    @APIRouter("/nvmeof/gateway", Scope.NVME_OF)
    @APIDoc("NVMe-oF Gateway Management API", "NVMe-oF Gateway")
    class NVMeoFGateway(RESTController):
        @EndpointDoc("Get information about the NVMeoF gateway")
        @map_model(model.GatewayInfo)
        @handle_nvmeof_error
        def list(self, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.get_gateway_info(
                NVMeoFClient.pb2.get_gateway_info_req()
            )

        @ReadPermission
        @Endpoint('GET')
        def group(self):
            try:
                orch = OrchClient.instance()
                return orch.services.list(service_type='nvmeof')
            except OrchestratorError as e:
                # just return none instead of raising an exception
                # since we need this to work regardless of the status
                # of orchestrator in UI
                logger.error('Failed to fetch the gateway groups: %s', e)
                return None

    @APIRouter("/nvmeof/subsystem", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Management API", "NVMe-oF Subsystem")
    class NVMeoFSubsystem(RESTController):
        @EndpointDoc("List all NVMeoF subsystems")
        @map_collection(model.Subsystem, pick="subsystems")
        @handle_nvmeof_error
        def list(self, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.list_subsystems(
                NVMeoFClient.pb2.list_subsystems_req()
            )

        @EndpointDoc(
            "Get information from a specific NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_model(model.Subsystem, first="subsystems")
        @handle_nvmeof_error
        def get(self, nqn: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.list_subsystems(
                NVMeoFClient.pb2.list_subsystems_req(subsystem_nqn=nqn)
            )

        @EndpointDoc(
            "Create a new NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "max_namespaces": Param(int, "Maximum number of namespaces", True, 1024),
                "enable_ha": Param(bool, "Enable high availability"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def create(self, nqn: str, enable_ha: bool, max_namespaces: int = 1024,
                   gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.create_subsystem(
                NVMeoFClient.pb2.create_subsystem_req(
                    subsystem_nqn=nqn, max_namespaces=max_namespaces, enable_ha=enable_ha
                )
            )

        @EndpointDoc(
            "Delete an existing NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "force": Param(bool, "Force delete", "false"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(self, nqn: str, force: Optional[str] = "false", gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.delete_subsystem(
                NVMeoFClient.pb2.delete_subsystem_req(
                    subsystem_nqn=nqn, force=str_to_bool(force)
                )
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/listener", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Listener Management API", "NVMe-oF Subsystem Listener")
    class NVMeoFListener(RESTController):
        @EndpointDoc(
            "List all NVMeoF listeners",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_collection(model.Listener, pick="listeners")
        @handle_nvmeof_error
        def list(self, nqn: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.list_listeners(
                NVMeoFClient.pb2.list_listeners_req(subsystem=nqn)
            )

        @EndpointDoc(
            "Create a new NVMeoF listener",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_name": Param(str, "NVMeoF hostname"),
                "traddr": Param(str, "NVMeoF transport address"),
                "trsvcid": Param(int, "NVMeoF transport service port", True, 4420),
                "adrfam": Param(int, "NVMeoF address family (0 - IPv4, 1 - IPv6)", True, 0),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def create(
            self,
            nqn: str,
            host_name: str,
            traddr: str,
            trsvcid: int = 4420,
            adrfam: int = 0,  # IPv4,
            gw_group: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.create_listener(
                NVMeoFClient.pb2.create_listener_req(
                    nqn=nqn,
                    host_name=host_name,
                    traddr=traddr,
                    trsvcid=int(trsvcid),
                    adrfam=int(adrfam),
                )
            )

        @EndpointDoc(
            "Delete an existing NVMeoF listener",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_name": Param(str, "NVMeoF hostname"),
                "traddr": Param(str, "NVMeoF transport address"),
                "trsvcid": Param(int, "NVMeoF transport service port", True, 4420),
                "adrfam": Param(int, "NVMeoF address family (0 - IPv4, 1 - IPv6)", True, 0),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(
            self,
            nqn: str,
            host_name: str,
            traddr: str,
            trsvcid: int = 4420,
            adrfam: int = 0,  # IPv4
            force: bool = False,
            gw_group: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.delete_listener(
                NVMeoFClient.pb2.delete_listener_req(
                    nqn=nqn,
                    host_name=host_name,
                    traddr=traddr,
                    trsvcid=int(trsvcid),
                    adrfam=int(adrfam),
                    force=str_to_bool(force),
                )
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/namespace", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Namespace Management API", "NVMe-oF Subsystem Namespace")
    class NVMeoFNamespace(RESTController):
        @EndpointDoc(
            "List all NVMeoF namespaces in a subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_collection(model.Namespace, pick="namespaces")
        @handle_nvmeof_error
        def list(self, nqn: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.list_namespaces(
                NVMeoFClient.pb2.list_namespaces_req(subsystem=nqn)
            )

        @EndpointDoc(
            "Get info from specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_model(model.Namespace, first="namespaces")
        @handle_nvmeof_error
        def get(self, nqn: str, nsid: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.list_namespaces(
                NVMeoFClient.pb2.list_namespaces_req(subsystem=nqn, nsid=int(nsid))
            )

        @ReadPermission
        @Endpoint('GET', '{nsid}/io_stats')
        @EndpointDoc(
            "Get IO stats from specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_model(model.NamespaceIOStats)
        @handle_nvmeof_error
        def io_stats(self, nqn: str, nsid: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.namespace_get_io_stats(
                NVMeoFClient.pb2.namespace_get_io_stats_req(
                    subsystem_nqn=nqn, nsid=int(nsid))
            )

        @EndpointDoc(
            "Create a new NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "rbd_pool": Param(str, "RBD pool name"),
                "rbd_image_name": Param(str, "RBD image name"),
                "create_image": Param(bool, "Create RBD image"),
                "size": Param(int, "RBD image size"),
                "block_size": Param(int, "NVMeoF namespace block size"),
                "load_balancing_group": Param(int, "Load balancing group"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_model(model.NamespaceCreation)
        @handle_nvmeof_error
        def create(
            self,
            nqn: str,
            rbd_image_name: str,
            rbd_pool: str = "rbd",
            create_image: Optional[bool] = True,
            size: Optional[int] = 1024,
            block_size: int = 512,
            load_balancing_group: Optional[int] = None,
            gw_group: Optional[str] = None,
        ):
            return NVMeoFClient(gw_group=gw_group).stub.namespace_add(
                NVMeoFClient.pb2.namespace_add_req(
                    subsystem_nqn=nqn,
                    rbd_image_name=rbd_image_name,
                    rbd_pool_name=rbd_pool,
                    block_size=block_size,
                    create_image=create_image,
                    size=size,
                    anagrpid=load_balancing_group,
                )
            )

        @EndpointDoc(
            "Update an existing NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "rbd_image_size": Param(int, "RBD image size"),
                "load_balancing_group": Param(int, "Load balancing group"),
                "rw_ios_per_second": Param(int, "Read/Write IOPS"),
                "rw_mbytes_per_second": Param(int, "Read/Write MB/s"),
                "r_mbytes_per_second": Param(int, "Read MB/s"),
                "w_mbytes_per_second": Param(int, "Write MB/s"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def update(
            self,
            nqn: str,
            nsid: str,
            rbd_image_size: Optional[int] = None,
            load_balancing_group: Optional[int] = None,
            rw_ios_per_second: Optional[int] = None,
            rw_mbytes_per_second: Optional[int] = None,
            r_mbytes_per_second: Optional[int] = None,
            w_mbytes_per_second: Optional[int] = None,
            gw_group: Optional[str] = None
        ):
            if rbd_image_size:
                mib = 1024 * 1024
                new_size_mib = int((rbd_image_size + mib - 1) / mib)

                response = NVMeoFClient(gw_group=gw_group).stub.namespace_resize(
                    NVMeoFClient.pb2.namespace_resize_req(
                        subsystem_nqn=nqn, nsid=int(nsid), new_size=new_size_mib
                    )
                )
                if response.status != 0:
                    return response

            if load_balancing_group:
                response = NVMeoFClient().stub.namespace_change_load_balancing_group(
                    NVMeoFClient.pb2.namespace_change_load_balancing_group_req(
                        subsystem_nqn=nqn, nsid=int(nsid), anagrpid=load_balancing_group
                    )
                )
                if response.status != 0:
                    return response

            if (
                rw_ios_per_second
                or rw_mbytes_per_second
                or r_mbytes_per_second
                or w_mbytes_per_second
            ):
                response = NVMeoFClient().stub.namespace_set_qos_limits(
                    NVMeoFClient.pb2.namespace_set_qos_req(
                        subsystem_nqn=nqn,
                        nsid=int(nsid),
                        rw_ios_per_second=rw_ios_per_second,
                        rw_mbytes_per_second=rw_mbytes_per_second,
                        r_mbytes_per_second=r_mbytes_per_second,
                        w_mbytes_per_second=w_mbytes_per_second,
                    )
                )
                if response.status != 0:
                    return response

            return response

        @EndpointDoc(
            "Delete an existing NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(self, nqn: str, nsid: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.namespace_delete(
                NVMeoFClient.pb2.namespace_delete_req(subsystem_nqn=nqn, nsid=int(nsid))
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/host", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Host Allowlist Management API",
            "NVMe-oF Subsystem Host Allowlist")
    class NVMeoFHost(RESTController):
        @EndpointDoc(
            "List all allowed hosts for an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_collection(
            model.Host,
            pick="hosts",
            # Display the "allow any host" option as another host item
            finalize=lambda i, o: [model.Host(nqn="*")._asdict()] + o
            if i.allow_any_host
            else o,
        )
        @handle_nvmeof_error
        def list(self, nqn: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.list_hosts(
                NVMeoFClient.pb2.list_hosts_req(subsystem=nqn)
            )

        @EndpointDoc(
            "Allow hosts to access an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to allow any host.'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def create(self, nqn: str, host_nqn: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.add_host(
                NVMeoFClient.pb2.add_host_req(subsystem_nqn=nqn, host_nqn=host_nqn)
            )

        @EndpointDoc(
            "Disallow hosts from accessing an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to disallow any host.'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(self, nqn: str, host_nqn: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.remove_host(
                NVMeoFClient.pb2.remove_host_req(subsystem_nqn=nqn, host_nqn=host_nqn)
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/connection", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Connection Management API", "NVMe-oF Subsystem Connection")
    class NVMeoFConnection(RESTController):
        @EndpointDoc(
            "List all NVMeoF Subsystem Connections",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @map_collection(model.Connection, pick="connections")
        @handle_nvmeof_error
        def list(self, nqn: str, gw_group: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group).stub.list_connections(
                NVMeoFClient.pb2.list_connections_req(subsystem=nqn)
            )

    @UIRouter('/nvmeof', Scope.NVME_OF)
    class NVMeoFTcpUI(BaseController):
        @Endpoint('GET', '/status')
        @ReadPermission
        @EndpointDoc("Display NVMe/TCP service status",
                     responses={200: NVME_SCHEMA})
        def status(self) -> dict:
            status: Dict[str, Any] = {'available': True, 'message': None}
            orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
            if orch_backend == 'cephadm':
                orch = OrchClient.instance()
                orch_status = orch.status()
                if not orch_status['available']:
                    return status
                if not orch.services.list_daemons(daemon_type='nvmeof'):
                    status["available"] = False
                    status["message"] = 'An NVMe/TCP service must be created.'
            return status

        @Endpoint('POST', "/subsystem/{subsystem_nqn}/host")
        @EndpointDoc("Add one or more initiator hosts to an NVMeoF subsystem",
                     parameters={
                         'subsystem_nqn': (str, 'Subsystem NQN'),
                         "host_nqn": Param(str, 'Comma separated list of NVMeoF host NQNs'),
                     })
        @empty_response
        @handle_nvmeof_error
        @CreatePermission
        def add(self, subsystem_nqn: str, host_nqn: str = ""):
            response = None
            all_host_nqns = host_nqn.split(',')

            for nqn in all_host_nqns:
                response = NVMeoFClient().stub.add_host(
                    NVMeoFClient.pb2.add_host_req(subsystem_nqn=subsystem_nqn, host_nqn=nqn)
                )
                if response.status != 0:
                    return response
            return response

        @Endpoint(method='DELETE', path="/subsystem/{subsystem_nqn}/host/{host_nqn}")
        @EndpointDoc("Remove on or more initiator hosts from an NVMeoF subsystem",
                     parameters={
                         "subsystem_nqn": Param(str, "NVMeoF subsystem NQN"),
                         "host_nqn": Param(str, 'Comma separated list of NVMeoF host NQN.'),
                     })
        @empty_response
        @handle_nvmeof_error
        @DeletePermission
        def remove(self, subsystem_nqn: str, host_nqn: str):
            response = None
            to_delete_nqns = host_nqn.split(',')

            for del_nqn in to_delete_nqns:
                response = NVMeoFClient().stub.remove_host(
                    NVMeoFClient.pb2.remove_host_req(subsystem_nqn=subsystem_nqn, host_nqn=del_nqn)
                )
                if response.status != 0:
                    return response
                logger.info("removed host %s from subsystem %s", del_nqn, subsystem_nqn)

            return response
