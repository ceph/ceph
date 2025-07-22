# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
import logging
from typing import Any, Dict, List, Optional

import cherrypy
from orchestrator import OrchestratorError

from .. import mgr
from ..model import nvmeof as model
from ..security import Scope
from ..services.nvmeof_cli import NvmeofCLICommand, convert_to_bytes
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
    from ..services.nvmeof_client import NVMeoFClient, convert_to_model, \
        empty_response, handle_nvmeof_error, pick
except ImportError as e:
    logger.error("Failed to import NVMeoFClient and related components: %s", e)
else:
    @APIRouter("/nvmeof/gateway", Scope.NVME_OF)
    @APIDoc("NVMe-oF Gateway Management API", "NVMe-oF Gateway")
    class NVMeoFGateway(RESTController):
        @NvmeofCLICommand(
            "nvmeof gateway info", model.GatewayInfo, alias="nvmeof gw info"
        )
        @EndpointDoc("Get information about the NVMeoF gateway")
        @convert_to_model(model.GatewayInfo)
        @handle_nvmeof_error
        def list(self, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.get_gateway_info(
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

        @ReadPermission
        @Endpoint('GET', '/version')
        @NvmeofCLICommand(
            "nvmeof gateway version", model.GatewayVersion, alias="nvmeof gw version"
        )
        @EndpointDoc("Get the version of the NVMeoF gateway")
        @convert_to_model(model.GatewayVersion)
        @handle_nvmeof_error
        def version(self, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            gw_info = NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.get_gateway_info(
                NVMeoFClient.pb2.get_gateway_info_req()
            )
            return NVMeoFClient.pb2.gw_version(status=gw_info.status,
                                               error_message=gw_info.error_message,
                                               version=gw_info.version)

        @ReadPermission
        @Endpoint('GET', '/log_level')
        @NvmeofCLICommand(
            "nvmeof gateway get_log_level", model.GatewayLogLevelInfo,
            alias="nvmeof gw get_log_level"
        )
        @EndpointDoc("Get NVMeoF gateway log level information")
        @convert_to_model(model.GatewayLogLevelInfo)
        @handle_nvmeof_error
        def get_log_level(self, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            gw_log_level = NVMeoFClient(gw_group=gw_group,
                                        traddr=traddr).stub.get_gateway_log_level(
                NVMeoFClient.pb2.get_gateway_log_level_req()
            )
            return gw_log_level

        @ReadPermission
        @Endpoint('PUT', '/log_level')
        @NvmeofCLICommand(
            "nvmeof gateway set_log_level", model.RequestStatus, alias="nvmeof gw set_log_level")
        @EndpointDoc("Set NVMeoF gateway log levels")
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def set_log_level(self, log_level: str, gw_group: Optional[str] = None,
                          traddr: Optional[str] = None):
            log_level = log_level.lower()
            gw_log_level = NVMeoFClient(gw_group=gw_group,
                                        traddr=traddr).stub.set_gateway_log_level(
                NVMeoFClient.pb2.set_gateway_log_level_req(log_level=log_level)
            )
            return gw_log_level

    @APIRouter("/nvmeof/spdk", Scope.NVME_OF)
    @APIDoc("NVMe-oF SPDK Management API", "NVMe-oF SPDK")
    class NVMeoFSpdk(RESTController):
        @ReadPermission
        @Endpoint('GET', '/log_level')
        @NvmeofCLICommand("nvmeof spdk_log_level get", model.SpdkNvmfLogFlagsAndLevelInfo)
        @EndpointDoc("Get NVMeoF gateway spdk log levels")
        @convert_to_model(model.SpdkNvmfLogFlagsAndLevelInfo)
        @handle_nvmeof_error
        def get_spdk_log_level(
            self, all_log_flags: Optional[bool] = None,
            gw_group: Optional[str] = None, traddr: Optional[str] = None
        ):
            spdk_log_level = NVMeoFClient(gw_group=gw_group,
                                          traddr=traddr).stub.get_spdk_nvmf_log_flags_and_level(
                NVMeoFClient.pb2.get_spdk_nvmf_log_flags_and_level_req(all_log_flags=all_log_flags)
            )
            return spdk_log_level

        @ReadPermission
        @Endpoint('PUT', '/log_level')
        @NvmeofCLICommand("nvmeof spdk_log_level set", model.RequestStatus)
        @EndpointDoc("Set NVMeoF gateway spdk log levels")
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def set_spdk_log_level(self, log_level: Optional[str] = None,
                               print_level: Optional[str] = None,
                               extra_log_flags: Optional[List[str]] = None,
                               gw_group: Optional[str] = None, traddr: Optional[str] = None):
            log_level = log_level.upper() if log_level else None
            print_level = print_level.upper() if print_level else None
            spdk_log_level = NVMeoFClient(gw_group=gw_group,
                                          traddr=traddr).stub.set_spdk_nvmf_logs(
                NVMeoFClient.pb2.set_spdk_nvmf_logs_req(log_level=log_level,
                                                        print_level=print_level,
                                                        extra_log_flags=extra_log_flags)
            )
            return spdk_log_level

        @ReadPermission
        @Endpoint('PUT', '/log_level/disable')
        @NvmeofCLICommand("nvmeof spdk_log_level disable", model.RequestStatus)
        @EndpointDoc("Disable NVMeoF gateway spdk log")
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def disable_spdk_log_level(
            self, extra_log_flags: Optional[List[str]] = None,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            spdk_log_level = NVMeoFClient(gw_group=gw_group,
                                          traddr=traddr).stub.disable_spdk_nvmf_logs(
                NVMeoFClient.pb2.disable_spdk_nvmf_logs_req(extra_log_flags=extra_log_flags)
            )
            return spdk_log_level

    @APIRouter("/nvmeof/subsystem", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Management API", "NVMe-oF Subsystem")
    class NVMeoFSubsystem(RESTController):
        @pick(field="subsystems")
        @NvmeofCLICommand("nvmeof subsystem list", model.SubsystemList)
        @EndpointDoc("List all NVMeoF subsystems")
        @convert_to_model(model.SubsystemList)
        @handle_nvmeof_error
        def list(self, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_subsystems(
                NVMeoFClient.pb2.list_subsystems_req()
            )

        @pick(field="subsystems", first=True)
        @NvmeofCLICommand("nvmeof subsystem get", model.SubsystemList)
        @EndpointDoc(
            "Get information from a specific NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.SubsystemList)
        @handle_nvmeof_error
        def get(self, nqn: str, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_subsystems(
                NVMeoFClient.pb2.list_subsystems_req(subsystem_nqn=nqn)
            )

        @empty_response
        @NvmeofCLICommand("nvmeof subsystem add", model.RequestStatus)
        @EndpointDoc(
            "Create a new NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "max_namespaces": Param(int, "Maximum number of namespaces", True, 1024),
                "enable_ha": Param(bool, "Enable high availability"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.SubsystemStatus)
        @handle_nvmeof_error
        def create(self, nqn: str, enable_ha: Optional[bool] = True,
                   max_namespaces: Optional[int] = 4096, no_group_append: Optional[bool] = True,
                   serial_number: Optional[str] = None, dhchap_key: Optional[str] = None,
                   gw_group: Optional[str] = None, traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.create_subsystem(
                NVMeoFClient.pb2.create_subsystem_req(
                    subsystem_nqn=nqn, serial_number=serial_number,
                    max_namespaces=max_namespaces, enable_ha=enable_ha,
                    no_group_append=no_group_append, dhchap_key=dhchap_key
                )
            )

        @empty_response
        @NvmeofCLICommand("nvmeof subsystem del", model.RequestStatus)
        @EndpointDoc(
            "Delete an existing NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "force": Param(bool, "Force delete", True, False),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def delete(self, nqn: str, force: Optional[str] = "false", gw_group: Optional[str] = None,
                   traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.delete_subsystem(
                NVMeoFClient.pb2.delete_subsystem_req(
                    subsystem_nqn=nqn, force=str_to_bool(force)
                )
            )

        @EndpointDoc(
            "Change subsystem inband authentication key",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "dhchap_key": Param(str, "Subsystem DH-HMAC-CHAP key"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @empty_response
        @NvmeofCLICommand("nvmeof subsystem change_key", model.RequestStatus)
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def change_key(self, nqn: str, dhchap_key: str, gw_group: Optional[str] = None,
                       traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.change_subsystem_key(
                NVMeoFClient.pb2.change_subsystem_key_req(
                    subsystem_nqn=nqn, dhchap_key=dhchap_key
                )
            )

        @EndpointDoc(
            "Delete subsystem inband authentication key",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @empty_response
        @NvmeofCLICommand("nvmeof subsystem del_key", model.RequestStatus)
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def del_key(self, nqn: str, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.change_subsystem_key(
                NVMeoFClient.pb2.change_subsystem_key_req(
                    subsystem_nqn=nqn, dhchap_key=None
                )
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/listener", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Listener Management API", "NVMe-oF Subsystem Listener")
    class NVMeoFListener(RESTController):
        @pick("listeners")
        @NvmeofCLICommand("nvmeof listener list", model.ListenerList)
        @EndpointDoc(
            "List all NVMeoF listeners",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.ListenerList)
        @handle_nvmeof_error
        def list(self, nqn: str, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_listeners(
                NVMeoFClient.pb2.list_listeners_req(subsystem=nqn)
            )

        @empty_response
        @NvmeofCLICommand("nvmeof listener add", model.RequestStatus)
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
        @convert_to_model(model.RequestStatus)
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

        @empty_response
        @NvmeofCLICommand("nvmeof listener del", model.RequestStatus)
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
        @convert_to_model(model.RequestStatus)
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
        @pick("namespaces")
        @NvmeofCLICommand(
            "nvmeof namespace list", model.NamespaceList, alias="nvmeof ns list"
        )
        @EndpointDoc(
            "List all NVMeoF namespaces in a subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.NamespaceList)
        @handle_nvmeof_error
        def list(self, nqn: str, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_namespaces(
                NVMeoFClient.pb2.list_namespaces_req(subsystem=nqn)
            )

        @pick("namespaces", first=True)
        @NvmeofCLICommand(
            "nvmeof namespace get", model.NamespaceList, alias="nvmeof ns get")
        @EndpointDoc(
            "Get info from specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.NamespaceList)
        @handle_nvmeof_error
        def get(self, nqn: str, nsid: str, gw_group: Optional[str] = None,
                traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_namespaces(
                NVMeoFClient.pb2.list_namespaces_req(subsystem=nqn, nsid=int(nsid))
            )

        @ReadPermission
        @Endpoint('GET', '{nsid}/io_stats')
        @NvmeofCLICommand(
            "nvmeof namespace get_io_stats", model.NamespaceIOStats,
            alias="nvmeof ns get_io_stats"
        )
        @EndpointDoc(
            "Get IO stats from specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.NamespaceIOStats)
        @handle_nvmeof_error
        def io_stats(self, nqn: str, nsid: str, gw_group: Optional[str] = None,
                     traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_get_io_stats(
                NVMeoFClient.pb2.namespace_get_io_stats_req(
                    subsystem_nqn=nqn, nsid=int(nsid))
            )

        @NvmeofCLICommand(
            "nvmeof namespace add", model.NamespaceCreation, alias="nvmeof ns add"
        )
        @EndpointDoc(
            "Create a new NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "rbd_pool": Param(str, "RBD pool name"),
                "rbd_image_name": Param(str, "RBD image name"),
                "create_image": Param(bool, "Create RBD image"),
                "size": Param(int, "RBD image size"),
                "rbd_image_size": Param(int, "RBD image size"),
                "trash_image": Param(bool, "Trash the RBD image when namespace is removed"),
                "block_size": Param(int, "NVMeoF namespace block size"),
                "load_balancing_group": Param(int, "Load balancing group"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "force": Param(
                    bool,
                    "Force create namespace even it image is used by other namespace"
                ),
                "no_auto_visible": Param(
                    bool,
                    "Namespace will be visible only for the allowed hosts"
                )
            },
        )
        @convert_to_model(model.NamespaceCreation)
        @handle_nvmeof_error
        def create(
            self,
            nqn: str,
            rbd_image_name: str,
            rbd_pool: str = "rbd",
            create_image: Optional[bool] = False,
            size: Optional[int] = 1024,
            rbd_image_size: Optional[int] = None,
            trash_image: Optional[bool] = False,
            block_size: int = 512,
            load_balancing_group: Optional[int] = None,
            force: Optional[bool] = False,
            no_auto_visible: Optional[bool] = False,
            disable_auto_resize: Optional[bool] = False,
            read_only: Optional[bool] = False,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None,
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_add(
                NVMeoFClient.pb2.namespace_add_req(
                    subsystem_nqn=nqn,
                    rbd_image_name=rbd_image_name,
                    rbd_pool_name=rbd_pool,
                    block_size=block_size,
                    create_image=create_image,
                    size=rbd_image_size or size,
                    trash_image=trash_image,
                    anagrpid=load_balancing_group,
                    force=force,
                    no_auto_visible=no_auto_visible,
                    disable_auto_resize=disable_auto_resize,
                    read_only=read_only
                )
            )

        @NvmeofCLICommand("nvmeof ns add", model.NamespaceCreation)
        @convert_to_model(model.NamespaceCreation)
        @handle_nvmeof_error
        def create_cli(
            self,
            nqn: str,
            rbd_image_name: str,
            rbd_pool: str = "rbd",
            create_image: Optional[bool] = False,
            size: Optional[str] = None,
            rbd_image_size: Optional[str] = None,
            trash_image: Optional[bool] = False,
            block_size: int = 512,
            load_balancing_group: Optional[int] = None,
            force: Optional[bool] = False,
            no_auto_visible: Optional[bool] = False,
            disable_auto_resize: Optional[bool] = False,
            read_only: Optional[bool] = False,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None,
        ):
            size_b = rbd_image_size_b = None
            if size:
                size_b = convert_to_bytes(size, default_unit='MB')
            if rbd_image_size:
                rbd_image_size_b = convert_to_bytes(rbd_image_size, default_unit='MB')
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_add(
                NVMeoFClient.pb2.namespace_add_req(
                    subsystem_nqn=nqn,
                    rbd_image_name=rbd_image_name,
                    rbd_pool_name=rbd_pool,
                    block_size=block_size,
                    create_image=create_image,
                    size=rbd_image_size_b or size_b,
                    trash_image=trash_image,
                    anagrpid=load_balancing_group,
                    force=force,
                    no_auto_visible=no_auto_visible,
                    disable_auto_resize=disable_auto_resize,
                    read_only=read_only
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/set_qos')
        @NvmeofCLICommand(
            "nvmeof namespace set_qos", model=model.RequestStatus, alias="nvmeof ns set_qos")
        @EndpointDoc(
            "set QOS for specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "rw_ios_per_second": Param(int, "Read/Write IOPS"),
                "rw_mbytes_per_second": Param(int, "Read/Write MB/s"),
                "r_mbytes_per_second": Param(int, "Read MB/s"),
                "w_mbytes_per_second": Param(int, "Write MB/s"),
                "force": Param(
                    bool,
                    "Set QOS limits even if they were changed by RBD"
                ),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def set_qos(
            self,
            nqn: str,
            nsid: str,
            rw_ios_per_second: Optional[int] = None,
            rw_mbytes_per_second: Optional[int] = None,
            r_mbytes_per_second: Optional[int] = None,
            w_mbytes_per_second: Optional[int] = None,
            force: Optional[bool] = False,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(
                gw_group=gw_group,
                traddr=traddr
            ).stub.namespace_set_qos_limits(
                NVMeoFClient.pb2.namespace_set_qos_req(
                    subsystem_nqn=nqn,
                    nsid=int(nsid),
                    rw_ios_per_second=rw_ios_per_second,
                    rw_mbytes_per_second=rw_mbytes_per_second,
                    r_mbytes_per_second=r_mbytes_per_second,
                    w_mbytes_per_second=w_mbytes_per_second,
                    force=force,
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/change_load_balancing_group')
        @NvmeofCLICommand(
            "nvmeof namespace change_load_balancing_group", model=model.RequestStatus,
            alias="nvmeof ns change_load_balancing_group"
        )
        @EndpointDoc(
            "set the load balancing group for specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "load_balancing_group": Param(int, "Load balancing group"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def change_load_balancing_group(
            self,
            nqn: str,
            nsid: str,
            load_balancing_group: Optional[int] = None,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(
                gw_group=gw_group,
                traddr=traddr
            ).stub.namespace_change_load_balancing_group(
                NVMeoFClient.pb2.namespace_change_load_balancing_group_req(
                    subsystem_nqn=nqn, nsid=int(nsid), anagrpid=load_balancing_group
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/resize')
        @NvmeofCLICommand(
            "nvmeof namespace resize", model=model.RequestStatus, alias="nvmeof ns resize"
        )
        @EndpointDoc(
            "resize the specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "rbd_image_size": Param(int, "RBD image size"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def resize(
            self,
            nqn: str,
            nsid: str,
            rbd_image_size: int,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            mib = 1024 * 1024
            new_size_mib = int((rbd_image_size + mib - 1) / mib)

            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_resize(
                NVMeoFClient.pb2.namespace_resize_req(
                    subsystem_nqn=nqn, nsid=int(nsid), new_size=new_size_mib
                )
            )

        @NvmeofCLICommand("nvmeof ns resize", model=model.RequestStatus)
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def resize_cli(
            self,
            nqn: str,
            nsid: str,
            rbd_image_size: str,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            if rbd_image_size:
                rbd_image_size_b = convert_to_bytes(rbd_image_size, default_unit='MB')
            mib = 1024 * 1024
            rbd_image_size_mb = rbd_image_size_b // mib

            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_resize(
                NVMeoFClient.pb2.namespace_resize_req(
                    subsystem_nqn=nqn, nsid=int(nsid), new_size=rbd_image_size_mb
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/add_host')
        @NvmeofCLICommand(
            "nvmeof namespace add_host", model=model.RequestStatus, alias="nvmeof ns add_host"
        )
        @EndpointDoc(
            "Adds a host to the specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to allow any host.'),
                "force": Param(
                    bool,
                    "Allow adding the host to the namespace even if the host "
                    "has no access to the subsystem"
                ),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def add_host(
            self,
            nqn: str,
            nsid: str,
            host_nqn: str,
            force: Optional[bool] = None,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_add_host(
                NVMeoFClient.pb2.namespace_add_host_req(subsystem_nqn=nqn,
                                                        nsid=int(nsid),
                                                        host_nqn=host_nqn,
                                                        force=str_to_bool(force))
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/del_host')
        @NvmeofCLICommand(
            "nvmeof namespace del_host", model=model.RequestStatus, alias="nvmeof ns del_host"
        )
        @EndpointDoc(
            "Removes a host from the specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to allow any host.'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def del_host(
            self,
            nqn: str,
            nsid: str,
            host_nqn: str,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_delete_host(
                NVMeoFClient.pb2.namespace_delete_host_req(
                    subsystem_nqn=nqn,
                    nsid=int(nsid),
                    host_nqn=host_nqn,
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/change_visibility')
        @NvmeofCLICommand(
            "nvmeof namespace change_visibility", model=model.RequestStatus,
            alias="nvmeof ns change_visibility"
        )
        @EndpointDoc(
            "changes the visibility of the specified NVMeoF namespace to all or selected hosts",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "auto_visible": Param(bool, 'True if visible to all hosts'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def change_visibility(
            self,
            nqn: str,
            nsid: str,
            auto_visible: str,
            force: Optional[bool] = False,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_change_visibility(
                NVMeoFClient.pb2.namespace_change_visibility_req(
                    subsystem_nqn=nqn,
                    nsid=int(nsid),
                    auto_visible=str_to_bool(auto_visible),
                    force=str_to_bool(force),
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/set_auto_resize')
        @NvmeofCLICommand(
            "nvmeof namespace set_auto_resize", model=model.RequestStatus,
            alias="nvmeof ns set_auto_resize"
        )
        @EndpointDoc(
            "Enable or disable namespace auto resize when RBD image is resized",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "auto_resize_enabled": Param(
                    bool,
                    'Enable or disable auto resize of '
                    'namespace when RBD image is resized'
                ),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def set_auto_resize(
            self,
            nqn: str,
            nsid: str,
            auto_resize_enabled: bool,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_set_auto_resize(
                NVMeoFClient.pb2.namespace_set_auto_resize_req(
                    subsystem_nqn=nqn,
                    nsid=int(nsid),
                    auto_resize=str_to_bool(auto_resize_enabled),
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/set_rbd_trash_image')
        @NvmeofCLICommand(
            "nvmeof namespace set_rbd_trash_image", model=model.RequestStatus,
            alias="nvmeof ns set_rbd_trash_image"
        )
        @EndpointDoc(
            "changes the trash image on delete of the specified NVMeoF \
                namespace to all or selected hosts",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "rbd_trash_image_on_delete": Param(bool, 'True if active'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def set_rbd_trash_image(
            self,
            nqn: str,
            nsid: str,
            rbd_trash_image_on_delete: str,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(
                gw_group=gw_group,
                traddr=traddr,
            ).stub.namespace_set_rbd_trash_image(
                NVMeoFClient.pb2.namespace_set_rbd_trash_image_req(
                    subsystem_nqn=nqn,
                    nsid=int(nsid),
                    trash_image=str_to_bool(rbd_trash_image_on_delete),
                )
            )

        @ReadPermission
        @Endpoint('PUT', '{nsid}/refresh_size')
        @NvmeofCLICommand(
            "nvmeof namespace refresh_size", model=model.RequestStatus,
            alias="nvmeof ns refresh_size"
        )
        @EndpointDoc(
            "refresh the specified NVMeoF namespace to current RBD image size",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "traddr": Param(str, "NVMeoF gateway address", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def refresh_size(
            self,
            nqn: str,
            nsid: str,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_resize(
                NVMeoFClient.pb2.namespace_resize_req(
                    subsystem_nqn=nqn,
                    nsid=int(nsid),
                    new_size=0
                )
            )

        @pick("namespaces", first=True)
        @NvmeofCLICommand(
            "nvmeof namespace update", model.NamespaceList, alias="nvmeof ns update"
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
                "trash_image": Param(bool, "Trash RBD image after removing namespace")
            },
        )
        @convert_to_model(model.NamespaceList)
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
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None,
            trash_image: Optional[bool] = None,
        ):
            contains_failure = False

            if rbd_image_size:
                mib = 1024 * 1024
                new_size_mib = int((rbd_image_size + mib - 1) / mib)

                resp = NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_resize(
                    NVMeoFClient.pb2.namespace_resize_req(
                        subsystem_nqn=nqn, nsid=int(nsid), new_size=new_size_mib
                    )
                )
                if resp.status != 0:
                    contains_failure = True

            if load_balancing_group:
                resp = NVMeoFClient().stub.namespace_change_load_balancing_group(
                    NVMeoFClient.pb2.namespace_change_load_balancing_group_req(
                        subsystem_nqn=nqn, nsid=int(nsid), anagrpid=load_balancing_group
                    )
                )
                if resp.status != 0:
                    contains_failure = True

            if rw_ios_per_second or rw_mbytes_per_second or r_mbytes_per_second \
               or w_mbytes_per_second:
                resp = NVMeoFClient().stub.namespace_set_qos_limits(
                    NVMeoFClient.pb2.namespace_set_qos_req(
                        subsystem_nqn=nqn,
                        nsid=int(nsid),
                        rw_ios_per_second=rw_ios_per_second,
                        rw_mbytes_per_second=rw_mbytes_per_second,
                        r_mbytes_per_second=r_mbytes_per_second,
                        w_mbytes_per_second=w_mbytes_per_second,
                    )
                )
                if resp.status != 0:
                    contains_failure = True

            if trash_image is not None:
                resp = NVMeoFClient().stub.namespace_set_rbd_trash_image(
                    NVMeoFClient.pb2.namespace_set_rbd_trash_image_req(
                        subsystem_nqn=nqn,
                        nsid=int(nsid),
                        trash_image=str_to_bool(trash_image)
                    )
                )
                if resp.status != 0:
                    contains_failure = True

            if contains_failure:
                cherrypy.response.status = 202

            response = NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_namespaces(
                NVMeoFClient.pb2.list_namespaces_req(subsystem=nqn, nsid=int(nsid))
            )
            return response

        @empty_response
        @NvmeofCLICommand("nvmeof namespace del", model.RequestStatus, alias="nvmeof ns del")
        @EndpointDoc(
            "Delete an existing NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
                "force": Param(str, "Force remove the RBD image")
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def delete(
            self,
            nqn: str,
            nsid: str,
            gw_group: Optional[str] = None,
            traddr: Optional[str] = None,
            force: Optional[str] = "false"
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.namespace_delete(
                NVMeoFClient.pb2.namespace_delete_req(
                    subsystem_nqn=nqn,
                    nsid=int(nsid),
                    i_am_sure=str_to_bool(force)
                )
            )

    def _update_hosts(hosts_info_resp):
        if hosts_info_resp.get('allow_any_host'):
            hosts_info_resp['hosts'].insert(0, {"nqn": "*"})
        return hosts_info_resp

    @APIRouter("/nvmeof/subsystem/{nqn}/host", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Host Allowlist Management API",
            "NVMe-oF Subsystem Host Allowlist")
    class NVMeoFHost(RESTController):
        @pick('hosts')
        @NvmeofCLICommand("nvmeof host list", model.HostsInfo)
        @EndpointDoc(
            "List all allowed hosts for an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "clear_alerts": Param(bool, "Clear any host alert signal after getting its value",
                                      True, False),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.HostsInfo, finalize=_update_hosts)
        @handle_nvmeof_error
        def list(
            self, nqn: str, clear_alerts: Optional[bool] = None,
            gw_group: Optional[str] = None, traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_hosts(
                NVMeoFClient.pb2.list_hosts_req(subsystem=nqn, clear_alerts=clear_alerts)
            )

        @empty_response
        @NvmeofCLICommand("nvmeof host add", model.RequestStatus)
        @EndpointDoc(
            "Allow hosts to access an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to allow any host.'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def create(
            self, nqn: str, host_nqn: str, dhchap_key: Optional[str] = None,
            psk: Optional[str] = None, gw_group: Optional[str] = None, traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.add_host(
                NVMeoFClient.pb2.add_host_req(subsystem_nqn=nqn, host_nqn=host_nqn,
                                              dhchap_key=dhchap_key, psk=psk)
            )

        @empty_response
        @NvmeofCLICommand("nvmeof host del", model.RequestStatus)
        @EndpointDoc(
            "Disallow hosts from accessing an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to disallow any host.'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def delete(self, nqn: str, host_nqn: str, gw_group: Optional[str] = None,
                   traddr: Optional[str] = None):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.remove_host(
                NVMeoFClient.pb2.remove_host_req(subsystem_nqn=nqn, host_nqn=host_nqn)
            )

        @empty_response
        @NvmeofCLICommand("nvmeof host change_key", model.RequestStatus)
        @EndpointDoc(
            "Change host DH-HMAC-CHAP key",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN'),
                "dhchap_key": Param(str, 'Host DH-HMAC-CHAP key'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def change_key(
            self, nqn: str, host_nqn: str, dhchap_key: str,
            gw_group: Optional[str] = None, traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.change_host_key(
                NVMeoFClient.pb2.change_host_key_req(subsystem_nqn=nqn,
                                                     host_nqn=host_nqn,
                                                     dhchap_key=dhchap_key)
            )

        @empty_response
        @NvmeofCLICommand("nvmeof host del_key", model.RequestStatus)
        @EndpointDoc(
            "Delete host DH-HMAC-CHAP key",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN.'),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.RequestStatus)
        @handle_nvmeof_error
        def del_key(
            self, nqn: str, host_nqn: str, gw_group: Optional[str] = None,
            traddr: Optional[str] = None
        ):
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.change_host_key(
                NVMeoFClient.pb2.change_host_key_req(subsystem_nqn=nqn,
                                                     host_nqn=host_nqn,
                                                     dhchap_key=None)
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/connection", Scope.NVME_OF)
    @APIDoc("NVMe-oF Subsystem Connection Management API", "NVMe-oF Subsystem Connection")
    class NVMeoFConnection(RESTController):
        @pick("connections")
        @NvmeofCLICommand("nvmeof connection list", model.ConnectionList)
        @EndpointDoc(
            "List all NVMeoF Subsystem Connections",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gw_group": Param(str, "NVMeoF gateway group", True, None),
            },
        )
        @convert_to_model(model.ConnectionList)
        @handle_nvmeof_error
        def list(self, nqn: Optional[str] = None,
                 gw_group: Optional[str] = None, traddr: Optional[str] = None):
            if not nqn:
                nqn = '*'
            return NVMeoFClient(gw_group=gw_group, traddr=traddr).stub.list_connections(
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
                         "gw_group": Param(str, "NVMeoF gateway group")
                     })
        @empty_response
        @handle_nvmeof_error
        @CreatePermission
        def add(self, subsystem_nqn: str, gw_group: str, host_nqn: str = ""):
            response = None
            all_host_nqns = host_nqn.split(',')

            for nqn in all_host_nqns:
                response = NVMeoFClient(gw_group=gw_group).stub.add_host(
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
                         "gw_group": Param(str, "NVMeoF gateway group")
                     })
        @empty_response
        @handle_nvmeof_error
        @DeletePermission
        def remove(self, subsystem_nqn: str, host_nqn: str, gw_group: str):
            response = None
            to_delete_nqns = host_nqn.split(',')

            for del_nqn in to_delete_nqns:
                response = NVMeoFClient(gw_group=gw_group).stub.remove_host(
                    NVMeoFClient.pb2.remove_host_req(subsystem_nqn=subsystem_nqn, host_nqn=del_nqn)
                )
                if response.status != 0:
                    return response
                logger.info("removed host %s from subsystem %s", del_nqn, subsystem_nqn)

            return response
