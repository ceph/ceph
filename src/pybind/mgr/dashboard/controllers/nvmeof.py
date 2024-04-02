# -*- coding: utf-8 -*-
from typing import Optional

from ..model import nvmeof as model
from ..security import Scope
from ..tools import str_to_bool
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, Param, ReadPermission, RESTController

try:
    from ..services.nvmeof_client import NVMeoFClient, empty_response, \
        handle_nvmeof_error, map_collection, map_model
except ImportError:
    pass
else:
    @APIRouter("/nvmeof/gateway", Scope.NVME_OF)
    class NVMeoFGateway(RESTController):
        """
        NVMe-oF Gateway

        NVMe-oF Gateway Management API
        """
        @EndpointDoc("Get information about the NVMeoF gateway")
        @map_model(model.GatewayInfo)
        @handle_nvmeof_error
        def list(self):
            return NVMeoFClient().stub.get_gateway_info(
                NVMeoFClient.pb2.get_gateway_info_req()
            )

    @APIRouter("/nvmeof/subsystem", Scope.NVME_OF)
    class NVMeoFSubsystem(RESTController):
        """
        NVMe-oF Subsystem: NVMe-oF Subsystem Management API
        """
        @EndpointDoc("List all NVMeoF subsystems")
        @map_collection(model.Subsystem, pick="subsystems")
        @handle_nvmeof_error
        def list(self):
            return NVMeoFClient().stub.list_subsystems(
                NVMeoFClient.pb2.list_subsystems_req()
            )

        @EndpointDoc(
            "Get information from a specific NVMeoF subsystem",
            parameters={"nqn": Param(str, "NVMeoF subsystem NQN")},
        )
        @map_model(model.Subsystem, first="subsystems")
        @handle_nvmeof_error
        def get(self, nqn: str):
            return NVMeoFClient().stub.list_subsystems(
                NVMeoFClient.pb2.list_subsystems_req(subsystem_nqn=nqn)
            )

        @EndpointDoc(
            "Create a new NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "max_namespaces": Param(int, "Maximum number of namespaces", True, 256),
                "enable_ha": Param(bool, "Enable high availability"),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def create(self, nqn: str, enable_ha: bool, max_namespaces: int = 256):
            return NVMeoFClient().stub.create_subsystem(
                NVMeoFClient.pb2.create_subsystem_req(
                    subsystem_nqn=nqn, max_namespaces=max_namespaces, enable_ha=enable_ha
                )
            )

        @EndpointDoc(
            "Delete an existing NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "force": Param(bool, "Force delete", "false"),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(self, nqn: str, force: Optional[str] = "false"):
            return NVMeoFClient().stub.delete_subsystem(
                NVMeoFClient.pb2.delete_subsystem_req(
                    subsystem_nqn=nqn, force=str_to_bool(force)
                )
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/listener", Scope.NVME_OF)
    class NVMeoFListener(RESTController):
        """
        NVMe-oF Subsystem Listener: NVMe-oF Subsystem Listener Management API
        """
        @EndpointDoc(
            "List all NVMeoF listeners",
            parameters={"nqn": Param(str, "NVMeoF subsystem NQN")},
        )
        @map_collection(model.Listener, pick="listeners")
        @handle_nvmeof_error
        def list(self, nqn: str):
            return NVMeoFClient().stub.list_listeners(
                NVMeoFClient.pb2.list_listeners_req(subsystem=nqn)
            )

        @EndpointDoc(
            "Create a new NVMeoF listener",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gateway": Param(str, "NVMeoF gateway"),
                "traddr": Param(str, "NVMeoF transport address"),
                "trsvcid": Param(int, "NVMeoF transport service port"),
                "adrfam": Param(str, "NVMeoF address family"),
                "trtype": Param(str, "NVMeoF transport type"),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def create(
            self,
            nqn: str,
            gateway: str,
            traddr: str,
            trsvcid: Optional[int] = 4420,
            adrfam: Optional[str] = "ipv4",
        ):
            return NVMeoFClient().stub.create_listener(
                NVMeoFClient.pb2.create_listener_req(
                    nqn=nqn,
                    gateway_name=gateway,
                    traddr=traddr,
                    trsvcid=trsvcid,
                    adrfam=adrfam,
                )
            )

        @EndpointDoc(
            "Delete an existing NVMeoF listener",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "gateway": Param(str, "NVMeoF gateway"),
                "traddr": Param(str, "NVMeoF transport address"),
                "trsvid": Param(int, "NVMeoF transport service port"),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(self, nqn: str, gateway: str, traddr: Optional[str] = None,
                   trsvcid: Optional[int] = 4420):
            return NVMeoFClient().stub.delete_listener(
                NVMeoFClient.pb2.delete_listener_req(
                    nqn=nqn, gateway_name=gateway, traddr=traddr, trsvcid=int(trsvcid)
                )
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/namespace", Scope.NVME_OF)
    class NVMeoFNamespace(RESTController):
        """
        NVMe-oF Subsystem Namespace: NVMe-oF Subsystem Namespace Management API
        """
        @EndpointDoc(
            "List all NVMeoF namespaces in a subsystem",
            parameters={"nqn": Param(str, "NVMeoF subsystem NQN")},
        )
        @map_collection(model.Namespace, pick="namespaces")
        @handle_nvmeof_error
        def list(self, nqn: str):
            return NVMeoFClient().stub.list_namespaces(
                NVMeoFClient.pb2.list_namespaces_req(subsystem=nqn)
            )

        @EndpointDoc(
            "Get info from specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
            },
        )
        @map_model(model.Namespace, first="namespaces")
        @handle_nvmeof_error
        def get(self, nqn: str, nsid: str):
            return NVMeoFClient().stub.list_namespaces(
                NVMeoFClient.pb2.list_namespaces_req(subsystem=nqn, nsid=int(nsid))
            )

        @ReadPermission
        @Endpoint('GET', '{nsid}/io_stats')
        @EndpointDoc(
            "Get IO stats from specified NVMeoF namespace",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "nsid": Param(str, "NVMeoF Namespace ID"),
            },
        )
        @map_model(model.NamespaceIOStats)
        @handle_nvmeof_error
        def io_stats(self, nqn: str, nsid: str):
            return NVMeoFClient().stub.namespace_get_io_stats(
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
        ):
            return NVMeoFClient().stub.namespace_add(
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
        ):
            if rbd_image_size:
                mib = 1024 * 1024
                new_size_mib = int((rbd_image_size + mib - 1) / mib)

                response = NVMeoFClient().stub.namespace_resize(
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
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(self, nqn: str, nsid: str):
            return NVMeoFClient().stub.namespace_delete(
                NVMeoFClient.pb2.namespace_delete_req(subsystem_nqn=nqn, nsid=int(nsid))
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/host", Scope.NVME_OF)
    class NVMeoFHost(RESTController):
        """
        NVMe-oF Subsystem Host Allowlist:

        NVMe-oF Subsystem Host Allowlist Management API
        """
        @EndpointDoc(
            "List all allowed hosts for an NVMeoF subsystem",
            parameters={"nqn": Param(str, "NVMeoF subsystem NQN")},
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
        def list(self, nqn: str):
            return NVMeoFClient().stub.list_hosts(
                NVMeoFClient.pb2.list_hosts_req(subsystem=nqn)
            )

        @EndpointDoc(
            "Allow hosts to access an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to allow any host.'),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def create(self, nqn: str, host_nqn: str):
            return NVMeoFClient().stub.add_host(
                NVMeoFClient.pb2.add_host_req(subsystem_nqn=nqn, host_nqn=host_nqn)
            )

        @EndpointDoc(
            "Disallow hosts from accessing an NVMeoF subsystem",
            parameters={
                "nqn": Param(str, "NVMeoF subsystem NQN"),
                "host_nqn": Param(str, 'NVMeoF host NQN. Use "*" to disallow any host.'),
            },
        )
        @empty_response
        @handle_nvmeof_error
        def delete(self, nqn: str, host_nqn: str):
            return NVMeoFClient().stub.remove_host(
                NVMeoFClient.pb2.remove_host_req(subsystem_nqn=nqn, host_nqn=host_nqn)
            )

    @APIRouter("/nvmeof/subsystem/{nqn}/connection", Scope.NVME_OF)
    class NVMeoFConnection(RESTController):
        """
        NVMe-oF Subsystem Connection: NVMe-oF Subsystem Connection Management API
        """
        @EndpointDoc(
            "List all NVMeoF Subsystem Connections",
            parameters={"nqn": Param(str, "NVMeoF subsystem NQN")},
        )
        @map_collection(model.Connection, pick="connections")
        @handle_nvmeof_error
        def list(self, nqn: str):
            return NVMeoFClient().stub.list_connections(
                NVMeoFClient.pb2.list_connections_req(subsystem=nqn)
            )
