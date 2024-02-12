# -*- coding: utf-8 -*-
import json
from typing import Optional

from ..security import Scope
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    EndpointDoc, ReadPermission, RESTController

try:
    from google.protobuf.json_format import MessageToJson

    from ..services.nvmeof_client import NVMeoFClient
except ImportError:
    MessageToJson = None
else:
    @APIRouter('/nvmeof/namespace', Scope.NVME_OF)
    @APIDoc('NVMe-oF Namespace Management API', 'NVMe-oF')
    class NvmeofNamespace(RESTController):
        @ReadPermission
        def list(self, subsystem_nqn: str):
            """
            List all NVMeoF namespaces
            """
            response = MessageToJson(NVMeoFClient().list_namespaces(subsystem_nqn))
            return json.loads(response)

        @CreatePermission
        def create(self, rbd_pool: str, rbd_image: str, subsystem_nqn: str,
                   create_image: Optional[bool] = True, image_size: Optional[int] = 1024,
                   block_size: int = 512):
            """
            Create a new NVMeoF namespace
            :param rbd_pool: RBD pool name
            :param rbd_image: RBD image name
            :param subsystem_nqn: NVMeoF subsystem NQN
            :param create_image: Create RBD image
            :param image_size: RBD image size
            :param block_size: NVMeoF namespace block size
            """
            response = NVMeoFClient().create_namespace(rbd_pool, rbd_image,
                                                       subsystem_nqn, block_size,
                                                       create_image, image_size)
            return json.loads(MessageToJson(response))

        @Endpoint('DELETE', path='{subsystem_nqn}')
        @DeletePermission
        def delete(self, subsystem_nqn: str):
            """
            Delete an existing NVMeoF namespace
            :param subsystem_nqn: NVMeoF subsystem NQN
            """
            response = NVMeoFClient().delete_namespace(subsystem_nqn)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/subsystem', Scope.NVME_OF)
    @APIDoc('NVMe-oF Subsystem Management API', 'NVMe-oF')
    class NvmeofSubsystem(RESTController):
        @ReadPermission
        @EndpointDoc("List all NVMeoF gateways",
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN'),
                     })
        @ReadPermission
        def list(self, subsystem_nqn: Optional[str] = None):
            response = MessageToJson(NVMeoFClient().list_subsystems(
                subsystem_nqn=subsystem_nqn))

            return json.loads(response)

        @CreatePermission
        def create(self, subsystem_nqn: str, serial_number: Optional[str] = None,
                   max_namespaces: Optional[int] = 256):
            """
            Create a new NVMeoF subsystem

            :param subsystem_nqn: NVMeoF subsystem NQN
            :param serial_number: NVMeoF subsystem serial number
            :param max_namespaces: NVMeoF subsystem maximum namespaces
            """
            response = NVMeoFClient().create_subsystem(subsystem_nqn, serial_number, max_namespaces)
            return json.loads(MessageToJson(response))

        @DeletePermission
        @Endpoint('DELETE', path='{subsystem_nqn}')
        def delete(self, subsystem_nqn: str, force: Optional[bool] = False):
            """
            Delete an existing NVMeoF subsystem
            :param subsystem_nqn: NVMeoF subsystem NQN
            :param force: Force delete
            """
            response = NVMeoFClient().delete_subsystem(subsystem_nqn, force)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/hosts', Scope.NVME_OF)
    @APIDoc('NVMe-oF Host Management API', 'NVMe-oF')
    class NvmeofHost(RESTController):
        @ReadPermission
        def list(self, subsystem_nqn: str):
            """
            List all NVMeoF hosts
            :param subsystem_nqn: NVMeoF subsystem NQN
            """
            response = MessageToJson(NVMeoFClient().list_hosts(subsystem_nqn))
            return json.loads(response)

        @CreatePermission
        def create(self, subsystem_nqn: str, host_nqn: str):
            """
            Create a new NVMeoF host
            :param subsystem_nqn: NVMeoF subsystem NQN
            :param host_nqn: NVMeoF host NQN
            """
            response = NVMeoFClient().add_host(subsystem_nqn, host_nqn)
            return json.loads(MessageToJson(response))

        @DeletePermission
        def delete(self, subsystem_nqn: str, host_nqn: str):
            """
            Delete an existing NVMeoF host
            :param subsystem_nqn: NVMeoF subsystem NQN
            :param host_nqn: NVMeoF host NQN
            """
            response = NVMeoFClient().remove_host(subsystem_nqn, host_nqn)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/listener', Scope.NVME_OF)
    @APIDoc('NVMe-oF Listener Management API', 'NVMe-oF')
    class NvmeofListener(RESTController):
        @ReadPermission
        def list(self, subsystem_nqn: str):
            """
            List all NVMeoF listeners
            :param nqn: NVMeoF subsystem NQN
            """
            response = MessageToJson(NVMeoFClient().list_listeners(subsystem_nqn))
            return json.loads(response)

        @CreatePermission
        def create(self, nqn: str, gateway: str, traddr: Optional[str] = None):
            """
            Create a new NVMeoF listener
            :param nqn: NVMeoF subsystem NQN
            :param gateway: NVMeoF gateway
            :param traddr: NVMeoF transport address
            """
            response = NVMeoFClient().create_listener(nqn, gateway, traddr)
            return json.loads(MessageToJson(response))

        @DeletePermission
        def delete(self, nqn: str, gateway: str, traddr: Optional[str] = None):
            """
            Delete an existing NVMeoF listener
            :param nqn: NVMeoF subsystem NQN
            :param gateway: NVMeoF gateway
            :param traddr: NVMeoF transport address
            """
            response = NVMeoFClient().delete_listener(nqn, gateway, traddr)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/gateway', Scope.NVME_OF)
    @APIDoc('NVMe-oF Gateway Management API', 'NVMe-oF')
    class NvmeofGateway(RESTController):
        @ReadPermission
        @Endpoint()
        def info(self):
            """
            Get NVMeoF gateway information
            """
            response = MessageToJson(NVMeoFClient().gateway_info())
            return json.loads(response)
