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
        @EndpointDoc('List all NVMeoF namespaces',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN')
                     })
        def list(self, subsystem_nqn: str):
            response = MessageToJson(NVMeoFClient().list_namespaces(subsystem_nqn))
            return json.loads(response)

        @CreatePermission
        @EndpointDoc('Create a new NVMeoF namespace',
                     parameters={
                         'rbd_pool': (str, 'RBD pool name'),
                         'rbd_image': (str, 'RBD image name'),
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN'),
                         'create_image': (bool, 'Create RBD image'),
                         'image_size': (int, 'RBD image size'),
                         'block_size': (int, 'NVMeoF namespace block size')
                     })
        def create(self, rbd_pool: str, rbd_image: str, subsystem_nqn: str,
                   create_image: Optional[bool] = True, image_size: Optional[int] = 1024,
                   block_size: int = 512):
            response = NVMeoFClient().create_namespace(rbd_pool, rbd_image,
                                                       subsystem_nqn, block_size,
                                                       create_image, image_size)
            return json.loads(MessageToJson(response))

        @Endpoint('DELETE', path='{subsystem_nqn}')
        @EndpointDoc('Delete an existing NVMeoF namespace',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN')
                     })
        @DeletePermission
        def delete(self, subsystem_nqn: str):
            response = NVMeoFClient().delete_namespace(subsystem_nqn)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/subsystem', Scope.NVME_OF)
    @APIDoc('NVMe-oF Subsystem Management API', 'NVMe-oF')
    class NvmeofSubsystem(RESTController):
        @ReadPermission
        @EndpointDoc("List all NVMeoF subsystems",
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN'),
                     })
        @ReadPermission
        def list(self, subsystem_nqn: Optional[str] = None):
            response = MessageToJson(NVMeoFClient().list_subsystems(
                subsystem_nqn=subsystem_nqn))

            return json.loads(response)

        @CreatePermission
        @EndpointDoc('Create a new NVMeoF subsystem',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN'),
                         'serial_number': (str, 'NVMeoF subsystem serial number'),
                         'max_namespaces': (int, 'Maximum number of namespaces')
                     })
        def create(self, subsystem_nqn: str):
            response = NVMeoFClient().create_subsystem(subsystem_nqn)
            return json.loads(MessageToJson(response))

        @DeletePermission
        @Endpoint('DELETE', path='{subsystem_nqn}')
        @EndpointDoc('Delete an existing NVMeoF subsystem',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN'),
                         'force': (bool, 'Force delete')
                     })
        def delete(self, subsystem_nqn: str, force: Optional[bool] = False):
            response = NVMeoFClient().delete_subsystem(subsystem_nqn, force)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/hosts', Scope.NVME_OF)
    @APIDoc('NVMe-oF Host Management API', 'NVMe-oF')
    class NvmeofHost(RESTController):
        @ReadPermission
        @EndpointDoc('List all allowed hosts for an NVMeoF subsystem',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN')
                     })
        def list(self, subsystem_nqn: str):
            response = MessageToJson(NVMeoFClient().list_hosts(subsystem_nqn))
            return json.loads(response)

        @CreatePermission
        @EndpointDoc('Allow hosts to access an NVMeoF subsystem',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN'),
                         'host_nqn': (str, 'NVMeoF host NQN')
                     })
        def create(self, subsystem_nqn: str, host_nqn: str):
            response = NVMeoFClient().add_host(subsystem_nqn, host_nqn)
            return json.loads(MessageToJson(response))

        @DeletePermission
        @EndpointDoc('Disallow hosts from accessing an NVMeoF subsystem',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN'),
                         'host_nqn': (str, 'NVMeoF host NQN')
                     })
        def delete(self, subsystem_nqn: str, host_nqn: str):
            response = NVMeoFClient().remove_host(subsystem_nqn, host_nqn)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/listener', Scope.NVME_OF)
    @APIDoc('NVMe-oF Listener Management API', 'NVMe-oF')
    class NvmeofListener(RESTController):
        @ReadPermission
        @EndpointDoc('List all NVMeoF listeners',
                     parameters={
                         'subsystem_nqn': (str, 'NVMeoF subsystem NQN')
                     })
        def list(self, subsystem_nqn: str):
            response = MessageToJson(NVMeoFClient().list_listeners(subsystem_nqn))
            return json.loads(response)

        @CreatePermission
        @EndpointDoc('Create a new NVMeoF listener',
                     parameters={
                         'nqn': (str, 'NVMeoF subsystem NQN'),
                         'gateway': (str, 'NVMeoF gateway'),
                         'traddr': (str, 'NVMeoF transport address')
                     })
        def create(self, nqn: str, gateway: str, traddr: Optional[str] = None):
            response = NVMeoFClient().create_listener(nqn, gateway, traddr)
            return json.loads(MessageToJson(response))

        @DeletePermission
        @EndpointDoc('Delete an existing NVMeoF listener',
                     parameters={
                         'nqn': (str, 'NVMeoF subsystem NQN'),
                         'gateway': (str, 'NVMeoF gateway'),
                         'traddr': (str, 'NVMeoF transport address')
                     })
        def delete(self, nqn: str, gateway: str, traddr: Optional[str] = None):
            response = NVMeoFClient().delete_listener(nqn, gateway, traddr)
            return json.loads(MessageToJson(response))

    @APIRouter('/nvmeof/gateway', Scope.NVME_OF)
    @APIDoc('NVMe-oF Gateway Management API', 'NVMe-oF')
    class NvmeofGateway(RESTController):
        @ReadPermission
        @Endpoint()
        @EndpointDoc('List all NVMeoF gateways')
        def info(self):
            response = MessageToJson(NVMeoFClient().gateway_info())
            return json.loads(response)
