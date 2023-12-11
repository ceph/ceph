# # import grpc

# from .proto import gateway_pb2 as pb2
# from .proto import gateway_pb2_grpc as pb2_grpc


# class NVMeoFClient(object):
#     def __init__(self):
#         self.host = '192.168.100.102'

# from ..cephnvmeof.control.cli import GatewayClient

from typing import Optional
from ..security import Scope
from ..services.nvmeof_client import NVMeoFClient
# from ..services.proto import gateway_pb2 as pb2
from . import APIDoc, APIRouter, RESTController, Endpoint, ReadPermission, CreatePermission, \
    DeletePermission, allow_empty_body, UpdatePermission
    

@APIRouter('/nvmeof', Scope.NVME_OF)
@APIDoc('NVMe-oF Management API', 'NVMe-oF')
class Nvmeof(RESTController):
    @ReadPermission
    def list(self):
        """List all NVMeoF gateways"""
        return NVMeoFClient().get_subsystems()


@APIRouter('/nvmeof/bdev', Scope.NVME_OF)
@APIDoc('NVMe-oF Block Device Management API', 'NVMe-oF')
class NvmeofBdev(RESTController):
    @CreatePermission
    def create(self, name: str, rbd_pool: str, rbd_image: str, block_size: int, uuid: Optional[str] = None):
        """Create a new NVMeoF block device"""
        return NVMeoFClient().create_bdev(name, rbd_pool, rbd_image, block_size, uuid)
    
    @DeletePermission
    @allow_empty_body
    def delete(self, name: str, force: bool):
        """Delete an existing NVMeoF block device"""
        return NVMeoFClient().delete_bdev(name, force)
    
    @Endpoint('PUT')
    @UpdatePermission
    @allow_empty_body
    def resize(self, name: str, size: int):
        """Resize an existing NVMeoF block device"""
        return NVMeoFClient().resize_bdev(name, size)


@APIRouter('/nvmeof/namespace', Scope.NVME_OF)
@APIDoc('NVMe-oF Namespace Management API', 'NVMe-oF')
class NvmeofNamespace(RESTController):
    @CreatePermission
    def create(self, subsystem_nqn: str, bdev_name: str, nsid: int, anagrpid: Optional[str] = None):
        """Create a new NVMeoF namespace"""
        return NVMeoFClient().create_namespace(subsystem_nqn, bdev_name, nsid, anagrpid)
    
    @Endpoint('DELETE', path='{subsystem_nqn}')
    def delete(self, subsystem_nqn: str, nsid: int):
        """Delete an existing NVMeoF namespace"""
        return NVMeoFClient().delete_namespace(subsystem_nqn, nsid)
    
@APIRouter('/nvmeof/subsystem', Scope.NVME_OF)
@APIDoc('NVMe-oF Subsystem Management API', 'NVMe-oF')
class NvmeofSubsystem(RESTController):
    @CreatePermission
    def create(self, subsystem_nqn: str, serial_number: str, max_namespaces: int,
                         ana_reporting: bool, enable_ha: bool) :
        """Create a new NVMeoF subsystem"""
        return NVMeoFClient().create_subsystem(subsystem_nqn, serial_number, max_namespaces,
                                               ana_reporting, enable_ha)
    
    @Endpoint('DELETE', path='{subsystem_nqn}')
    def delete(self, subsystem_nqn: str):
        """Delete an existing NVMeoF subsystem"""
        return NVMeoFClient().delete_subsystem(subsystem_nqn)


@APIRouter('/nvmeof/hosts', Scope.NVME_OF)
@APIDoc('NVMe-oF Host Management API', 'NVMe-oF')
class NvmeofHost(RESTController):
    @CreatePermission
    def create(self, subsystem_nqn: str, host_nqn: str):
        """Create a new NVMeoF host"""
        return NVMeoFClient().add_host(subsystem_nqn, host_nqn)
    
    @Endpoint('DELETE')
    def delete(self, subsystem_nqn: str, host_nqn: str):
        """Delete an existing NVMeoF host"""
        return NVMeoFClient().remove_host(subsystem_nqn, host_nqn)


@APIRouter('/nvmeof/listener', Scope.NVME_OF)
@APIDoc('NVMe-oF Listener Management API', 'NVMe-oF')
class NvmeofListener(RESTController):
    @CreatePermission
    def create(self, nqn: str, gateway: str, trtype: str, adrfam: str,
               traddr: str, trsvcid: str):
        """Create a new NVMeoF listener"""
        return NVMeoFClient().create_listener(nqn, gateway, trtype, adrfam, traddr, trsvcid)
    
    @Endpoint('DELETE')
    def delete(self, nqn: str, gateway: str, trtype, adrfam,
               traddr: str, trsvcid: str):
        """Delete an existing NVMeoF listener"""
        return NVMeoFClient().delete_listener(nqn, gateway, trtype, adrfam, traddr, trsvcid)