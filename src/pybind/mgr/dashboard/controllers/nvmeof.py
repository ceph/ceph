# # import grpc

# from .proto import gateway_pb2 as pb2
# from .proto import gateway_pb2_grpc as pb2_grpc


# class NVMeoFClient(object):
#     def __init__(self):
#         self.host = '192.168.100.102'

# from ..cephnvmeof.control.cli import GatewayClient

from ..security import Scope
from ..services.nvmeof_client import NVMeoFClient
from . import APIDoc, APIRouter, RESTController, Endpoint, ReadPermission, CreatePermission

@APIRouter('/nvmeof', Scope.ISCSI)
@APIDoc('NVMe-oF Management API', 'NVMe-oF')
class Nvmeof(RESTController):
    @ReadPermission
    def list(self):
        """List all NVMeoF gateways"""
        return NVMeoFClient().get_subsystems()
