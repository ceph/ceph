from enum import Enum
from typing import Optional
import grpc
import json

import logging

from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc

from google.protobuf.json_format import MessageToJson

from .nvmeof_conf import NvmeofGatewaysConfig
from ..tools import str_to_bool

logger = logging.getLogger('nvmeof_client')


class NVMeoFClient(object):
    def __init__(self):
        logger.info('Initiating nvmeof gateway connection...')

        self.gateway_addr = list(NvmeofGatewaysConfig.get_gateways_config()['gateways'].values())[0]['service_url']
        self.channel = grpc.insecure_channel(
            '{}'.format(self.gateway_addr)
        )
        logger.info('Found nvmeof gateway at {}'.format(self.gateway_addr))
        self.stub = pb2_grpc.GatewayStub(self.channel)

    def get_subsystems(self):
        response = self.stub.get_subsystems(pb2.get_subsystems_req())
        return json.loads(MessageToJson(response))
    
    def create_bdev(self, name: str, rbd_pool: str, rbd_image: str, block_size: int, uuid: Optional[str] = None):
        response = self.stub.create_bdev(pb2.create_bdev_req(
            bdev_name=name,
            rbd_pool_name=rbd_pool,
            rbd_image_name=rbd_image,
            block_size=block_size,
            uuid=uuid
        ))
        return json.loads(MessageToJson(response))
    
    def resize_bdev(self, name: str, size: int):
        response = self.stub.resize_bdev(pb2.resize_bdev_req(
            bdev_name=name,
            new_size=size
        ))
        return json.loads(MessageToJson(response))
    
    def delete_bdev(self, name: str, force: bool):
        response = self.stub.delete_bdev(pb2.delete_bdev_req(
            bdev_name=name,
            force=str_to_bool(force)
        ))
        return json.loads(MessageToJson(response))
    
    def create_subsystem(self, subsystem_nqn: str, serial_number: str, max_namespaces: int,
                         ana_reporting: bool, enable_ha: bool) :
        response = self.stub.create_subsystem(pb2.create_subsystem_req(
            subsystem_nqn=subsystem_nqn,
            serial_number=serial_number,
            max_namespaces=int(max_namespaces),
            ana_reporting=str_to_bool(ana_reporting),
            enable_ha=str_to_bool(enable_ha)
        ))
        return json.loads(MessageToJson(response))
    
    def delete_subsystem(self, subsystem_nqn: str):
        response = self.stub.delete_subsystem(pb2.delete_subsystem_req(
            subsystem_nqn=subsystem_nqn
        ))
        return json.loads(MessageToJson(response))
    
    def create_namespace(self, subsystem_nqn: str, bdev_name: str, nsid: int, anagrpid: Optional[str] = None):
        response = self.stub.add_namespace(pb2.add_namespace_req(
            subsystem_nqn=subsystem_nqn,
            bdev_name=bdev_name,
            nsid=int(nsid),
            anagrpid=anagrpid
        ))
        return json.loads(MessageToJson(response))
    
    def delete_namespace(self, subsystem_nqn: str, nsid: int):
        response = self.stub.remove_namespace(pb2.remove_namespace_req(
            subsystem_nqn=subsystem_nqn,
            nsid=nsid
        ))
        return json.loads(MessageToJson(response))
    
    def add_host(self, subsystem_nqn: str, host_nqn: str):
        response = self.stub.add_host(pb2.add_host_req(
            subsystem_nqn=subsystem_nqn,
            host_nqn=host_nqn
        ))
        return json.loads(MessageToJson(response))
    
    def remove_host(self, subsystem_nqn: str, host_nqn: str):
        response = self.stub.remove_host(pb2.remove_host_req(
            subsystem_nqn=subsystem_nqn,
            host_nqn=host_nqn
        ))
        return json.loads(MessageToJson(response))

    def create_listener(self, nqn: str, gateway: str, trtype: str, adrfam: str,
                        traddr: str, trsvcid: str):
        req = pb2.create_listener_req(
                nqn=nqn,
                gateway_name=gateway,
                trtype=pb2.TransportType.Value(trtype.upper()),
                adrfam=pb2.AddressFamily.Value(adrfam.lower()),
                traddr=traddr,
                trsvcid=trsvcid,
            )
        ret = self.stub.create_listener(req)
        return json.loads(MessageToJson(ret))
    
    def delete_listener(self, nqn: str, gateway: str, trttype, adrfam,
                        traddr: str, trsvcid: str):
        response = self.stub.delete_listener(pb2.delete_listener_req(
            nqn=nqn,
            gateway_name=gateway,
            trtype=trttype,
            adrfam=adrfam,
            traddr=traddr,
            trsvcid=trsvcid
        ))
        return json.loads(MessageToJson(response))
