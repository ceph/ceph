import grpc
import json

import logging

from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc

from google.protobuf.json_format import MessageToJson

from .nvmeof_conf import NvmeofGatewaysConfig

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
