import logging
import threading
from typing import Optional

from .nvmeof_conf import NvmeofGatewaysConfig

logger = logging.getLogger('nvmeof_client')

try:
    import grpc

    from .proto import gateway_pb2 as pb2
    from .proto import gateway_pb2_grpc as pb2_grpc
except ImportError:
    grpc = None
else:
    class ChannelPool:
        def __init__(self):
            self.gateway_addr = list(NvmeofGatewaysConfig.get_gateways_config()[
                                     'gateways'].values())[0]['service_url']
            self.pool_lock = threading.Lock()
            self.channels = []

        def get_channel(self):
            with self.pool_lock:
                if self.channels:
                    return self.channels.pop()

            return self.create_channel()

        def create_channel(self):
            logger.info('Found nvmeof gateway at %s', self.gateway_addr)
            return grpc.insecure_channel(self.gateway_addr)

        def release_channel(self, channel):
            with self.pool_lock:
                self.channels.append(channel)

    class NVMeoFClient:
        def __init__(self):
            logger.info('Initiating nvmeof gateway connection...')
            self.channel_pool = ChannelPool()
            self.channel = self.channel_pool.get_channel()
            self.stub = pb2_grpc.GatewayStub(self.channel)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):  # noqa pylint: disable=unused-argument
            if self.channel:
                self.channel_pool.release_channel(self.channel)
                self.channel = None

        def list_subsystems(self, subsystem_nqn: Optional[str] = None):
            return self.stub.list_subsystems(pb2.list_subsystems_req(
                subsystem_nqn=subsystem_nqn
            ))

        def create_subsystem(self, subsystem_nqn: str):
            return self.stub.create_subsystem(pb2.create_subsystem_req(
                subsystem_nqn=subsystem_nqn
            ))

        def delete_subsystem(self, subsystem_nqn: str, force: Optional[bool] = False):
            return self.stub.delete_subsystem(pb2.delete_subsystem_req(
                subsystem_nqn=subsystem_nqn,
                force=force
            ))

        def list_namespaces(self, subsystem_nqn: str):
            return self.stub.list_namespaces(pb2.list_namespaces_req(
                subsystem=subsystem_nqn
            ))

        def create_namespace(self, rbd_pool_name: str, rbd_image_name: str,
                             subsystem_nqn: str, block_size: int = 512,
                             create_image: Optional[bool] = True,
                             size: Optional[int] = 1024):
            return self.stub.namespace_add(pb2.namespace_add_req(
                rbd_pool_name=rbd_pool_name,
                rbd_image_name=rbd_image_name,
                subsystem_nqn=subsystem_nqn,
                block_size=block_size,
                create_image=create_image,
                size=size
            ))

        def delete_namespace(self, subsystem_nqn: str):
            return self.stub.namespace_delete(pb2.namespace_delete_req(
                subsystem_nqn=subsystem_nqn
            ))

        def list_hosts(self, subsystem_nqn: str):
            return self.stub.list_hosts(pb2.list_hosts_req(
                subsystem=subsystem_nqn
            ))

        def add_host(self, subsystem_nqn: str, host_nqn: str):
            return self.stub.add_host(pb2.add_host_req(
                subsystem_nqn=subsystem_nqn,
                host_nqn=host_nqn
            ))

        def remove_host(self, subsystem_nqn: str, host_nqn: str):
            return self.stub.remove_host(pb2.remove_host_req(
                subsystem_nqn=subsystem_nqn,
                host_nqn=host_nqn
            ))

        def list_listeners(self, subsystem_nqn: str):
            return self.stub.list_listeners(pb2.list_listeners_req(
                subsystem=subsystem_nqn
            ))

        def create_listener(self, nqn: str, gateway: str, traddr: Optional[str] = None):
            if traddr is None:
                addr = self.gateway_addr
                ip_address, _ = addr.split(':')
                traddr = self._escape_address_if_ipv6(ip_address)

            req = pb2.create_listener_req(
                nqn=nqn,
                gateway_name=gateway,
                traddr=traddr
            )
            return self.stub.create_listener(req)

        def delete_listener(self, nqn: str, gateway: str, traddr: Optional[str] = None):
            if traddr is None:
                addr = self.gateway_addr
                ip_address, _ = addr.split(':')
                traddr = self._escape_address_if_ipv6(ip_address)

            return self.stub.delete_listener(pb2.delete_listener_req(
                nqn=nqn,
                gateway_name=gateway,
                traddr=traddr
            ))

        def gateway_info(self):
            return self.stub.get_gateway_info(pb2.get_gateway_info_req())

        def _escape_address_if_ipv6(self, addr):
            ret_addr = addr
            if ":" in addr and not addr.strip().startswith("["):
                ret_addr = f"[{addr}]"
            return ret_addr
