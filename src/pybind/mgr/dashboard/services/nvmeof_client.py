import logging
from typing import Optional

from ..tools import str_to_bool
from .nvmeof_conf import NvmeofGatewaysConfig

logger = logging.getLogger('nvmeof_client')

try:
    import grpc

    from .proto import gateway_pb2 as pb2
    from .proto import gateway_pb2_grpc as pb2_grpc
except ImportError:
    grpc = None
else:
    class NVMeoFClient(object):
        def __init__(self):
            logger.info('Initiating nvmeof gateway connection...')

            self.gateway_addr = list(NvmeofGatewaysConfig.get_gateways_config()[
                                     'gateways'].values())[0]['service_url']
            self.channel = grpc.insecure_channel(
                '{}'.format(self.gateway_addr)
            )
            logger.info('Found nvmeof gateway at %s', self.gateway_addr)
            self.stub = pb2_grpc.GatewayStub(self.channel)

        def list_subsystems(self, subsystem_nqn: Optional[str] = None,
                            serial_number: Optional[str] = None):
            return self.stub.list_subsystems(pb2.list_subsystems_req(
                subsystem_nqn=subsystem_nqn,
                serial_number=serial_number
            ))

        def create_subsystem(self, subsystem_nqn: str, serial_number: str, max_namespaces: int,
                             ana_reporting: bool, enable_ha: bool):
            return self.stub.create_subsystem(pb2.create_subsystem_req(
                subsystem_nqn=subsystem_nqn,
                serial_number=serial_number,
                max_namespaces=int(max_namespaces),
                ana_reporting=str_to_bool(ana_reporting),
                enable_ha=str_to_bool(enable_ha)
            ))

        def delete_subsystem(self, subsystem_nqn: str):
            return self.stub.delete_subsystem(pb2.delete_subsystem_req(
                subsystem_nqn=subsystem_nqn
            ))

        def list_namespaces(self, subsystem_nqn: str, nsid: Optional[int] = 1,
                            uuid: Optional[str] = None):
            return self.stub.list_namespaces(pb2.list_namespaces_req(
                subsystem=subsystem_nqn,
                nsid=int(nsid),
                uuid=uuid
            ))

        def create_namespace(self, rbd_pool_name: str, rbd_image_name: str,
                             subsystem_nqn: str, block_size: int = 512,
                             nsid: Optional[int] = 1, uuid: Optional[str] = None,
                             anagrpid: Optional[int] = 1, create_image: Optional[bool] = True,
                             size: Optional[int] = 1024):
            return self.stub.namespace_add(pb2.namespace_add_req(
                rbd_pool_name=rbd_pool_name,
                rbd_image_name=rbd_image_name,
                subsystem_nqn=subsystem_nqn,
                nsid=int(nsid),
                block_size=block_size,
                uuid=uuid,
                anagrpid=anagrpid,
                create_image=create_image,
                size=size
            ))

        def delete_namespace(self, subsystem_nqn: str, nsid: int):
            return self.stub.remove_namespace(pb2.remove_namespace_req(
                subsystem_nqn=subsystem_nqn,
                nsid=nsid
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

        def create_listener(self, nqn: str, gateway: str, traddr: Optional[str] = None,
                            transport_type: Optional[str] = 'TCP',
                            addr_family: Optional[str] = 'IPV4',
                            transport_svc_id: Optional[int] = 4420,
                            auto_ha_state: Optional[str] = 'AUTO_HA_UNSET'):
            traddr = None
            if traddr is None:
                addr = self.gateway_addr
                ip_address, _ = addr.split(':')
                traddr = self._escape_address_if_ipv6(ip_address)

            req = pb2.create_listener_req(
                nqn=nqn,
                gateway_name=gateway,
                traddr=traddr,
                trtype=pb2.TransportType.Value(transport_type.upper()),
                adrfam=pb2.AddressFamily.Value(addr_family.lower()),
                trsvcid=transport_svc_id,
                auto_ha_state=pb2.AutoHAState.Value(auto_ha_state.upper())
            )
            return self.stub.create_listener(req)

        def delete_listener(self, nqn: str, gateway: str, traddr: Optional[str] = None,
                            transport_type: Optional[str] = 'TCP',
                            addr_family: Optional[str] = 'IPV4',
                            transport_svc_id: Optional[int] = 4420):
            traddr = None
            if traddr is None:
                addr = self.gateway_addr
                ip_address, _ = addr.split(':')
                traddr = self._escape_address_if_ipv6(ip_address)

            return self.stub.delete_listener(pb2.delete_listener_req(
                nqn=nqn,
                gateway_name=gateway,
                traddr=traddr,
                trtype=pb2.TransportType.Value(transport_type.upper()),
                adrfam=pb2.AddressFamily.Value(addr_family.lower()),
                trsvcid=int(transport_svc_id)
            ))

        def gateway_info(self):
            return self.stub.get_gateway_info(pb2.get_gateway_info_req())

        def _escape_address_if_ipv6(self, addr):
            ret_addr = addr
            if ":" in addr and not addr.strip().startswith("["):
                ret_addr = f"[{addr}]"
            return ret_addr
