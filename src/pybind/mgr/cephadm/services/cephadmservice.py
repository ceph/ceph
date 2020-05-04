from typing import  TYPE_CHECKING

from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class CephadmService:
    """
    Base class for service types. Often providing a create() and config() fn.
    """
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr


class MonService(CephadmService):
    def create(self, name, host, network):
        """
        Create a new monitor on the given host.
        """
        # get mon. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': 'mon.',
        })

        extra_config = '[mon.%s]\n' % name
        if network:
            # infer whether this is a CIDR network, addrvec, or plain IP
            if '/' in network:
                extra_config += 'public network = %s\n' % network
            elif network.startswith('[v') and network.endswith(']'):
                extra_config += 'public addrv = %s\n' % network
            elif ':' not in network:
                extra_config += 'public addr = %s\n' % network
            else:
                raise OrchestratorError('Must specify a CIDR network, ceph addrvec, or plain IP: \'%s\'' % network)
        else:
            # try to get the public_network from the config
            ret, network, err = self.mgr.check_mon_command({
                'prefix': 'config get',
                'who': 'mon',
                'key': 'public_network',
            })
            network = network.strip() # type: ignore
            if not network:
                raise OrchestratorError('Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP')
            if '/' not in network:
                raise OrchestratorError('public_network is set but does not look like a CIDR network: \'%s\'' % network)
            extra_config += 'public network = %s\n' % network

        return self.mgr._create_daemon('mon', name, host,
                                       keyring=keyring,
                                       extra_config={'config': extra_config})