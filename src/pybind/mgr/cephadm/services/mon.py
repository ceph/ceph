import json
from typing import List, Dict, Any, Tuple, cast
from ceph.deployment.service_spec import MONSpec, ServiceSpec
from ceph.deployment.utils import is_ipv6, unwrap_ipv6
from orchestrator import OrchestratorError, DaemonDescription
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext
import logging

logger = logging.getLogger(__name__)


@register_cephadm_service
class MonService(CephService):
    TYPE = 'mon'

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        """
        Create a new monitor on the given host.
        """
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        name, _, network = daemon_spec.daemon_id, daemon_spec.host, daemon_spec.network

        # get mon. key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': daemon_spec.entity_name(),
        })

        extra_config = '[mon.%s]\n' % name
        if network:
            # infer whether this is a CIDR network, addrvec, or plain IP
            if '/' in network:
                extra_config += 'public network = %s\n' % network
            elif network.startswith('[v') and network.endswith(']'):
                extra_config += 'public addrv = %s\n' % network
            elif is_ipv6(network):
                extra_config += 'public addr = %s\n' % unwrap_ipv6(network)
            elif ':' not in network:
                extra_config += 'public addr = %s\n' % network
            else:
                raise OrchestratorError(
                    'Must specify a CIDR network, ceph addrvec, or plain IP: \'%s\'' % network)
        else:
            # try to get the public_network from the config
            ret, network, err = self.mgr.check_mon_command({
                'prefix': 'config get',
                'who': 'mon',
                'key': 'public_network',
            })
            network = network.strip() if network else network
            if not network:
                raise OrchestratorError(
                    'Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP')
            if '/' not in network:
                raise OrchestratorError(
                    'public_network is set but does not look like a CIDR network: \'%s\'' % network)
            extra_config += 'public network = %s\n' % network

        daemon_spec.ceph_conf = extra_config
        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        return daemon_spec

    def config(self, spec: ServiceSpec) -> None:
        assert self.TYPE == spec.service_type
        self.set_crush_locations(self.mgr.cache.get_daemons_by_type('mon'), spec)

    def _get_quorum_status(self) -> Dict[Any, Any]:
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'quorum_status',
        })
        try:
            j = json.loads(out)
        except Exception as e:
            raise OrchestratorError(f'failed to parse mon quorum status: {e}')
        return j

    def _check_safe_to_destroy(self, mon_id: str) -> None:
        quorum_status = self._get_quorum_status()
        mons = [m['name'] for m in quorum_status['monmap']['mons']]
        if mon_id not in mons:
            logger.info('Safe to remove mon.%s: not in monmap (%s)' % (
                mon_id, mons))
            return
        new_mons = [m for m in mons if m != mon_id]
        new_quorum = [m for m in quorum_status['quorum_names'] if m != mon_id]
        if len(new_quorum) > len(new_mons) / 2:
            logger.info('Safe to remove mon.%s: new quorum should be %s (from %s)' %
                        (mon_id, new_quorum, new_mons))
            return
        raise OrchestratorError(
            'Removing %s would break mon quorum (new quorum %s, new mons %s)' % (mon_id, new_quorum, new_mons))

    def pre_remove(self, daemon: DaemonDescription) -> None:
        super().pre_remove(daemon)

        assert daemon.daemon_id is not None
        daemon_id: str = daemon.daemon_id
        self._check_safe_to_destroy(daemon_id)

        # remove mon from quorum before we destroy the daemon
        logger.info('Removing monitor %s from monmap...' % daemon_id)
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'mon rm',
            'name': daemon_id,
        })

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        # Do not remove the mon keyring.
        # super().post_remove(daemon)
        pass

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        daemon_spec.final_config, daemon_spec.deps = super().generate_config(deploy_ctx)

        # realistically, we expect there to always be a mon spec
        # in a real deployment, but the way teuthology deploys some daemons
        # it's possible there might not be. For that reason we need to
        # verify the service is present in the spec store.
        if daemon_spec.service_name in self.mgr.spec_store:
            mon_spec = cast(MONSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
            if mon_spec.crush_locations:
                if daemon_spec.host in mon_spec.crush_locations:
                    # the --crush-location flag only supports a single bucket=loc pair so
                    # others will have to be handled later. The idea is to set the flag
                    # for the first bucket=loc pair in the list in order to facilitate
                    # replacing a tiebreaker mon (https://docs.ceph.com/en/quincy/rados/operations/stretch-mode/#other-commands)
                    c_loc = mon_spec.crush_locations[daemon_spec.host][0]
                    daemon_spec.final_config['crush_location'] = c_loc

        return daemon_spec.final_config, daemon_spec.deps

    def set_crush_locations(self, daemon_descrs: List[DaemonDescription], spec: ServiceSpec) -> None:
        logger.debug('Setting mon crush locations from spec')
        if not daemon_descrs:
            return
        assert self.TYPE == spec.service_type
        mon_spec = cast(MONSpec, spec)

        if not mon_spec.crush_locations:
            return

        quorum_status = self._get_quorum_status()
        mons_in_monmap = [m['name'] for m in quorum_status['monmap']['mons']]
        for dd in daemon_descrs:
            assert dd.daemon_id is not None
            assert dd.hostname is not None
            if dd.hostname not in mon_spec.crush_locations:
                continue
            if dd.daemon_id not in mons_in_monmap:
                continue
            # expected format for crush_locations from the quorum status is
            # {bucket1=loc1,bucket2=loc2} etc. for the number of bucket=loc pairs
            try:
                current_crush_locs = [m['crush_location'] for m in quorum_status['monmap']['mons'] if m['name'] == dd.daemon_id][0]
            except (KeyError, IndexError) as e:
                logger.warning(f'Failed setting crush location for mon {dd.daemon_id}: {e}\n'
                               'Mon may not have a monmap entry yet. Try re-applying mon spec once mon is confirmed up.')
            desired_crush_locs = '{' + ','.join(mon_spec.crush_locations[dd.hostname]) + '}'
            logger.debug(f'Found spec defined crush locations for mon on {dd.hostname}: {desired_crush_locs}')
            logger.debug(f'Current crush locations for mon on {dd.hostname}: {current_crush_locs}')
            if current_crush_locs != desired_crush_locs:
                logger.info(f'Setting crush location for mon {dd.daemon_id} to {desired_crush_locs}')
                try:
                    ret, out, err = self.mgr.check_mon_command({
                        'prefix': 'mon set_location',
                        'name': dd.daemon_id,
                        'args': mon_spec.crush_locations[dd.hostname]
                    })
                except Exception as e:
                    logger.error(f'Failed setting crush location for mon {dd.daemon_id}: {e}')
