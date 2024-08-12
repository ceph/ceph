import errno
import ipaddress
import logging
import os
import subprocess
import tempfile
from typing import Dict, Tuple, Any, List, cast, Optional
from configparser import ConfigParser
from io import StringIO

from mgr_module import HandleCommandResult
from mgr_module import NFS_POOL_NAME as POOL_NAME

from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec

from orchestrator import DaemonDescription

from cephadm.services.cephadmservice import AuthEntity, CephadmDaemonDeploySpec, CephService

logger = logging.getLogger(__name__)


class NFSService(CephService):
    TYPE = 'nfs'

    def ranked(self, spec: ServiceSpec) -> bool:
        return True

    def fence(self, daemon_id: str) -> None:
        logger.info(f'Fencing old nfs.{daemon_id}')
        ret, out, err = self.mgr.mon_command({
            'prefix': 'auth rm',
            'entity': f'client.nfs.{daemon_id}',
        })

        # TODO: block/fence this entity (in case it is still running somewhere)

    def fence_old_ranks(self,
                        spec: ServiceSpec,
                        rank_map: Dict[int, Dict[int, Optional[str]]],
                        num_ranks: int) -> None:
        for rank, m in list(rank_map.items()):
            if rank >= num_ranks:
                for daemon_id in m.values():
                    if daemon_id is not None:
                        self.fence(daemon_id)
                del rank_map[rank]
                nodeid = f'{spec.service_name()}.{rank}'
                self.mgr.log.info(f'Removing {nodeid} from the ganesha grace table')
                self.run_grace_tool(cast(NFSServiceSpec, spec), 'remove', nodeid)
                self.mgr.spec_store.save_rank_map(spec.service_name(), rank_map)
            else:
                max_gen = max(m.keys())
                for gen, daemon_id in list(m.items()):
                    if gen < max_gen:
                        if daemon_id is not None:
                            self.fence(daemon_id)
                        del rank_map[rank][gen]
                        self.mgr.spec_store.save_rank_map(spec.service_name(), rank_map)

    def config(self, spec: NFSServiceSpec) -> None:  # type: ignore
        from nfs.cluster import create_ganesha_pool

        assert self.TYPE == spec.service_type
        create_ganesha_pool(self.mgr)

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type

        daemon_type = daemon_spec.daemon_type
        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(NFSServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        deps: List[str] = []

        nodeid = f'{daemon_spec.service_name}.{daemon_spec.rank}'

        nfs_idmap_conf = '/etc/ganesha/idmap.conf'

        # create the RADOS recovery pool keyring
        rados_user = f'{daemon_type}.{daemon_id}'
        rados_keyring = self.create_keyring(daemon_spec)

        # ensure rank is known to ganesha
        self.mgr.log.info(f'Ensuring {nodeid} is in the ganesha grace table')
        self.run_grace_tool(spec, 'add', nodeid)

        # create the rados config object
        self.create_rados_config_obj(spec)

        # create the RGW keyring
        rgw_user = f'{rados_user}-rgw'
        rgw_keyring = self.create_rgw_keyring(daemon_spec)
        if spec.virtual_ip:
            bind_addr = spec.virtual_ip
        else:
            bind_addr = daemon_spec.ip if daemon_spec.ip else ''
        if not bind_addr:
            logger.warning(f'Bind address in {daemon_type}.{daemon_id}\'s ganesha conf is defaulting to empty')
        else:
            logger.debug("using haproxy bind address: %r", bind_addr)

        # generate the ganesha config
        def get_ganesha_conf() -> str:
            context: Dict[str, Any] = {
                "user": rados_user,
                "nodeid": nodeid,
                "pool": POOL_NAME,
                "namespace": spec.service_id,
                "rgw_user": rgw_user,
                "url": f'rados://{POOL_NAME}/{spec.service_id}/{spec.rados_config_name()}',
                # fall back to default NFS port if not present in daemon_spec
                "port": daemon_spec.ports[0] if daemon_spec.ports else 2049,
                "bind_addr": bind_addr,
                "haproxy_hosts": [],
                "nfs_idmap_conf": nfs_idmap_conf,
                "enable_nlm": str(spec.enable_nlm).lower(),
            }
            if spec.enable_haproxy_protocol:
                context["haproxy_hosts"] = self._haproxy_hosts()
                logger.debug("selected haproxy_hosts: %r", context["haproxy_hosts"])
            return self.mgr.template.render('services/nfs/ganesha.conf.j2', context)

        # generate the idmap config
        def get_idmap_conf() -> str:
            idmap_conf = spec.idmap_conf
            output = ''
            if idmap_conf is not None:
                cp = ConfigParser()
                out = StringIO()
                cp.read_dict(idmap_conf)
                cp.write(out)
                out.seek(0)
                output = out.read()
                out.close()
            return output

        # generate the cephadm config json
        def get_cephadm_config() -> Dict[str, Any]:
            config: Dict[str, Any] = {}
            config['pool'] = POOL_NAME
            config['namespace'] = spec.service_id
            config['userid'] = rados_user
            config['extra_args'] = ['-N', 'NIV_EVENT']
            config['files'] = {
                'ganesha.conf': get_ganesha_conf(),
                'idmap.conf': get_idmap_conf()
            }
            config.update(
                self.get_config_and_keyring(
                    daemon_type, daemon_id,
                    keyring=rados_keyring,
                    host=host
                )
            )
            config['rgw'] = {
                'cluster': 'ceph',
                'user': rgw_user,
                'keyring': rgw_keyring,
            }
            logger.debug('Generated cephadm config-json: %s' % config)
            return config

        return get_cephadm_config(), deps

    def create_rados_config_obj(self,
                                spec: NFSServiceSpec,
                                clobber: bool = False) -> None:
        objname = spec.rados_config_name()
        cmd = [
            'rados',
            '-n', f"mgr.{self.mgr.get_mgr_id()}",
            '-k', str(self.mgr.get_ceph_option('keyring')),
            '-p', POOL_NAME,
            '--namespace', cast(str, spec.service_id),
        ]
        result = subprocess.run(
            cmd + ['get', objname, '-'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=10)
        if not result.returncode and not clobber:
            logger.info('Rados config object exists: %s' % objname)
        else:
            logger.info('Creating rados config object: %s' % objname)
            result = subprocess.run(
                cmd + ['put', objname, '-'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=10)
            if result.returncode:
                self.mgr.log.warning(
                    f'Unable to create rados config object {objname}: {result.stderr.decode("utf-8")}'
                )
                raise RuntimeError(result.stderr.decode("utf-8"))

    def create_keyring(self, daemon_spec: CephadmDaemonDeploySpec) -> str:
        daemon_id = daemon_spec.daemon_id
        spec = cast(NFSServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        entity: AuthEntity = self.get_auth_entity(daemon_id)

        osd_caps = 'allow rw pool=%s namespace=%s' % (POOL_NAME, spec.service_id)

        logger.info('Creating key for %s' % entity)
        keyring = self.get_keyring_with_caps(entity,
                                             ['mon', 'allow r',
                                              'osd', osd_caps])

        return keyring

    def create_rgw_keyring(self, daemon_spec: CephadmDaemonDeploySpec) -> str:
        daemon_id = daemon_spec.daemon_id
        entity: AuthEntity = self.get_auth_entity(f'{daemon_id}-rgw')

        logger.info('Creating key for %s' % entity)
        keyring = self.get_keyring_with_caps(entity,
                                             ['mon', 'allow r',
                                              'osd', 'allow rwx tag rgw *=*'])

        return keyring

    def run_grace_tool(self,
                       spec: NFSServiceSpec,
                       action: str,
                       nodeid: str) -> None:
        # write a temp keyring and referencing config file.  this is a kludge
        # because the ganesha-grace-tool can only authenticate as a client (and
        # not a mgr).  Also, it doesn't allow you to pass a keyring location via
        # the command line, nor does it parse the CEPH_ARGS env var.
        tmp_id = f'mgr.nfs.grace.{spec.service_name()}'
        entity = AuthEntity(f'client.{tmp_id}')
        keyring = self.get_keyring_with_caps(
            entity,
            ['mon', 'allow r', 'osd', f'allow rwx pool {POOL_NAME}']
        )
        tmp_keyring = tempfile.NamedTemporaryFile(mode='w', prefix='mgr-grace-keyring')
        os.fchmod(tmp_keyring.fileno(), 0o600)
        tmp_keyring.write(keyring)
        tmp_keyring.flush()
        tmp_conf = tempfile.NamedTemporaryFile(mode='w', prefix='mgr-grace-conf')
        tmp_conf.write(self.mgr.get_minimal_ceph_conf())
        tmp_conf.write(f'\tkeyring = {tmp_keyring.name}\n')
        tmp_conf.flush()
        try:
            cmd: List[str] = [
                'ganesha-rados-grace',
                '--cephconf', tmp_conf.name,
                '--userid', tmp_id,
                '--pool', POOL_NAME,
                '--ns', cast(str, spec.service_id),
                action, nodeid,
            ]
            self.mgr.log.debug(cmd)
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=10)
            if result.returncode:
                self.mgr.log.warning(
                    f'ganesha-rados-grace tool failed: {result.stderr.decode("utf-8")}'
                )
                raise RuntimeError(f'grace tool failed: {result.stderr.decode("utf-8")}')

        finally:
            self.mgr.check_mon_command({
                'prefix': 'auth rm',
                'entity': entity,
            })

    def remove_rgw_keyring(self, daemon: DaemonDescription) -> None:
        assert daemon.daemon_id is not None
        daemon_id: str = daemon.daemon_id
        entity: AuthEntity = self.get_auth_entity(f'{daemon_id}-rgw')

        logger.info(f'Removing key for {entity}')
        self.mgr.check_mon_command({
            'prefix': 'auth rm',
            'entity': entity,
        })

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        super().post_remove(daemon, is_failed_deploy=is_failed_deploy)
        self.remove_rgw_keyring(daemon)

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # if only 1 nfs, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'NFS', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 nfs daemon.
        if force:
            return HandleCommandResult(0, warn_message, '')

        # if reached here, > 1 nfs daemon and no force flag.
        # Provide warning
        warn_message = "WARNING: Removing NFS daemons can cause clients to lose connectivity. "
        return HandleCommandResult(-errno.EBUSY, '', warn_message)

    def purge(self, service_name: str) -> None:
        if service_name not in self.mgr.spec_store:
            return
        spec = cast(NFSServiceSpec, self.mgr.spec_store[service_name].spec)

        logger.info(f'Removing grace file for {service_name}')
        cmd = [
            'rados',
            '-n', f"mgr.{self.mgr.get_mgr_id()}",
            '-k', str(self.mgr.get_ceph_option('keyring')),
            '-p', POOL_NAME,
            '--namespace', cast(str, spec.service_id),
            'rm', 'grace',
        ]
        subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10
        )

    def _haproxy_hosts(self) -> List[str]:
        # NB: Ideally, we would limit the list to IPs on hosts running
        # haproxy/ingress only, but due to the nature of cephadm today
        # we'd "only know the set of haproxy hosts after they've been
        # deployed" (quoth @adk7398). As it is today we limit the list
        # of hosts we know are managed by cephadm. That ought to be
        # good enough to prevent acceping haproxy protocol messages
        # from "rouge" systems that are not under our control. At
        # least until we learn otherwise.
        cluster_ips: List[str] = []
        for host in self.mgr.inventory.keys():
            default_addr = self.mgr.inventory.get_addr(host)
            cluster_ips.append(default_addr)
            nets = self.mgr.cache.networks.get(host)
            if not nets:
                continue
            for subnet, iface in nets.items():
                ip_subnet = ipaddress.ip_network(subnet)
                if ipaddress.ip_address(default_addr) in ip_subnet:
                    continue  # already present
                if ip_subnet.is_loopback or ip_subnet.is_link_local:
                    continue  # ignore special subnets
                addrs: List[str] = sum((addr_list for addr_list in iface.values()), [])
                if addrs:
                    # one address per interface/subnet is enough
                    cluster_ips.append(addrs[0])
        return cluster_ips
