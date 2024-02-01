import logging
from textwrap import dedent
from teuthology.task import Task
from teuthology import misc
from teuthology.exceptions import ConfigError
from tasks.util import get_remote_for_role
from tasks.cephadm import _shell

log = logging.getLogger(__name__)

conf_file = '/etc/ceph/nvmeof.env'


class Nvmeof(Task):
    """
    Setup nvmeof gateway on client and then share gateway config to target host.

        - nvmeof:
            client: client.0
            version: default
            rbd:
                pool_name: mypool
                image_name: myimage
                rbd_size: 1024
            gateway_config:
                source: host.a 
                target: client.2
                vars:
                    cli_version: latest
                    
    """

    def setup(self):
        super(Nvmeof, self).setup()
        try:
            self.client = self.config['client']
        except KeyError:
            raise ConfigError('nvmeof requires a client to connect with')

        self.cluster_name, type_, self.client_id = misc.split_role(self.client)
        if type_ != 'client':
            msg = 'client role ({0}) must be a client'.format(self.client)
            raise ConfigError(msg)
        self.remote = get_remote_for_role(self.ctx, self.client)

    def begin(self):
        super(Nvmeof, self).begin()
        self._set_defaults()
        self.deploy_nvmeof()
        self.set_gateway_cfg()

    def _set_defaults(self):
        self.gateway_image = self.config.get('version', 'default')

        rbd_config = self.config.get('rbd', {})
        self.poolname = rbd_config.get('pool_name', 'mypool')
        self.rbd_image_name = rbd_config.get('image_name', 'myimage')
        self.rbd_size = rbd_config.get('rbd_size', 1024*8)

        gateway_config = self.config.get('gateway_config', {})
        conf_vars = gateway_config.get('vars', {})
        self.cli_image = conf_vars.get('cli_version', 'latest')
        self.bdev = conf_vars.get('bdev', 'mybdev')
        self.serial = conf_vars.get('serial', 'SPDK00000000000001')
        self.nqn = conf_vars.get('nqn', 'nqn.2016-06.io.spdk:cnode1')
        self.port = conf_vars.get('port', '4420')
        self.srport = conf_vars.get('srport', '5500')

    def deploy_nvmeof(self):
        """
        Deploy nvmeof gateway.
        """
        log.info('[nvmeof]: deploying nvmeof gateway...')
        if not hasattr(self.ctx, 'ceph'):
            self.ctx.ceph = {}
        fsid = self.ctx.ceph[self.cluster_name].fsid

        nodes = []
        daemons = {}

        for remote, roles in self.ctx.cluster.remotes.items():
            for role in [r for r in roles
                         if misc.is_type('nvmeof', self.cluster_name)(r)]:
                c_, _, id_ = misc.split_role(role)
                log.info('Adding %s on %s' % (role, remote.shortname))
                nodes.append(remote.shortname + '=' + id_)
                daemons[role] = (remote, id_)

        if nodes:
            image = self.gateway_image
            if (image != "default"):
                log.info(f'[nvmeof]: ceph config set mgr mgr/cephadm/container_image_nvmeof quay.io/ceph/nvmeof:{image}')
                _shell(self.ctx, self.cluster_name, self.remote, [
                    'ceph', 'config', 'set', 'mgr', 
                    'mgr/cephadm/container_image_nvmeof',
                    f'quay.io/ceph/nvmeof:{image}'
                ])

            poolname = self.poolname
            imagename = self.rbd_image_name

            log.info(f'[nvmeof]: ceph osd pool create {poolname}')
            _shell(self.ctx, self.cluster_name, self.remote, [
                'ceph', 'osd', 'pool', 'create', poolname
            ])

            log.info(f'[nvmeof]: rbd pool init {poolname}')
            _shell(self.ctx, self.cluster_name, self.remote, [
                'rbd', 'pool', 'init', poolname
            ])

            log.info(f'[nvmeof]: ceph orch apply nvmeof {poolname}')
            _shell(self.ctx, self.cluster_name, self.remote, [
                'ceph', 'orch', 'apply', 'nvmeof', poolname, 
                '--placement', str(len(nodes)) + ';' + ';'.join(nodes)
            ])

            log.info(f'[nvmeof]: rbd create {poolname}/{imagename} --size {self.rbd_size}')
            _shell(self.ctx, self.cluster_name, self.remote, [
                'rbd', 'create', f'{poolname}/{imagename}', '--size', f'{self.rbd_size}'
            ])

        for role, i in daemons.items():
            remote, id_ = i
            self.ctx.daemons.register_daemon(
                remote, 'nvmeof', id_,
                cluster=self.cluster_name,
                fsid=fsid,
                logger=log.getChild(role),
                wait=False,
                started=True,
            )
        log.info("[nvmeof]: executed deploy_nvmeof successfully!")
        
    def set_gateway_cfg(self):
        log.info('[nvmeof]: running set_gateway_cfg...')
        gateway_config = self.config.get('gateway_config', {})
        source_host = gateway_config.get('source')
        target_host = gateway_config.get('target')
        if not (source_host and target_host):
            raise ConfigError('gateway_config requires "source" and "target"')
        remote = list(self.ctx.cluster.only(source_host).remotes.keys())[0]
        ip_address = remote.ip_address
        gateway_name = ""
        nvmeof_daemons = self.ctx.daemons.iter_daemons_of_role('nvmeof', cluster=self.cluster_name)
        for daemon in nvmeof_daemons:
            if ip_address == daemon.remote.ip_address:
                gateway_name = daemon.name()
        conf_data = dedent(f"""
            NVMEOF_GATEWAY_IP_ADDRESS={ip_address}
            NVMEOF_GATEWAY_NAME={gateway_name}
            NVMEOF_CLI_IMAGE="quay.io/ceph/nvmeof-cli:{self.cli_image}"
            NVMEOF_BDEV={self.bdev}
            NVMEOF_SERIAL={self.serial}
            NVMEOF_NQN={self.nqn}
            NVMEOF_PORT={self.port}
            NVMEOF_SRPORT={self.srport}
            """)
        target_remote = list(self.ctx.cluster.only(target_host).remotes.keys())[0]
        target_remote.write_file(
            path=conf_file,
            data=conf_data,
            sudo=True
        )
        log.info("[nvmeof]: executed set_gateway_cfg successfully!")


task = Nvmeof
