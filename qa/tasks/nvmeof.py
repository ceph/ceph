import logging
import random
import time
import gevent
from textwrap import dedent
from gevent.event import Event
from gevent.greenlet import Greenlet
from teuthology.task import Task
from teuthology import misc
from teuthology.exceptions import ConfigError
from tasks.util import get_remote_for_role
from tasks.cephadm import _shell
from tasks.thrasher import Thrasher

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
                rbd_size: 1024
            gateway_config:
                namespaces_count: 10
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
        self.gateway_image = self.config.get('gw_image', 'default')

        rbd_config = self.config.get('rbd', {})
        self.poolname = rbd_config.get('pool_name', 'mypool')
        self.image_name_prefix = rbd_config.get('image_name_prefix', 'myimage')
        self.rbd_size = rbd_config.get('rbd_size', 1024*8)

        gateway_config = self.config.get('gateway_config', {})
        self.cli_image = gateway_config.get('cli_image', 'quay.io/ceph/nvmeof-cli:latest')
        self.nqn_prefix = gateway_config.get('subsystem_nqn_prefix', 'nqn.2016-06.io.spdk:cnode')
        self.subsystems_count = gateway_config.get('subsystems_count', 1) 
        self.namespaces_count = gateway_config.get('namespaces_count', 1) # namepsaces per subsystem
        self.bdev = gateway_config.get('bdev', 'mybdev')
        self.serial = gateway_config.get('serial', 'SPDK00000000000001')
        self.port = gateway_config.get('port', '4420')
        self.srport = gateway_config.get('srport', '5500')

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
            gw_image = self.gateway_image
            if (gw_image != "default"):
                log.info(f'[nvmeof]: ceph config set mgr mgr/cephadm/container_image_nvmeof {gw_image}')
                _shell(self.ctx, self.cluster_name, self.remote, [
                    'ceph', 'config', 'set', 'mgr', 
                    'mgr/cephadm/container_image_nvmeof',
                    gw_image
                ])

            poolname = self.poolname

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

            total_images = int(self.namespaces_count) * int(self.subsystems_count)
            log.info(f'[nvmeof]: creating {total_images} images')
            for i in range(1, total_images + 1):
                imagename = self.image_name_prefix + str(i)
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
        ip_address = self.remote.ip_address
        gateway_names = []
        gateway_ips = []
        nvmeof_daemons = self.ctx.daemons.iter_daemons_of_role('nvmeof', cluster=self.cluster_name)
        for daemon in nvmeof_daemons:
            gateway_names += [daemon.remote.shortname]
            gateway_ips += [daemon.remote.ip_address]
        conf_data = dedent(f"""
            NVMEOF_GATEWAY_IP_ADDRESSES={",".join(gateway_ips)}
            NVMEOF_GATEWAY_NAMES={",".join(gateway_names)}
            NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS={ip_address}
            NVMEOF_CLI_IMAGE="{self.cli_image}"
            NVMEOF_SUBSYSTEMS_PREFIX={self.nqn_prefix}
            NVMEOF_SUBSYSTEMS_COUNT={self.subsystems_count}
            NVMEOF_NAMESPACES_COUNT={self.namespaces_count}
            NVMEOF_PORT={self.port}
            NVMEOF_SRPORT={self.srport}
            """)
        for remote in self.ctx.cluster.remotes.keys():
            remote.write_file(
                path=conf_file,
                data=conf_data,
                sudo=True
            )
        log.info("[nvmeof]: executed set_gateway_cfg successfully!")


class NvmeofThrasher(Thrasher, Greenlet):
    def __init__(self, config, daemons) -> None:
        super(NvmeofThrasher, self).__init__()

        if config is None:
            self.config = dict()
        self.config = config
        self.daemons = daemons
        self.logger = log.getChild('[nvmeof.thrasher]')
        self.stopping = Event()

        """ Random seed """
        self.random_seed = self.config.get('seed', None)
        if self.random_seed is None:
            self.random_seed = int(time.time())

        self.rng = random.Random()
        self.rng.seed(int(self.random_seed))

        """ Thrashing params """
        self.randomize = bool(self.config.get('randomize', True))
        self.max_thrash_iters_each = int(self.config.get('max_thrash_iters_each', 3)) # thrash each daemon only 3 times be default
        self.max_thrash = int(self.config.get('max_thrash', len(self.daemons) - 1))
        self.min_thrash_delay = int(self.config.get('min_thrash_delay', 60))
        self.max_thrash_delay = int(self.config.get('max_thrash_delay', self.min_thrash_delay + 30))
        self.min_revive_delay = int(self.config.get('min_revive_delay', 60))
        self.max_revive_delay = int(self.config.get('max_revive_delay', self.min_revive_delay + 30))

    def log(self, x):
        self.logger.info(x)

    def _run(self): # overriding 
        try:
            self.do_thrash()
        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...
    
    def stop(self):
        self.stopping.set()

    def do_thrash(self):
        self.log('start thrashing')
        self.log(f'seed: {self.random_seed}, , '\
                 f'max thrash delay: {self.max_thrash_delay}, min thrash delay: {self.min_thrash_delay} '\
                 f'max revive delay: {self.max_revive_delay}, min revive delay: {self.min_revive_delay} '\
                 f'daemons: {len(self.daemons)} '\
                )
        thrash_count = {}

        while not self.stopping.is_set():
            # delay before thrashing
            thrash_delay = self.min_thrash_delay
            if self.randomize:
                thrash_delay = random.randrange(self.min_thrash_delay, self.max_thrash_delay)

            if thrash_delay > 0.0:
                self.log(f'waiting for {thrash_delay} secs before thrashing')
                self.stopping.wait(thrash_delay)
                if self.stopping.is_set():
                    continue

            killed_daemons = []

            weight = 1.0 / len(self.daemons)
            count = 0
            for daemon in self.daemons:
                skip = self.rng.uniform(0.0, 1.0)
                if weight <= skip:
                    self.log('skipping daemon {label} with skip ({skip}) > weight ({weight})'.format(
                        label=daemon.id_, skip=skip, weight=weight))
                    continue
                if thrash_count.get(daemon.id_, 0) >= self.max_thrash_iters_each:
                    self.log(f'skipping daemon {daemon.id_}: already thrashed {self.max_thrash_iters_each} times')
                    continue

                self.log('kill {label}'.format(label=daemon.id_))
                daemon.stop()
                
                killed_daemons.append(daemon)
                thrash_count[daemon.id_] = thrash_count.get(daemon.id_, 0) + 1

                # if we've reached max_thrash, we're done
                count += 1
                if count >= self.max_thrash:
                    break

            if killed_daemons:  
                # delay before reviving
                revive_delay = self.min_revive_delay
                if self.randomize:
                    revive_delay = random.randrange(self.min_revive_delay, self.max_revive_delay)

                self.log(f'waiting for {revive_delay} secs before reviving')
                gevent.sleep(revive_delay)

                # revive after thrashing
                for daemon in killed_daemons:
                    self.log('reviving {label}'.format(label=daemon.id_))
                    daemon.start()
        self.log(thrash_count)

class ThrashTest(Nvmeof):
    name = 'nvmeof.thrash'
    def setup(self):
        if self.config is None:
            self.config = {}
        assert isinstance(self.config, dict), \
            'nvmeof.thrash task only accepts a dict for configuration'

        self.cluster = self.config.get('cluster', 'ceph')
        daemons = list(self.ctx.daemons.iter_daemons_of_role('nvmeof', self.cluster))
        assert len(daemons) > 1, \
            'nvmeof.thrash task requires at least 2 nvmeof daemon'
        self.thrasher = NvmeofThrasher(self.config, daemons)

    def begin(self):
        self.thrasher.start()
        self.ctx.ceph[self.cluster].thrashers.append(self.thrasher) 

    def end(self):
        log.info('joining nvmeof.thrash')
        self.thrasher.stop()
        if self.thrasher.exception is not None:
            raise RuntimeError('error during thrashing')
        self.thrasher.join()
        log.info('done joining')


task = Nvmeof
thrash = ThrashTest
