import logging

from teuthology import misc as teuthology
from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.packaging import remove_package

log = logging.getLogger(__name__)

class Redis(Task):

    def __init__(self, ctx, config):
        super(Redis, self).__init__(ctx, config)
        self.log = log
        log.info('Redis Task: __INIT__ ')
        
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            self.all_clients = 'client.0'

    def setup(self):
        super(Redis, self).setup()
        log.info('Redis Task: SETUP')

    def begin(self):
        super(Redis, self).begin()
        log.info('Redis Task: BEGIN')

        for (host, roles) in self.ctx.cluster.remotes.items():
            log.debug('Redis Task: Cluster config is: {cfg}'.format(cfg=roles))
            log.debug('Redis Task: Host is: {host}'.format(host=host))

        self.redis_startup()

    def end(self):
        super(Redis, self).end()
        log.info('Redis Task: END')

        self.redis_shutdown()

        for client in self.all_clients:
            self.remove_redis_package(client)

    def redis_startup(self):
        try:
            for client in self.all_clients:
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'redis-server',
                        '--daemonize',
                        'yes'
                        ],
                    )
    
        except Exception as err:
            log.debug('Redis Task: Error starting up a Redis server')
            log.debug(err)

    def redis_shutdown(self):
        try:
            for client in self.all_clients:
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'redis-cli',
                        'shutdown',
                        ],
                    )
    
        except Exception as err:
            log.debug('Redis Task: Error shutting down a Redis server')
            log.debug(err)

    def remove_redis_package(self, client):
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        remove_package('redis', remote)

task = Redis
