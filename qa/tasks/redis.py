import logging

from teuthology import misc as teuthology
from teuthology.task import Task

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

    def valkey_service(self, client):
        # the unit is named after the package: valkey-server on deb,
        # valkey on rpm
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        if remote.os.package_type == 'deb':
            return 'valkey-server'
        return 'valkey'

    def redis_startup(self):
        try:
            for client in self.all_clients:
                # restart rather than start: the deb package's postinst
                # already started the server, while the rpm package does
                # not, and either way this leaves us with a fresh one.
                # the units are Type=notify, so systemd waits for the
                # server to report ready and this fails if it does not
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'systemctl',
                        'restart',
                        self.valkey_service(client)
                        ],
                    )

        except Exception as err:
            log.debug('Redis Task: Error starting up a Redis server')
            log.debug(err)
            raise

    def redis_shutdown(self):
        try:
            for client in self.all_clients:
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'systemctl',
                        'stop',
                        self.valkey_service(client)
                        ],
                    )

        except Exception as err:
            log.debug('Redis Task: Error shutting down a Redis server')
            log.debug(err)

task = Redis
