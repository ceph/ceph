import logging

from teuthology import misc as teuthology
from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.packaging import remove_package

log = logging.getLogger(__name__)

display_name='Foo'
email='foo@foo.com'
access_key='test3'
secret_key='test3'

class D4NTests(Task):

    def __init__(self, ctx, config):
        super(D4NTests, self).__init__(ctx, config)
        self.log = log
        log.info('D4N Tests: __INIT__ ')
        
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            self.all_clients = 'client.0'
        
        self.user = {'s3main': 'tester'}

    def setup(self):
        super(D4NTests, self).setup()
        log.info('D4N Tests: SETUP')

    def begin(self):
        super(D4NTests, self).begin()
        log.info('D4N Tests: BEGIN')

        for (host, roles) in self.ctx.cluster.remotes.items():
            log.debug('D4N Tests: Cluster config is: {cfg}'.format(cfg=roles))
            log.debug('D4N Tests: Host is: {host}'.format(host=host))

        self.create_user()

    def end(self):
        super(D4NTests, self).end()
        log.info('D4N Tests: END')

        for client in self.all_clients:
            self.remove_packages(client)
            self.delete_user(client)

    def create_user(self):
        log.info("D4N Tests: Creating S3 user...")
        testdir = teuthology.get_testdir(self.ctx)

        for client in self.all_clients:
            for user in list(self.user.items()):
                s3_user_id = 's3main'
                log.debug(
                    'D4N Tests: Creating user {s3_user_id}'.format(s3_user_id=s3_user_id))
                cluster_name, daemon_type, client_id = teuthology.split_role(
                    client)
                client_with_id = daemon_type + '.' + client_id
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'create',
                        '--uid', s3_user_id,
                        '--display-name', display_name,
                        '--access-key', access_key,
                        '--secret', secret_key,
                        '--email', email,
                        '--cluster', cluster_name,
                        ],
                    )

    def remove_packages(self, client):
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        remove_package('s3cmd', remote)

    def delete_user(self, client):
        log.info("D4N Tests: Deleting S3 user...")
        testdir = teuthology.get_testdir(self.ctx)

        for user in self.user.items():
            s3_user_id = 's3main'
            self.ctx.cluster.only(client).run(
                args=[
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    'user', 'rm',
                    '--uid', s3_user_id,
                    '--purge-data',
                    '--cluster', 'ceph',
                ],
            )

task = D4NTests
