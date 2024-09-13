import logging

from teuthology import misc as teuthology
from teuthology.task import Task
from teuthology.packaging import remove_package

log = logging.getLogger(__name__)

display_name='Foo'
email='foo@foo.com'
access_key='test3'
secret_key='test3'

class D4NTestsRemote(Task):

    def __init__(self, ctx, config):
        super(D4NTestsRemote, self).__init__(ctx, config)
        self.log = log
        log.info('D4N Tests: __INIT__ ')
        
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
                log.debug('D4N Tests: Client is {cli}'.format(cli=client))

        self.user = {'s3main': 'tester'}

    def setup(self):
        super(D4NTestsRemote, self).setup()
        log.info('D4N Tests: SETUP')

    def begin(self):
        super(D4NTestsRemote, self).begin()
        log.info('D4N Tests: BEGIN')

        for (host, roles) in self.ctx.cluster.remotes.items():
            log.debug('D4N Tests: Cluster config is: {cfg}'.format(cfg=roles))
            log.debug('D4N Tests: Host is: {host}'.format(host=host))

            for role in roles:
                if 'client' in role:
                    self.all_clients.extend([role])
        if self.all_clients is None:
            self.all_clients = 'client.0'

        log.debug('D4N Tests: Client list:')
        log.debug(self.all_clients)

        self.create_user()

    def end(self):
        for client in self.all_clients:
            self.delete_user(client)
            self.remove_packages(client)

        super(D4NTestsRemote, self).end()
        log.info('D4N Tests: END')

    def create_user(self):
        log.info("D4N Tests: Creating S3 user...")
        testdir = teuthology.get_testdir(self.ctx)

        for client in self.all_clients:
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

        for client in self.all_clients:
            s3_user_id = 's3main'
            log.debug(
                'D4N Tests: Removing user {s3_user_id}'.format(s3_user_id=s3_user_id))
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
                    'user', 'rm',
                    '--uid', s3_user_id,
                    '--purge-data',
                    #'--cluster', cluster_name 
                ],
            )

task = D4NTestsRemote
