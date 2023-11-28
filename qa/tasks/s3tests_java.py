"""
Task for running RGW S3 tests with the AWS Java SDK
"""
from io import BytesIO
import logging

import base64
import os
import random
import string
import yaml
import getpass

from teuthology import misc as teuthology
from teuthology.task import Task
from teuthology.orchestra import run

log = logging.getLogger(__name__)

"""
    Task for running RGW S3 tests with the AWS Java SDK
    
    Tests run only on clients specified in the s3tests-java config section. 
    If no client is given a default 'client.0' is chosen.
    If such does not match the rgw client the task will fail.
        
        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests-java:
            client.0:

    Extra arguments can be passed by adding options to the corresponding client
    section under the s3tests-java task (e.g. to run a certain test, 
    specify a different repository and branch for the test suite, 
    run in info/debug mode (for the java suite) or forward the gradle output to a log file):

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests-java:
            client.0:
                force-branch: wip
                force-repo: 'https://github.com/adamyanova/java_s3tests.git'
                log-fwd: '../s3tests-java.log'
                log-level: info
                extra-args: ['--tests', 'ObjectTest.testEncryptionKeySSECInvalidMd5']

    To run a specific test, provide its name to the extra-args section e.g.:
        - s3tests-java:
            client.0:
                extra-args: ['--tests', 'ObjectTest.testEncryptionKeySSECInvalidMd5']
    
"""


class S3tests_java(Task):
    """
    Download and install S3 tests in Java
    This will require openjdk and gradle
    """

    def __init__(self, ctx, config):
        super(S3tests_java, self).__init__(ctx, config)
        self.log = log
        log.debug('S3 Tests Java: __INIT__ ')
        assert hasattr(ctx, 'rgw'), 'S3tests_java must run after the rgw task'
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            self.all_clients = 'client.0'
        self.users = {'s3main': 'tester',
                      's3alt': 'johndoe', 'tenanted': 'testx$tenanteduser'}

    def setup(self):
        super(S3tests_java, self).setup()
        log.debug('S3 Tests Java: SETUP')
        for client in self.all_clients:
            self.download_test_suite(client)
            self.install_required_packages(client)

    def begin(self):
        super(S3tests_java, self).begin()
        log.debug('S3 Tests Java: BEGIN')
        for (host, roles) in self.ctx.cluster.remotes.items():
            log.debug(
                'S3 Tests Java: Cluster config is: {cfg}'.format(cfg=roles))
            log.debug('S3 Tests Java: Host is: {host}'.format(host=host))
        self.create_users()
        self.run_tests()

    def end(self):
        super(S3tests_java, self).end()
        log.debug('S3 Tests Java: END')
        for client in self.all_clients:
            self.remove_tests(client)
            self.delete_users(client)

    def download_test_suite(self, client):
        log.info("S3 Tests Java: Downloading test suite...")
        testdir = teuthology.get_testdir(self.ctx)
        branch = 'master'
        repo = 'https://github.com/ceph/java_s3tests.git'
        if client in self.config and self.config[client] is not None:
            if 'force-branch' in self.config[client] and self.config[client]['force-branch'] is not None:
                branch = self.config[client]['force-branch']
            if 'force-repo' in self.config[client] and self.config[client]['force-repo'] is not None:
                repo = self.config[client]['force-repo']
        self.ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
                repo,
                '{tdir}/s3-tests-java'.format(tdir=testdir),
            ],
            stdout=BytesIO()
        )
        if client in self.config and self.config[client] is not None:
            if 'sha1' in self.config[client] and self.config[client]['sha1'] is not None:
                self.ctx.cluster.only(client).run(
                    args=[
                        'cd', '{tdir}/s3-tests-java'.format(tdir=testdir),
                        run.Raw('&&'),
                        'git', 'reset', '--hard', self.config[client]['sha1'],
                    ],
                )

            if 'log-level' in self.config[client]:
                if self.config[client]['log-level'] == 'info':
                    self.ctx.cluster.only(client).run(
                        args=[
                            'sed', '-i', '\'s/log4j.rootLogger=WARN/log4j.rootLogger=INFO/g\'',
                            '{tdir}/s3-tests-java/src/main/resources/log4j.properties'.format(
                                tdir=testdir)
                        ]
                    )
                if self.config[client]['log-level'] == 'debug':
                    self.ctx.cluster.only(client).run(
                        args=[
                            'sed', '-i', '\'s/log4j.rootLogger=WARN/log4j.rootLogger=DEBUG/g\'',
                            '{tdir}/s3-tests-java/src/main/resources/log4j.properties'.format(
                                tdir=testdir)
                        ]
                    )

    def install_required_packages(self, client):
        """
        Run bootstrap script to install openjdk and gradle.
        Add certificates to java keystore
        """
        log.info("S3 Tests Java: Installing required packages...")
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=['{tdir}/s3-tests-java/bootstrap.sh'.format(tdir=testdir)],
            stdout=BytesIO()
        )

        endpoint = self.ctx.rgw.role_endpoints[client]
        if endpoint.cert:
            path = 'lib/security/cacerts'
            self.ctx.cluster.only(client).run(
                args=['sudo',
                      'keytool',
                      '-import', '-alias', '{alias}'.format(
                          alias=endpoint.hostname),
                      '-keystore',
                      run.Raw(
                          '$(readlink -e $(dirname $(readlink -e $(which keytool)))/../{path})'.format(path=path)),
                      '-file', endpoint.cert.certificate,
                      '-storepass', 'changeit',
                      ],
                stdout=BytesIO()
            )

    def create_users(self):
        """
        Create a main and an alternative s3 user.
        Configuration is read from a skelethon config file
        s3tests.teuth.config.yaml in the java-s3tests repository
        and missing information is added from the task.
        Existing values are NOT overridden unless they are empty!
        """
        log.info("S3 Tests Java: Creating S3 users...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            endpoint = self.ctx.rgw.role_endpoints.get(client)
            local_user = getpass.getuser()
            remote_user = teuthology.get_test_user()
            os.system("scp {remote}@{host}:{tdir}/s3-tests-java/s3tests.teuth.config.yaml /home/{local}/".format(
                host=endpoint.hostname, tdir=testdir, remote=remote_user, local=local_user))
            s3tests_conf = teuthology.config_file(
                '/home/{local}/s3tests.teuth.config.yaml'.format(local=local_user))
            log.debug("S3 Tests Java: s3tests_conf is {s3cfg}".format(
                s3cfg=s3tests_conf))
            for section, user in list(self.users.items()):
                if section in s3tests_conf:
                    s3_user_id = '{user}.{client}'.format(
                        user=user, client=client)
                    log.debug(
                        'S3 Tests Java: Creating user {s3_user_id}'.format(s3_user_id=s3_user_id))
                    self._config_user(s3tests_conf=s3tests_conf,
                                      section=section, user=s3_user_id, client=client)
                    cluster_name, daemon_type, client_id = teuthology.split_role(
                        client)
                    client_with_id = daemon_type + '.' + client_id
                    args = [
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'create',
                        '--uid', s3tests_conf[section]['user_id'],
                        '--display-name', s3tests_conf[section]['display_name'],
                        '--access-key', s3tests_conf[section]['access_key'],
                        '--secret', s3tests_conf[section]['access_secret'],
                        '--email', s3tests_conf[section]['email'],
                        '--cluster', cluster_name,
                    ]
                    log.info('{args}'.format(args=args))
                    self.ctx.cluster.only(client).run(
                        args=args,
                        stdout=BytesIO()
                    )
                else:
                    self.users.pop(section)
            self._write_cfg_file(s3tests_conf, client)
            os.system(
                "rm -rf /home/{local}/s3tests.teuth.config.yaml".format(local=local_user))

    def _config_user(self, s3tests_conf, section, user, client):
        """
        Generate missing users data for this section by stashing away keys, ids, and
        email addresses.
        """
        access_key = ''.join(random.choice(string.ascii_uppercase)
                             for i in range(20))
        access_secret = base64.b64encode(os.urandom(40)).decode('ascii')
        endpoint = self.ctx.rgw.role_endpoints.get(client)

        self._set_cfg_entry(
            s3tests_conf[section], 'user_id', '{user}'.format(user=user))
        self._set_cfg_entry(
            s3tests_conf[section], 'email', '{user}_test@test.test'.format(user=user))
        self._set_cfg_entry(
            s3tests_conf[section], 'display_name', 'Ms. {user}'.format(user=user))
        self._set_cfg_entry(
            s3tests_conf[section], 'access_key', '{ak}'.format(ak=access_key))
        self._set_cfg_entry(
            s3tests_conf[section], 'access_secret', '{asc}'.format(asc=access_secret))
        self._set_cfg_entry(
            s3tests_conf[section], 'region', 'us-east-1')
        self._set_cfg_entry(
            s3tests_conf[section], 'endpoint', '{ip}:{port}'.format(
                ip=endpoint.hostname, port=endpoint.port))
        self._set_cfg_entry(
            s3tests_conf[section], 'host', endpoint.hostname)
        self._set_cfg_entry(
            s3tests_conf[section], 'port', endpoint.port)
        self._set_cfg_entry(
            s3tests_conf[section], 'is_secure', True if endpoint.cert else False)

        log.debug("S3 Tests Java: s3tests_conf[{sect}] is {s3cfg}".format(
            sect=section, s3cfg=s3tests_conf[section]))
        log.debug('S3 Tests Java: Setion, User = {sect}, {user}'.format(
            sect=section, user=user))

    def _write_cfg_file(self, cfg_dict, client):
        """
        Write s3 tests java config file on the remote node.
        """
        testdir = teuthology.get_testdir(self.ctx)
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        data = yaml.safe_dump(cfg_dict, default_flow_style=False)
        path = testdir + '/archive/s3-tests-java.' + client + '.conf'
        remote.write_file(path, data)

    def _set_cfg_entry(self, cfg_dict, key, value):
        if not (key in cfg_dict):
            cfg_dict.setdefault(key, value)
        elif cfg_dict[key] is None:
            cfg_dict[key] = value

    def run_tests(self):
        log.info("S3 Tests Java: Running tests...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            self.ctx.cluster.only(client).run(
                args=['cp',
                      '{tdir}/archive/s3-tests-java.{client}.conf'.format(
                          tdir=testdir, client=client),
                      '{tdir}/s3-tests-java/config.properties'.format(
                          tdir=testdir)
                      ],
                stdout=BytesIO()
            )
            args = ['cd',
                    '{tdir}/s3-tests-java'.format(tdir=testdir),
                    run.Raw('&&'),
                    '/opt/gradle/gradle/bin/gradle', 'clean', 'test',
                    '--rerun-tasks', '--no-build-cache',
                    ]
            extra_args = []
            suppress_groups = False
            self.log_fwd = False
            self.log_name = ''
            if client in self.config and self.config[client] is not None:
                if 'extra-args' in self.config[client]:
                    extra_args.extend(self.config[client]['extra-args'])
                    suppress_groups = True
                if 'log-level' in self.config[client] and self.config[client]['log-level'] == 'debug':
                    extra_args += ['--debug']
                if 'log-fwd' in self.config[client]:
                    self.log_fwd = True
                    self.log_name = '{tdir}/s3tests_log.txt'.format(
                        tdir=testdir)
                    if self.config[client]['log-fwd'] is not None:
                        self.log_name = self.config[client]['log-fwd']
                    extra_args += [run.Raw('>>'),
                                   self.log_name]

            if not suppress_groups:
                test_groups = ['AWS4Test', 'BucketTest', 'ObjectTest']
            else:
                test_groups = ['All']

            for gr in test_groups:
                for i in range(2):
                    self.ctx.cluster.only(client).run(
                        args=['radosgw-admin', 'gc',
                              'process', '--include-all'],
                        stdout=BytesIO()
                    )

                if gr != 'All':
                    self.ctx.cluster.only(client).run(
                        args=args + ['--tests'] + [gr] + extra_args,
                        stdout=BytesIO()
                    )
                else:
                    self.ctx.cluster.only(client).run(
                        args=args + extra_args,
                        stdout=BytesIO()
                    )

                for i in range(2):
                    self.ctx.cluster.only(client).run(
                        args=['radosgw-admin', 'gc',
                              'process', '--include-all'],
                        stdout=BytesIO()
                    )

    def remove_tests(self, client):
        log.info('S3 Tests Java: Cleaning up s3-tests-java...')
        testdir = teuthology.get_testdir(self.ctx)

        if self.log_fwd:
            self.ctx.cluster.only(client).run(
                args=['cd',
                      '{tdir}/s3-tests-java'.format(tdir=testdir),
                      run.Raw('&&'),
                      'cat', self.log_name,
                      run.Raw('&&'),
                      'rm', self.log_name],
                stdout=BytesIO()
            )

        self.ctx.cluster.only(client).run(
            args=[
                'rm',
                '-rf',
                '{tdir}/s3-tests-java'.format(tdir=testdir),
            ],
            stdout=BytesIO()
        )

    def delete_users(self, client):
        log.info("S3 Tests Java: Deleting S3 users...")
        testdir = teuthology.get_testdir(self.ctx)
        for section, user in self.users.items():
            s3_user_id = '{user}.{client}'.format(user=user, client=client)
            self.ctx.cluster.only(client).run(
                args=[
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
                stdout=BytesIO()
            )


task = S3tests_java
