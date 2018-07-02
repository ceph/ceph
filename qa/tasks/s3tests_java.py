from cStringIO import StringIO
import logging

import base64
import os
import random
import string
import yaml
import socket
import getpass

from teuthology import misc as teuthology
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.orchestra.remote import Remote

log = logging.getLogger(__name__)

"""
    Task for running RGW S3 tests with the AWS Java SDK
    
    To run all tests on all clients::

        tasks:
        - ceph:
        - rgw:
        - s3tests-java:

    To restrict testing to particular clients::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests-java: [client.0]

    To pass extra arguments to gradle (e.g. to run a certain test, 
    specify a different repository and branch for the test suite, 
    run in debug mode or forward the gradle output to a log file)::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests_java_local:
            branch: master
            repo: 'https://github.com/adamyanova/java_s3tests.git'
            sha1:
            client.0:
                extra_args: ['--tests', 'ObjectTest.testEncryptionKeySSECInvalidMd5']
                debug:
                log_fwd: ../sample_log.txt     
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
        self.all_clients = [clients[0]]
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
        for (host, roles) in self.ctx.cluster.remotes.iteritems():
            log.info(
                'S3 Tests Java: Cluster config is: {cfg}'.format(cfg=roles))
            log.info('S3 Tests Java: Host is: {host}'.format(host=host))
        self.create_users()
        self.run_tests()

    def teardown(self):
        super(S3tests_java, self).teardown()
        log.debug('S3 Tests Java: TEARDOWN')
        for client in self.all_clients:
            self.remove_tests(client)
            self.delete_users(client)

    def download_test_suite(self, client):
        log.info("S3 Tests Java Local: Downloading test suite...")
        testdir = teuthology.get_testdir(self.ctx)
        if 'branch' in self.config and self.config['branch'] is not None:
            branch = self.config['branch']
        else:
            branch = 'master'
        if 'repo' in self.config and self.config['repo'] is not None:
            repo = self.config['repo']
        else:
            repo = 'https://github.com/ceph/java_s3tests.git'
        self.ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
                repo,
                '{tdir}/s3-tests-java'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )

        if 'sha1' in self.config and self.config['sha1'] is not None:
            self.ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/s3-tests-java'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', self.config['sha1'],
                ],
            )

        if 'debug' in self.config[client]:
            self.ctx.cluster.only(client).run(
                args=['mkdir', '-p',
                      '{tdir}/s3-tests-java/src/main/resources/'.format(
                          tdir=testdir),
                      run.Raw('&&'),
                      'cp',
                      '{tdir}/s3-tests-java/log4j.properties'.format(
                          tdir=testdir),
                      '{tdir}/s3-tests-java/src/main/resources/'.format(
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
            stdout=StringIO()
        )

        # The openssl_keys task generates a self signed certificate for each client
        # It is located in the {testdir}/ca/ and should be added to the java keystore
        for task in self.ctx.config['tasks']:
            if 'openssl_keys' in task:
                endpoint = self.ctx.rgw.role_endpoints.get(client)
                path = 'lib/security/cacerts'
                self.ctx.cluster.only(client).run(
                    args=['sudo',
                          'keytool',
                          '-import', '-alias', '{alias}'.format(
                              alias=endpoint.hostname),
                          '-keystore',
                          run.Raw(
                              '$(readlink -e $(dirname $(readlink -e $(which keytool)))/../{path})'.format(path=path)),
                          '-file', '{tdir}/ca/rgw.{client}.crt'.format(
                              tdir=testdir, client=client),
                          '-storepass', 'changeit',
                          ],
                    stdout=StringIO()
                )

    def create_users(self):
        """
        Create a main and an alternative s3 user.
        Configuration is read from a skelethon config file
        s3tests.teuth.config.yaml in the java-s3tests repository
        and missing information is added from the task.
        Existing values are NOT overriden unless they are empty!
        """
        log.info("S3 Tests Java: Creating users...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            endpoint = self.ctx.rgw.role_endpoints.get(client)
            local_user = getpass.getuser()
            remote_user = teuthology.get_test_user()
            os.system("scp {remote}@{host}:{tdir}/s3-tests-java/s3tests.teuth.config.yaml /home/{local}/".format(
                host=endpoint.hostname, tdir=testdir, remote=remote_user, local=local_user))
            s3tests_conf = teuthology.config_file(
                '/home/{local}/s3tests.teuth.config.yaml'.format(local=local_user))
            log.info("S3 Tests Java: s3tests_conf is {s3cfg}".format(
                s3cfg=s3tests_conf))
            for section, user in self.users.items():
                if section in s3tests_conf:
                    userid = '{user}.{client}'.format(user=user, client=client)
                    log.debug(
                        'S3 Tests Java: Creating user {userid}'.format(userid=userid))
                    self._config_user(s3tests_conf=s3tests_conf,
                                      section=section, user=userid, client=client)
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
                        stdout=StringIO()
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
        access_secret = base64.b64encode(os.urandom(40))
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

        log.info("S3 Tests Java: s3tests_conf[{sect}] is {s3cfg}".format(
            sect=section, s3cfg=s3tests_conf[section]))
        log.debug('S3 Tests Java: Setion, User = {sect}, {user}'.format(
            sect=section, user=user))

    def _write_cfg_file(self, cfg_dict, client):
        """
        To write the final s3 tests config file on the remote
        a temporary one is created on the local machine
        """
        testdir = teuthology.get_testdir(self.ctx)
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        with open('s3_tests_tmp.yaml', 'w') as outfile:
            yaml.dump(cfg_dict, outfile, default_flow_style=False)

        conf_fp = StringIO()
        with open('s3_tests_tmp.yaml', 'r') as infile:
            for line in infile:
                conf_fp.write(line)

        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/s3-tests-java.{client}.conf'.format(
                tdir=testdir, client=client),
            data=conf_fp.getvalue(),
        )
        os.remove('s3_tests_tmp.yaml')

    def _set_cfg_entry(self, cfg_dict, key, value):
        if not (key in cfg_dict):
            cfg_dict.setdefault(key, value)
        elif cfg_dict[key] is None:
            cfg_dict[key] = value

    def run_tests(self):
        log.info("S3 Tests Java Local: Running tests...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            self.ctx.cluster.only(client).run(
                args=['cp',
                      '{tdir}/archive/s3-tests-java.{client}.conf'.format(
                          tdir=testdir, client=client),
                      '{tdir}/s3-tests-java/config.properties'.format(
                          tdir=testdir)
                      ],
                stdout=StringIO()
            )
            args = ['cd',
                    '{tdir}/s3-tests-java'.format(tdir=testdir),
                    run.Raw('&&'),
                    '/opt/gradle/gradle-4.7/bin/gradle', 'clean', 'test',
                    '-S', '--console', 'verbose', '--no-build-cache',
                    ]
            if 'extra_args' in self.config[client]:
                args.extend(self.config[client]['extra_args'])
            if 'debug' in self.config[client]:
                args += ['--debug']
            if 'log_fwd' in self.config[client]:
                log_name = '{tdir}/s3tests_log.txt'.format(tdir=testdir)
                if self.config[client]['log_fwd'] is not None:
                    log_name = self.config[client]['log_fwd']
                args += [run.Raw('>>'),
                         log_name]

            self.ctx.cluster.only(client).run(
                args=args,
                stdout=StringIO()
            )

    def remove_tests(self, client):
        log.info('S3 Tests Java: Removing s3-tests-java...')
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=[
                'rm',
                '-rf',
                '{tdir}/s3-tests-java'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )

    def delete_users(self, client):
        log.info("S3 Tests Java: Deleting users...")
        testdir = teuthology.get_testdir(self.ctx)
        for section, user in self.users.items():
            userid = '{user}.{client}'.format(user=user, client=client)
            self.ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    'user', 'rm',
                    '--uid', userid,
                    '--purge-data',
                    '--cluster', 'ceph',
                ],
                stdout=StringIO()
            )


task = S3tests_java
