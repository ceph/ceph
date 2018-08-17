"""
Task for running RGW S3 tests with the AWS GO SDK
"""
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
    Task for running RGW S3 tests with the AWS Go SDK
    
    Tests run only on clients specified in the s3tests-go config section. 
    If no client is given a default 'client.0' is chosen.
    If it does not match the rgw client the task will fail.
        
        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests-go:
            client.0:

    Extra arguments can be passed by adding options to the corresponding client
    section under the s3tests-go task (e.g. to run a certain test, 
    specify a different repository and branch for the test suite, 
    or forward the test output to a log file):

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests-go:
            client.0:
                force-branch: wip
                force-repo: 'https://github.com/adamyanova/go_s3tests.git'
                log-fwd: '../s3tests-go.log'
                extra-args: ['-run', 'TestSuite/TestSSEKMS']

    To run a specific test or tests with names containing an expresssing, provide the expression to the extra-args section e.g.:

        - s3tests-go:
            client.0:
                extra-args: ['-run', 'TestSuite/TestSSEKMS']
    
"""


class S3tests_go(Task):
    """
    Download and install S3TestsGo
    This will require golang
    """

    def __init__(self, ctx, config):
        super(S3tests_go, self).__init__(ctx, config)
        self.log = log
        log.debug('S3 Tests GO: __INIT__ ')
        assert hasattr(ctx, 'rgw'), 'S3tests_go must run after the rgw task'
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            self.all_clients = 'client.0'
        self.users = {'s3main': 'tester', 's3alt': 'johndoe'}

    def setup(self):
        super(S3tests_go, self).setup()
        log.debug('S3 Tests GO: SETUP')
        for client in self.all_clients:
            self.download_test_suite(client)
            self.install_required_packages(client)

    def begin(self):
        super(S3tests_go, self).begin()
        log.debug('S3 Tests GO: BEGIN')
        self.create_users()
        self.run_tests()

    def end(self):
        super(S3tests_go, self).end()
        log.debug('S3 Tests GO: END')
        for client in self.all_clients:
            self.remove_tests(client)
            self.delete_users(client)

    def download_test_suite(self, client):
        log.info("S3 Tests GO: Downloading test suite...")
        testdir = teuthology.get_testdir(self.ctx)
        branch = 'master'
        repo = 'https://github.com/ceph/go_s3tests.git'
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
                '{tdir}/s3-tests-go'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )
        if client in self.config and self.config[client] is not None:
            if 'sha1' in self.config[client] and self.config[client]['sha1'] is not None:
                self.ctx.cluster.only(client).run(
                    args=[
                        'cd', '{tdir}/s3-tests-go'.format(tdir=testdir),
                        run.Raw('&&'),
                        'git', 'reset', '--hard', self.config[client]['sha1'],
                    ],
                )

    def install_required_packages(self, client):
        """
        Run bootstrap.sh script to install golang.
        """
        log.info("S3 Tests GO: Installing required packages...")
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=['{tdir}/s3-tests-go/bootstrap.sh'.format(tdir=testdir)],
            stdout=StringIO()
        )
        remote_user = teuthology.get_test_user()
        self.gopath = '/home/{remote}/go'.format(remote=remote_user)
        self._setup_golang(client)
        self._install_tests_pkgs(client)

    def _setup_golang(self, client):
        self.ctx.cluster.only(client).run(
            args = ['mkdir', '-p',
                self.gopath,
                run.Raw('&&'),
                'GOPATH={path}'.format(path = self.gopath)
            ],
            stdout = StringIO()
            )

    def _install_tests_pkgs(self, client):
        testdir = teuthology.get_testdir(self.ctx)
        # explicit download of stretchr/testify is required
        self.ctx.cluster.only(client).run(
                args = ['cd', 
                    '{tdir}/s3-tests-go'.format(tdir = testdir),
                    run.Raw(';'),
                    'go', 'get', '-v', '-d', './...',
                    run.Raw(';'),
                    'go', 'get', '-v', 'github.com/stretchr/testify',
                ],
                stdout = StringIO()
            )


    def create_users(self):
        """
        Create a main and an alternative s3 user.
        Configuration is read from a skelethon config file
        s3tests.teuth.config.yaml in the go_s3tests repository
        and missing information is added from the task.
        Existing values are NOT overriden unless they are empty!
        """
        log.info("S3 Tests GO: Creating S3 users...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            endpoint = self.ctx.rgw.role_endpoints.get(client)
            local_user = getpass.getuser()
            remote_user = teuthology.get_test_user()
            os.system("scp {remote}@{host}:{tdir}/s3-tests-go/s3tests.teuth.config.yaml /home/{local}/".format(
                host=endpoint.hostname, tdir=testdir, remote=remote_user, local=local_user))
            s3tests_conf = teuthology.config_file(
                '/home/{local}/s3tests.teuth.config.yaml'.format(local=local_user))
            self._s3tests_cfg_default_section(client = client, cfg_dict = s3tests_conf)
            for section, user in self.users.items():
                if section in s3tests_conf:
                    s3_user_id = '{user}.{client}'.format(user=user, client=client)
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
                    self.ctx.cluster.only(client).run(
                        args=args,
                        stdout=StringIO()
                    )
                else:
                    self.users.pop(section)
            self._write_cfg_file(s3tests_conf, client)
            os.system(
                "rm -rf /home/{local}/s3tests.teuth.config.yaml".format(local=local_user))

    def _s3tests_cfg_default_section(self, client, cfg_dict):
        endpoint = self.ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'S3 Tests Go: No RGW endpoint for {clt}'.format(clt = client) 

        cfg_dict['DEFAULT']['host'] = endpoint.hostname
        cfg_dict['DEFAULT']['port'] = endpoint.port
        cfg_dict['DEFAULT']['is_secure'] = True if endpoint.cert else False

    def _config_user(self, s3tests_conf, section, user, client):
        """
        Generate missing users data for this section by stashing away keys, ids, and
        email addresses.
        """

        self._set_cfg_entry(
            s3tests_conf[section], 'user_id', '{user}'.format(user=user))
        self._set_cfg_entry(
            s3tests_conf[section], 'email', '{user}_test@test.test'.format(user=user))
        self._set_cfg_entry(
            s3tests_conf[section], 'display_name', 'Ms. {user}'.format(user=user))
        access_key = ''.join(random.choice(string.ascii_uppercase)
                             for i in range(20))
        secret = base64.b64encode(os.urandom(40))
        self._set_cfg_entry(
            s3tests_conf[section], 'access_key', '{ak}'.format(ak=access_key))
        self._set_cfg_entry(
            s3tests_conf[section], 'access_secret', '{sk}'.format(sk=secret))
        self._set_cfg_entry(s3tests_conf[section], 'region', 'us-east-1')
        self._set_cfg_entry(s3tests_conf[section], 'bucket', 'bucket1')
        self._set_cfg_entry(s3tests_conf[section], 'SSE', 'aws:kms')

        endpoint = self.ctx.rgw.role_endpoints.get(client)
        self._set_cfg_entry(s3tests_conf[section], 'endpoint', '{ip}:{port}'.format(
            ip=endpoint.hostname, port=endpoint.port))
        self._set_cfg_entry(s3tests_conf[section], 'port', endpoint.port)
        self._set_cfg_entry(
            s3tests_conf[section], 'is_secure', True if endpoint.cert else False)

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
            path='{tdir}/archive/s3-tests-go.{client}.conf'.format(
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
        log.info("S3 Tests GO: Running tests...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            self.ctx.cluster.only(client).run(
                args=['cp',
                      '{tdir}/archive/s3-tests-go.{client}.conf'.format(
                          tdir=testdir, client=client),
                      '{tdir}/s3-tests-go/config.yaml'.format(
                          tdir=testdir)
                      ],
                stdout=StringIO()
            )
            args = ['cd',
                    '{tdir}/s3-tests-go/s3tests'.format(tdir=testdir),
                    run.Raw('&&'),
                    'go', 'test', '-v'
                    ]
            extra_args = []
            self.log_fwd = False
            if client in self.config and self.config[client] is not None:
                if 'extra-args' in self.config[client]:
                    extra_args.extend(self.config[client]['extra-args'])
                if 'log-fwd' in self.config[client]:
                    self.log_fwd = True
                    self.log_name = '{tdir}/s3tests-go-log.txt'.format(tdir=testdir)
                    if self.config[client]['log-fwd'] is not None:
                        self.log_name = self.config[client]['log-fwd']
                    extra_args += [run.Raw('>>'),
                            self.log_name]

            for i in range(2):
                self.ctx.cluster.only(client).run(
                    args=['radosgw-admin', 'gc', 'process', '--include-all'],
                    stdout=StringIO()
                )

            self.ctx.cluster.only(client).run(
                args= args + extra_args,
                stdout=StringIO()
            )

            for i in range(2):
                self.ctx.cluster.only(client).run(
                    args=['radosgw-admin', 'gc', 'process', '--include-all'],
                    stdout=StringIO()
                )


    def remove_tests(self, client):
        log.info('S3 Tests GO: Removing s3-tests-go...')
        testdir = teuthology.get_testdir(self.ctx)

        if self.log_fwd:
            self.ctx.cluster.only(client).run(
                args=['cd',
                        '{tdir}/s3-tests-go'.format(tdir=testdir),
                        run.Raw('&&'),
                        'cat', self.log_name,
                        run.Raw('&&'),
                        'rm', self.log_name],
                stdout=StringIO()
            )

        self.ctx.cluster.only(client).run(
            args=[
                'rm',
                '-rf',
                '{tdir}/s3-tests-go'.format(tdir=testdir),
                '{gopath}'.format(gopath = self.gopath)  
            ],
            stdout=StringIO()
        )

    def delete_users(self, client):
        log.info("S3 Tests GO: Deleting S3 users...")
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

task = S3tests_go
