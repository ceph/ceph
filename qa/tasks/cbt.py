import logging
import os
import yaml
from tasks.cbt_performance import CBTperformance

from teuthology import misc
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)


class CBT(Task):
    """
    Passes through a CBT configuration yaml fragment.
    """
    def __init__(self, ctx, config):
        super(CBT, self).__init__(ctx, config)
        self.log = log

    def hosts_of_type(self, type_):
        return [r.name for r in self.ctx.cluster.only(misc.is_type(type_)).remotes.keys()]

    def generate_cbt_config(self):
        mon_hosts = self.hosts_of_type('mon')
        osd_hosts = self.hosts_of_type('osd')
        client_hosts = self.hosts_of_type('client')
        rgw_client = {}
        rgw_client[client_hosts[0]] = None
        rgw_hosts = self.config.get('cluster', {}).get('rgws', rgw_client)
        cluster_config = dict(
            user=self.config.get('cluster', {}).get('user', 'ubuntu'),
            head=mon_hosts[0],
            osds=osd_hosts,
            mons=mon_hosts,
            clients=client_hosts,
            rgws=rgw_hosts,
            osds_per_node=self.config.get('cluster', {}).get('osds_per_node', 1),
            rebuild_every_test=False,
            use_existing=True,
            is_teuthology=self.config.get('cluster', {}).get('is_teuthology', True),
            iterations=self.config.get('cluster', {}).get('iterations', 1),
            tmp_dir='/tmp/cbt',
            pool_profiles=self.config.get('cluster', {}).get('pool_profiles'),
            pid_dir=self.config.get('cluster', {}).get('pid_dir', '/var/run/ceph'),
            )

        benchmark_config = self.config.get('benchmarks')
        benchmark_type = next(iter(benchmark_config.keys()))
        if benchmark_type in ['librbdfio', 'fio']:
          testdir = misc.get_testdir(self.ctx)
          benchmark_config[benchmark_type]['cmd_path'] = os.path.join(testdir, 'fio/fio')
        if benchmark_type == 'cosbench':
            # create cosbench_dir and cosbench_xml_dir
            testdir = misc.get_testdir(self.ctx)
            benchmark_config['cosbench']['cosbench_dir'] = os.path.join(testdir, 'cos')
            benchmark_config['cosbench']['cosbench_xml_dir'] = os.path.join(testdir, 'xml')
            self.ctx.cluster.run(args=['mkdir', '-p', '-m0755', '--', benchmark_config['cosbench']['cosbench_xml_dir']])
            benchmark_config['cosbench']['controller'] = osd_hosts[0]

            # set auth details
            remotes_and_roles = self.ctx.cluster.remotes.items()
            ips = [host for (host, port) in
                   (remote.ssh.get_transport().getpeername() for (remote, role_list) in remotes_and_roles)]
            benchmark_config['cosbench']['auth'] = "username=cosbench:operator;password=intel2012;url=http://%s:80/auth/v1.0;retry=9" %(ips[0])
        client_endpoints_config = self.config.get('client_endpoints', None)
        monitoring_profiles = self.config.get('monitoring_profiles', None)

        return dict(
            cluster=cluster_config,
            benchmarks=benchmark_config,
            client_endpoints = client_endpoints_config,
            monitoring_profiles = monitoring_profiles,
            )

    def install_dependencies(self):
        system_type = misc.get_system_type(self.first_mon)

        if system_type == 'rpm':
            install_cmd = ['sudo', 'yum', '-y', 'install']
            cbt_depends = ['python3-yaml', 'python3-lxml', 'librbd-devel', 'pdsh', 'pdsh-rcmd-ssh','perf']
            self.log.info('Installing collectl')
            collectl_location = "https://sourceforge.net/projects/collectl/files/collectl/collectl-4.3.1/collectl-4.3.1.src.tar.gz/download"
            self.first_mon.run(
                args=[
                    'sudo', 'mkdir', 'collectl', run.Raw('&&'),
                    'cd', 'collectl', run.Raw('&&'),
                    'sudo', 'wget', collectl_location, '-O', 'collectl.tar.gz', run.Raw('&&'),
                    'sudo', 'tar', '-xvf', 'collectl.tar.gz' , run.Raw('&&'),
                    'cd', 'collectl-4.3.1', run.Raw('&&'),
                    'sudo', './INSTALL'
                ]
            )
        else:
            install_cmd = ['sudo', 'apt-get', '-y', '--force-yes', 'install']
            cbt_depends = ['python3-yaml', 'python3-lxml', 'librbd-dev', 'collectl', 'linux-tools-generic']
        self.first_mon.run(args=install_cmd + cbt_depends)

        benchmark_type = next(iter(self.cbt_config.get('benchmarks').keys()))
        self.log.info('benchmark: %s', benchmark_type)

        if benchmark_type in ['librbdfio', 'fio']:
            # install fio
            testdir = misc.get_testdir(self.ctx)
            self.first_mon.run(
                args=[
                    'git', 'clone', '-b', 'master',
                    'https://github.com/axboe/fio.git',
                    '{tdir}/fio'.format(tdir=testdir)
                ]
            )
            self.first_mon.run(
                args=[
                    'cd', os.path.join(testdir, 'fio'), run.Raw('&&'),
                    './configure', run.Raw('&&'),
                    'make'
                ]
            )

        if benchmark_type == 'cosbench':
            # install cosbench
            self.log.info('install dependencies for cosbench')
            if system_type == 'rpm':
                cosbench_depends = ['wget', 'unzip', 'java-1.7.0-openjdk', 'curl']
            else:
                cosbench_depends = ['wget', 'unzip', 'openjdk-8-jre', 'curl']
            self.first_mon.run(args=install_cmd + cosbench_depends)
            testdir = misc.get_testdir(self.ctx)
            cosbench_version = '0.4.2.c3'
            cosbench_location = 'https://github.com/intel-cloud/cosbench/releases/download/v0.4.2.c3/0.4.2.c3.zip'
            os_version = misc.get_system_type(self.first_mon, False, True)

            # additional requirements for bionic
            if os_version == '18.04':
                self.first_mon.run(
                    args=['sudo', 'apt-get', '-y', 'purge', 'openjdk-11*'])
                # use our own version of cosbench
                cosbench_version = 'cosbench-0.4.2.c3.1'
                # contains additional parameter "-N" to nc
                cosbench_location = 'http://drop.ceph.com/qa/cosbench-0.4.2.c3.1.zip'
                cosbench_dir = os.path.join(testdir, cosbench_version)
                self.ctx.cluster.run(args=['mkdir', '-p', '-m0755', '--', cosbench_dir])
                self.first_mon.run(
                    args=[
                        'cd', testdir, run.Raw('&&'),
                        'wget',
                        cosbench_location, run.Raw('&&'),
                        'unzip', '{name}.zip'.format(name=cosbench_version), '-d', cosbench_version
                    ]
                )
            else:
                self.first_mon.run(
                    args=[
                        'cd', testdir, run.Raw('&&'),
                        'wget',
                        cosbench_location, run.Raw('&&'),
                        'unzip', '{name}.zip'.format(name=cosbench_version)
                    ]
                )
            self.first_mon.run(
                args=[
                    'cd', testdir, run.Raw('&&'),
                    'ln', '-s', cosbench_version, 'cos',
                ]
            )
            self.first_mon.run(
                args=[
                    'cd', os.path.join(testdir, 'cos'), run.Raw('&&'),
                    'chmod', '+x', run.Raw('*.sh'),
                ]
            )

            # start cosbench and check info
            self.log.info('start cosbench')
            self.first_mon.run(
                args=[
                    'cd', testdir, run.Raw('&&'),
                    'cd', 'cos', run.Raw('&&'),
                    'sh', 'start-all.sh'
                ]
            )
            self.log.info('check cosbench info')
            self.first_mon.run(
                args=[
                    'cd', testdir, run.Raw('&&'),
                    'cd', 'cos', run.Raw('&&'),
                    'sh', 'cli.sh', 'info'
                ]
            )

    def checkout_cbt(self):
        testdir = misc.get_testdir(self.ctx)
        repo = self.config.get('repo', 'https://github.com/ceph/cbt.git')
        branch = self.config.get('branch', 'master')
        branch = self.config.get('force-branch', branch)
        sha1 = self.config.get('sha1')
        if sha1 is None:
            self.first_mon.run(
                args=[
                    'git', 'clone', '--depth', '1', '-b', branch, repo,
                    '{tdir}/cbt'.format(tdir=testdir)
                ]
            )
        else:
            self.first_mon.run(
                args=[
                    'git', 'clone', '-b', branch, repo,
                    '{tdir}/cbt'.format(tdir=testdir)
                ]
            )
            self.first_mon.run(
                args=[
                    'cd', os.path.join(testdir, 'cbt'), run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                ]
            )

    def setup(self):
        super(CBT, self).setup()
        self.first_mon = next(iter(self.ctx.cluster.only(misc.get_first_mon(self.ctx, self.config)).remotes.keys()))
        self.cbt_config = self.generate_cbt_config()
        self.log.info('cbt configuration is %s', self.cbt_config)
        self.cbt_dir = os.path.join(misc.get_archive_dir(self.ctx), 'cbt')
        self.ctx.cluster.run(args=['mkdir', '-p', '-m0755', '--', self.cbt_dir])
        self.first_mon.write_file(
                os.path.join(self.cbt_dir, 'cbt_config.yaml'),
                yaml.safe_dump(self.cbt_config, default_flow_style=False))
        self.checkout_cbt()
        self.install_dependencies()

    def begin(self):
        super(CBT, self).begin()
        testdir = misc.get_testdir(self.ctx)
        # disable perf_event_paranoid to allow perf to run
        self.first_mon.run(
            args=[
                'sudo',
                '/sbin/sysctl',
                '-q',
                '-w',
                'kernel.perf_event_paranoid=0',
            ],
        )
        self.first_mon.run(
            args=[
                '{tdir}/cbt/cbt.py'.format(tdir=testdir),
                '-a', self.cbt_dir,
                '{cbtdir}/cbt_config.yaml'.format(cbtdir=self.cbt_dir),
            ],
        )
        preserve_file = os.path.join(self.ctx.archive, '.preserve')
        open(preserve_file, 'a').close()

    def end(self):
        super(CBT, self).end()
        testdir = misc.get_testdir(self.ctx)
        self.first_mon.run(
            args=[
                'rm', '--one-file-system', '-rf', '--',
                '{tdir}/cbt'.format(tdir=testdir),
            ]
        )
        benchmark_type = next(iter(self.cbt_config.get('benchmarks').keys()))
        if benchmark_type in ['librbdfio', 'fio']:
            self.first_mon.run(
                args=[
                    'rm', '--one-file-system', '-rf', '--',
                    '{tdir}/fio'.format(tdir=testdir),
                ]
            )

        if benchmark_type == 'cosbench':
            os_version = misc.get_system_type(self.first_mon, False, True)
            if os_version == '18.04':
                cosbench_version = 'cosbench-0.4.2.c3.1'
            else:
                cosbench_version = '0.4.2.c3'
            # note: stop-all requires 'nc'
            self.first_mon.run(
                args=[
                    'cd', testdir, run.Raw('&&'),
                    'cd', 'cos', run.Raw('&&'),
                    'sh', 'stop-all.sh',
                    run.Raw('||'), 'true'
                ]
            )
            self.first_mon.run(
                args=[
                    'sudo', 'killall', '-9', 'java',
                    run.Raw('||'), 'true'
                ]
            )
            self.first_mon.run(
                args=[
                    'rm', '--one-file-system', '-rf', '--',
                    '{tdir}/cos'.format(tdir=testdir),
                ]
            )
            self.first_mon.run(
                args=[
                    'rm', '--one-file-system', '-rf', '--',
                    '{tdir}/{version}'.format(tdir=testdir, version=cosbench_version),
                ]
            )
            self.first_mon.run(
                args=[
                    'rm', '--one-file-system', '-rf', '--',
                    '{tdir}/{version}.zip'.format(tdir=testdir, version=cosbench_version),
                ]
            )
            self.first_mon.run(
                args=[
                    'rm', '--one-file-system', '-rf', '--',
                    '{tdir}/xml'.format(tdir=testdir),
                ]
            )
        # Collect cbt performance data
        cbt_performance = CBTperformance()
        cbt_performance.collect(self.ctx, self.config)

task = CBT
