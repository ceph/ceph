import logging
import os
import yaml

from teuthology import misc
from teuthology.config import config as teuth_config
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
        cluster_config = dict(
            user=self.config.get('cluster', {}).get('user', 'ubuntu'),
            head=mon_hosts[0],
            osds=osd_hosts,
            mons=mon_hosts,
            clients=client_hosts,
            osds_per_node=self.config.get('cluster', {}).get('osds_per_node', 1),
            rebuild_every_test=False,
            use_existing=True,
            iterations=self.config.get('cluster', {}).get('iterations', 1),
            tmp_dir='/tmp/cbt',
            pool_profiles=self.config.get('cluster', {}).get('pool_profiles'),
            )
        benchmark_config = self.config.get('benchmarks')
        benchmark_type = benchmark_config.keys()[0]
        if benchmark_type == 'librbdfio':
          testdir = misc.get_testdir(self.ctx)
          benchmark_config['librbdfio']['cmd_path'] = os.path.join(testdir, 'fio/fio')
        return dict(
            cluster=cluster_config,
            benchmarks=benchmark_config,
            )

    def install_dependencies(self):
        system_type = misc.get_system_type(self.first_mon)

        if system_type == 'rpm':
            install_cmd = ['sudo', 'yum', '-y', 'install']
            cbt_depends = ['python-yaml', 'python-lxml', 'librbd-devel', 'pdsh', 'collectl']
        else:
            install_cmd = ['sudo', 'apt-get', '-y', '--force-yes', 'install']
            cbt_depends = ['python-yaml', 'python-lxml', 'librbd-dev', 'collectl']
        self.first_mon.run(args=install_cmd + cbt_depends)
         
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

    def checkout_cbt(self):
        testdir = misc.get_testdir(self.ctx)
        repo = self.config.get('repo', 'https://github.com/ceph/cbt.git')
        branch = self.config.get('branch', 'master')
        branch = self.config.get('force-branch', branch)
        sha1 = self.config.get('sha1')
        self.first_mon.run(
            args=[
                'git', 'clone', '-b', branch, repo,
                '{tdir}/cbt'.format(tdir=testdir)
            ]
        )
        if sha1:
            self.first_mon.run(
                args=[
                    'cd', os.path.join(testdir, 'cbt'), run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                ]
            )

    def setup(self):
        super(CBT, self).setup()
        self.first_mon = self.ctx.cluster.only(misc.get_first_mon(self.ctx, self.config)).remotes.keys()[0]
        self.cbt_config = self.generate_cbt_config()
        self.log.info('cbt configuration is %s', self.cbt_config)
        self.cbt_dir = os.path.join(misc.get_archive_dir(self.ctx), 'cbt')
        self.ctx.cluster.run(args=['mkdir', '-p', '-m0755', '--', self.cbt_dir])
        misc.write_file(self.first_mon, os.path.join(self.cbt_dir, 'cbt_config.yaml'),
                        yaml.safe_dump(self.cbt_config, default_flow_style=False))
        self.checkout_cbt()
        self.install_dependencies()

    def begin(self):
        super(CBT, self).begin()
        testdir = misc.get_testdir(self.ctx)
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
        self.first_mon.run(
            args=[
                'rm', '--one-file-system', '-rf', '--',
                '{tdir}/fio'.format(tdir=testdir),
            ]
        )

task = CBT
