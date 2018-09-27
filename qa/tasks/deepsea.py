'''
Task that deploys a Ceph cluster using DeepSea
'''
import logging
import os.path
import time

from salt_manager import SaltManager
from teuthology import misc
from teuthology.exceptions import (CommandFailedError, ConfigError)
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)

class DeepSea(Task):
    """
    Install DeepSea on the Salt Master node.

    Assumes a Salt cluster is already running (use the Salt task to achieve this).

    This task understands the following config keys:

        repo: (DeepSea git repo, e.g. https://github.com/SUSE/DeepSea.git)
        branch: (DeepSea git branch, e.g. master)
        install:
            package|pkg deepsea will be installed via package system
            source|src  deepsea will be installed via 'make install' (default)

    Example:

        tasks
        - deepsea:
            repo: https://github.com/SUSE/DeepSea.git
            branch: wip-foo
            install: source

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)

        def _check_config_key(key, default_value):
            if key not in self.config or not self.config[key]:
                self.config[key] = default_value

        _check_config_key('install', 'source')
        _check_config_key('repo', 'https://github.com/SUSE/DeepSea.git')
        _check_config_key('branch', 'master')

        log.debug("Munged config is {}".format(self.config))

        self.sm = SaltManager(self.ctx, self.config)
        self.master_remote = self.sm.master_remote

    def __purge_osds(self):
        # FIXME: purge osds only on nodes that have osd role
        for _remote in self.ctx.cluster.remotes.iterkeys():
            self.log.info("stopping OSD services on {}"
                .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'systemctl stop ceph-osd.target ; sleep 10'
                ])
            self.log.info("unmounting OSD partitions on {}"
                .format(_remote.hostname))
            # bluestore XFS partition is vd?1 - unmount up to five OSDs
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'for f in vdb1 vdc1 vdd1 vde1 vdf1 ; do test -b /dev/$f && umount /dev/$f || true ; done'
                ])
            # filestore XFS partition is vd?2 - unmount up to five OSDs
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'for f in vdb2 vdc2 vdd2 vde2 vdf2; do test -b /dev/$f && umount /dev/$f || true ; done'
                ])

    def __install_deepsea_from_source(self):
        self.log.info("Installing DeepSea from source - repo: {}, branch: {}"
                      .format(self.config["repo"], self.config["branch"]))
        self.master_remote.run(args=[
            'git',
            '--version',
            run.Raw(';'),
            'git',
            'clone',
            '--branch',
            self.config["branch"],
            self.config["repo"],
            run.Raw(';'),
            'cd',
            'DeepSea',
            run.Raw(';'),
            'git',
            'rev-parse',
            '--abbrev-ref',
            'HEAD',
            run.Raw(';'),
            'git',
            'rev-parse',
            'HEAD',
            run.Raw(';'),
            'git',
            'describe',
            run.Raw('||'),
            'true',
            ])

        self.log.info("Running \"make install\" in DeepSea clone...")
        self.master_remote.run(args=[
            'cd',
            'DeepSea',
            run.Raw(';'),
            'sudo',
            'make',
            'install',
            ])

        self.log.info("installing deepsea dependencies...")
        self.master_remote.run(args = [
            'sudo',
            'zypper',
            '--non-interactive',
            'install',
            '--no-recommends',
            run.Raw('$(rpmspec --requires -q DeepSea/deepsea.spec.in 2>/dev/null)')
            ])

    def __install_deepsea_using_zypper(self):
            self.log.info("Installing DeepSea using zypper")
            self.master_remote.run(args=[
                'sudo',
                'zypper',
                '--non-interactive',
                'search',
                '--details',
                'deepsea'
                ])
            self.master_remote.run(args=[
                'sudo',
                'zypper',
                '--non-interactive',
                '--no-gpg-checks',
                'install',
                '--force',
                '--no-recommends',
                'deepsea',
                'deepsea-cli',
                'deepsea-qa'
                ])

    def setup(self):
        super(DeepSea, self).setup()
        log.debug("beginning of DeepSea task setup method...")
        pass
        log.debug("end of DeepSea task setup...")

    def begin(self):
        super(DeepSea, self).begin()
        log.debug("beginning of DeepSea task begin method...")
        self.master_remote.run(args=[
            'sudo',
            'rpm',
            '-q',
            'ceph-test'
            ])
        suite_path = self.ctx.config.get('suite_path')
        log.info("suite_path is ->{}<-".format(suite_path))
        if self.config['install'] in ['source', 'src']:
            self.__install_deepsea_from_source()
        elif self.config['install'] in ['package', 'pkg']:
            self.__install_deepsea_using_zypper()
        else:
            raise ConfigError("Unsupported deepsea install method '%s'"
                                                % self.config['install'])
        log.debug("end of DeepSea task begin method...")

    def end(self):
        super(DeepSea, self).end()
        log.debug("beginning of DeepSea task end method...")
        self.sm.gather_logfile('deepsea.log')
        self.sm.gather_logs('ganesha')
        log.debug("end of DeepSea task end method...")

    def teardown(self):
        super(DeepSea, self).teardown()
        log.debug("beginning of DeepSea task teardown method...")
        self.__purge_osds()
        log.debug("end of DeepSea task teardown method...")


task = DeepSea
