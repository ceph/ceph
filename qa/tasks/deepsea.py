'''
Task that installs DeepSea
'''
import logging

from salt_manager import SaltManager
from teuthology.exceptions import ConfigError
from teuthology.orchestra import run
from teuthology.task import Task
from util import check_config_key

log = logging.getLogger(__name__)


class DeepSea(Task):
    """
    Install DeepSea on the Salt Master node.

    Assumes a Salt cluster is already running (use the Salt task to achieve
    this).

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
        check_config_key(
                self.config,
                'repo',
                'https://github.com/SUSE/DeepSea.git'
            )
        check_config_key(self.config, 'branch', 'master')
        install_conf = check_config_key(self.config, 'install', 'source')
        self._determine_install_method(install_conf)
        self.sm = SaltManager(self.ctx, self.config)
        self.master_remote = self.sm.master_remote
#       self.log.debug("ctx.config {}".format(ctx.config))
        log.debug("Munged config is {}".format(self.config))

    def _determine_install_method(self, conf_val):
        install_lookup = {
                'source': self._install_deepsea_from_source,
                'src': self._install_deepsea_from_source,
                'package': self._install_deepsea_using_zypper,
                'pkg': self._install_deepsea_using_zypper,
            }
        if conf_val in ['source', 'src', 'package', 'pkg']:
            self._install_deepsea = install_lookup[conf_val]
        else:
            raise ConfigError(
                    "deepsea: unrecognized install config value ->{}<-"
                    .format(conf_val)
                )

    def _master_whoami(self):
        """Demonstrate that remote.run() does not run stuff as root"""
        self.master_remote.run(args=[
            'whoami',
        ])

    def _install_deepsea_from_source(self):
        """Install DeepSea from source (unless already installed from RPM)"""
        if self.sm.master_rpm_q('deepsea'):
            self.log.info("DeepSea already installed from RPM")
            return None
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
        rpmspec_cmd = (
                '$(rpmspec --requires -q DeepSea/deepsea.spec.in 2>/dev/null)'
            )
        self.master_remote.run(args=[
            'sudo',
            'zypper',
            '--non-interactive',
            'install',
            '--no-recommends',
            run.Raw(rpmspec_cmd)
            ])

    def _install_deepsea_using_zypper(self):
        """Install DeepSea using zypper"""
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

    def _purge_osds(self):
        # needed as long as teuthology install task purges /var/lib/ceph
        # in its teardown phase
        for _remote in self.ctx.cluster.remotes.iterkeys():
            self.log.info("stopping OSD services on {}"
                          .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'systemctl stop ceph-osd.target ; sleep 10'
                ])
            self.log.info("unmounting OSD partitions on {}"
                          .format(_remote.hostname))
            # unmount up to five OSDs
            # bluestore XFS partition is vd?1
            # filestore XFS partition is vd?2
            for_loop = (
                    'for f in vdb{pn} vdc{pn} vdd{pn} vde{pn} vdf{pn} ; '
                    'do test -b /dev/$f && umount /dev/$f || true ; '
                    'done'
                )
            for pn in [1, 2]:
                _remote.run(args=['sudo', 'sh', '-c', for_loop.format(pn=1)])

    def setup(self):
        super(DeepSea, self).setup()
#       log.debug("beginning of DeepSea task setup method...")
        pass
#       log.debug("end of DeepSea task setup...")

    def begin(self):
        super(DeepSea, self).begin()
        log.debug("beginning of DeepSea task begin method...")
        self._install_deepsea()
        log.debug("end of DeepSea task begin method...")

    def end(self):
        super(DeepSea, self).end()
#       log.debug("beginning of DeepSea task end method...")
        pass
#       log.debug("end of DeepSea task end method...")

    def teardown(self):
        super(DeepSea, self).teardown()
        log.debug("beginning of DeepSea task teardown method...")
        self._purge_osds()
        log.debug("end of DeepSea task teardown method...")


task = DeepSea
