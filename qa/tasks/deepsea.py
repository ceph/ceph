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
        log.debug("beginning of constructor method")
        if 'install' in self.config:
            if self.config['install'] in ['package', 'pkg']:
                self.config['install'] = 'package'
            elif self.config['install'] in ['source', 'src']:
                self.config['install'] = 'source'
            else:
                raise ConfigError(
                        "deepsea: unrecognized install config value ->{}<-"
                        .format(self.config['install'])
                    )
        else:
            if 'repo' in self.config or 'branch' in self.config:
                self.config['install'] = 'source'
            else:
                self.config['install'] = 'package'
        self.sm = SaltManager(self.ctx)
        self.master_remote = self.sm.master_remote
        # log.debug("ctx.config {}".format(ctx.config))
        log.debug("munged config is {}".format(self.config))
        log.debug("end of constructor method")

    def _disable_gpg_checks(self):
        log.info("disabling zypper GPG checks on all test nodes")
        self.master_remote.run(args=(
            "echo \"pre-disable repo state\" ; "
            "zypper lr -upEP ; grep gpg /etc/zypp/repos.d/*"
            ))
        cmd = (
            'sed -i -e \'/gpgcheck/ d\' /etc/zypp/repos.d/* ; '
            'sed -i -e \'/gpgkey/ d\' /etc/zypp/repos.d/* ; '
            'sed -i -e \'$a gpgcheck=0\' /etc/zypp/repos.d/*'
            )
        log.info("zypper repos after disabling GPG checks")
        self.ctx.cluster.run(args=[
            'sudo', 'sh', '-c', cmd
            ])
        self.master_remote.run(args=(
            "echo \"post-disable repo state\" ; "
            "zypper lr -upEP ; grep gpg /etc/zypp/repos.d/*"
            ))

    def _install_deepsea(self):
        if self.config['install'] == 'package':
            self._install_deepsea_using_zypper()
        elif self.config['install'] == 'source':
            self._install_deepsea_from_source()
        else:
            raise ConfigError(
                    "deepsea: unrecognized install config value ->{}<-"
                    .format(self.config['install'])
                )

    def _install_deepsea_from_source(self):
        """Install DeepSea from source (unless already installed from RPM)"""
        if self.sm.master_rpm_q('deepsea'):
            log.info("DeepSea already installed from RPM")
            return None
        check_config_key(
                self.config,
                'repo',
                'https://github.com/SUSE/DeepSea.git'
            )
        check_config_key(self.config, 'branch', 'master')
        log.info("Installing DeepSea from source - repo: {}, branch: {}"
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
        log.info("Running \"make install\" in DeepSea clone...")
        self.master_remote.run(args=[
            'cd',
            'DeepSea',
            run.Raw(';'),
            'sudo',
            'make',
            'install',
            ])
        log.info("installing deepsea dependencies...")
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
        log.info("Installing DeepSea using zypper")
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
            log.info("stopping OSD services on {}"
                     .format(_remote.hostname))
            _remote.run(args=[
                'sudo', 'sh', '-c',
                'systemctl stop ceph-osd.target ; sleep 10'
                ])
            log.info("unmounting OSD partitions on {}"
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
        # log.debug("beginning of setup method")
        pass
        # log.debug("end of setup method")

    def begin(self):
        super(DeepSea, self).begin()
        log.debug("beginning of begin method")
        self._disable_gpg_checks()
        self._install_deepsea()
        log.debug("end of begin method")

    def end(self):
        super(DeepSea, self).end()
        # log.debug("beginning of end method")
        pass
        # log.debug("end of end method")

    def teardown(self):
        super(DeepSea, self).teardown()
        log.debug("beginning of teardown method")
        self._purge_osds()
        log.debug("end of teardown method")


task = DeepSea
