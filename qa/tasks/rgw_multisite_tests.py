"""
rgw multisite testing
"""
import importlib.util
import logging
import subprocess
import sys

from teuthology.config import config as teuth_config
from teuthology.exceptions import ConfigError
from teuthology.repo_utils import fetch_repo
from teuthology.task import Task
from teuthology import misc

log = logging.getLogger(__name__)


class RGWMultisiteTests(Task):
    """
    Runs the rgw_multi tests against a multisite configuration created by the
    rgw-multisite task. Tests are run with unittest, using any additional
    'args' provided. Overrides for tests.Config can be set in 'config'. The
    'branch' and 'repo' can be overridden to clone the rgw_multi tests from
    another release.

        - rgw-multisite-tests:
            args:
            - tests.py:test_object_sync
            config:
              reconfigure_delay: 60
            branch: octopus
            repo: https://github.com/ceph/ceph.git

    """
    def __init__(self, ctx, config):
        super(RGWMultisiteTests, self).__init__(ctx, config)

    def setup(self):
        super(RGWMultisiteTests, self).setup()

        overrides = self.ctx.config.get('overrides', {})
        misc.deep_merge(self.config, overrides.get('rgw-multisite-tests', {}))

        if not self.ctx.rgw_multisite:
            raise ConfigError('rgw-multisite-tests must run after the rgw-multisite task')
        realm = self.ctx.rgw_multisite.realm
        master_zone = realm.meta_master_zone()

        branch = self.config.get('branch')
        if not branch:
            # run from suite_path
            suite_path = self.ctx.config.get('suite_path')
            self.module_path = suite_path + '/../src/test/rgw/rgw_multi'
        else:
            # clone the qa branch
            repo = self.config.get('repo', teuth_config.get_ceph_qa_suite_git_url())
            log.info("cloning suite branch %s from %s...", branch, repo)
            clonedir = fetch_repo(repo, branch)
            # import its version of rgw_multi
            self.module_path = clonedir + '/src/test/rgw/rgw_multi'

        log.info("importing tests from %s", self.module_path)
        spec = importlib.util.spec_from_file_location('rgw_multi', self.module_path + '/__init__.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        from rgw_multi import multisite, tests

        # create the test user
        log.info('creating test user..')
        user = multisite.User('rgw-multisite-test-user')
        user.create(master_zone, ['--display-name', 'Multisite Test User',
                                  '--gen-access-key', '--gen-secret'])

        config = self.config.get('config', {})
        tests.init_multi(realm, user, tests.Config(**config))
        tests.realm_meta_checkpoint(realm)

    def begin(self):
        # extra arguments can be passed as a string or list
        extra_args = self.config.get('args', [])
        if not isinstance(extra_args, list):
            extra_args = [extra_args]

        log.info("running rgw multisite tests on '%s' with args=%r",
                 self.module_path, extra_args)

        argv = [sys.executable, self.module_path + "../test_multi.py"] + extra_args
        testsuite = subprocess.run(argv, check=True)


task = RGWMultisiteTests
