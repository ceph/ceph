"""
rgw multisite testing
"""
import logging
import sys
import nose.core
import nose.config

from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology import misc

from rgw_multi import multisite, tests

log = logging.getLogger(__name__)

class RGWMultisiteTests(Task):
    """
    Runs the rgw_multi tests against a multisite configuration created by the
    rgw-multisite task. Tests are run with nose, using any additional 'args'
    provided. Overrides for tests.Config can be set in 'config'.

        - rgw-multisite-tests:
            args:
            - tasks.rgw_multi.tests:test_object_sync
            config:
              reconfigure_delay: 60

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

        # create the test user
        log.info('creating test user..')
        user = multisite.User('rgw-multisite-test-user')
        user.create(master_zone, ['--display-name', 'Multisite Test User',
                                  '--gen-access-key', '--gen-secret'])

        config = self.config.get('config', {})
        tests.init_multi(realm, user, tests.Config(**config))
        tests.realm_meta_checkpoint(realm)

    def begin(self):
        # extra arguments for nose can be passed as a string or list
        extra_args = self.config.get('args', [])
        if not isinstance(extra_args, list):
            extra_args = [extra_args]
        argv = [__name__] + extra_args

        log.info("running rgw multisite tests on '%s' with args=%r",
                 tests.__name__, extra_args)

        # run nose tests in the rgw_multi.tests module
        conf = nose.config.Config(stream=get_log_stream(), verbosity=2)
        result = nose.run(defaultTest=tests.__name__, argv=argv, config=conf)
        if not result:
            raise RuntimeError('rgw multisite test failures')

def get_log_stream():
    """ return a log stream for nose output """
    # XXX: this is a workaround for IOErrors when nose writes to stderr,
    # copied from vstart_runner.py
    class LogStream(object):
        def __init__(self):
            self.buffer = ""

        def write(self, data):
            self.buffer += data
            if "\n" in self.buffer:
                lines = self.buffer.split("\n")
                for line in lines[:-1]:
                    log.info(line)
                self.buffer = lines[-1]

        def flush(self):
            pass

    return LogStream()

task = RGWMultisiteTests
