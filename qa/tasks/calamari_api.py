import contextlib
import logging
import os
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
     Run Calamari API tests

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('calamari-api', {}))
    api_node = ctx.cluster.only(teuthology.get_first_mon(ctx, config))

    # clone the repo

    api_node.run(args=['sudo', 'rm', '-rf', 'api-tests'], check_status=False)
    api_node.run(args=['sudo', 'rm', '-rf', run.Raw('/tmp/apilog*')], check_status=False)
    api_node.run(args=['mkdir', 'api-tests'])
    api_node.run(args=['cd', 'api-tests', run.Raw(';'), 'git', 'clone',
                 'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git'])

    api_node.run(args=['sudo', run.Raw('calamari-ctl'), 'initialize', run.Raw('--admin-username admin'),
                       run.Raw('--admin-password admin123'), run.Raw('--admin-email api@redhat.com')])

    # restart calamari due to bz

    api_node.run(args=['sudo', 'service', 'supervisord', 'restart'])
    api_node.run(args=['sudo',  'supervisorctl', 'restart', 'all'])

    api_node.run(args=['cd', 'api-tests/ceph-qe-scripts/calamari/api_test_cases',
                       run.Raw(';'), run.Raw('sh run_calamari_tests.sh')], timeout=600)

    try:
        yield
    finally:
        log.info("Deleting repo's")
        if ctx.archive is not None:
            path = os.path.join(ctx.archive, 'remote')
            os.makedirs(path)
            sub = os.path.join(path, api_node.shortname)
            os.makedirs(sub)
            teuthology.pull_directory(api_node, '/tmp/apilog',
                                      os.path.join(sub, 'log'))
        api_node.run(args=['sudo', 'rm', '-rf', 'api-tests'])
