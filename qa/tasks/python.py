import logging
from teuthology import misc as teuthology
from tasks.vip import subst_vip

log = logging.getLogger(__name__)


def task(ctx, config):
    """
    Execute some python code.
 
      tasks:
      - python:
          host.a: |
            import boto3
            c = boto3.resource(...)

    or

      tasks:
      - python:
          sudo: true
          host.b: |
            import boto3
            c = boto3.resource(...)

    The provided dict is normally indexed by role.  However, you can also include a 
    'sudo: true' key to run the code with sudo.
    """
    assert isinstance(config, dict), "task python got invalid config"

    testdir = teuthology.get_testdir(ctx)

    sudo = config.pop('sudo', False)

    for role, code in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Running python on role %s host %s', role, remote.name)
        log.info(code)
        args=[
            'TESTDIR={tdir}'.format(tdir=testdir),
            'python3',
        ]
        remote.run(args=args, stdin=subst_vip(ctx, code))

