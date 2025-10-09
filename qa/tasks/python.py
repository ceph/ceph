import logging
from teuthology import misc as teuthology
from tasks import template

log = logging.getLogger(__name__)


def task(ctx, config):
    """
    Execute some python code.
 
      tasks:
      - python:
          host.a: |
            import boto3
            c = boto3.resource(...)

    The provided dict is normally indexed by role.  You can also include a 
    'sudo: false' key to run the code without sudo.

      tasks:
      - python:
          sudo: false
          host.b: |
            import boto3
            c = boto3.resource(...)
    """
    assert isinstance(config, dict), "task python got invalid config"

    testdir = teuthology.get_testdir(ctx)

    sudo = config.pop('sudo', True)

    for role, code in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Running python on role %s host %s', role, remote.name)
        log.info(code)
        args=[
            'TESTDIR={tdir}'.format(tdir=testdir),
            'python3',
        ]
        if sudo:
            args = ['sudo'] + args
        code = template.transform(ctx, config, code)
        remote.run(args=args, stdin=code)

