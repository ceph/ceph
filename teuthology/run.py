import argparse
import os
import yaml

def config_file(string):
    config = {}
    try:
        with file(string) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                config.update(new)
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))
    return config

class MergeConfig(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        config = getattr(namespace, self.dest)
        for new in values:
            config.update(new)

def parse_args():
    parser = argparse.ArgumentParser(description='Run ceph integration tests')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
        )
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='+',
        type=config_file,
        action=MergeConfig,
        default={},
        help='config file to read',
        )
    parser.add_argument(
        '--archive',
        metavar='DIR',
        help='path to archive results in',
        )

    args = parser.parse_args()
    return args

def main():
    from gevent import monkey; monkey.patch_all()
    from orchestra import monkey; monkey.patch_all()

    import logging

    log = logging.getLogger(__name__)
    ctx = parse_args()

    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    if ctx.archive is not None:
        os.mkdir(ctx.archive)

        handler = logging.FileHandler(
            filename=os.path.join(ctx.archive, 'teuthology.log'),
            )
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S',
            )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)


        with file(os.path.join(ctx.archive, 'config.yaml'), 'w') as f:
            yaml.safe_dump(ctx.config, f, default_flow_style=False)

    log.debug('\n  '.join(['Config:', ] + yaml.safe_dump(ctx.config, default_flow_style=False).splitlines()))
    log.info('Opening connections...')

    from orchestra import connection, remote
    import orchestra.cluster

    remotes = [remote.Remote(name=t, ssh=connection.connect(t))
               for t in ctx.config['targets']]
    ctx.cluster = orchestra.cluster.Cluster()
    for rem, roles in zip(remotes, ctx.config['roles']):
        ctx.cluster.add(rem, roles)

    from teuthology.run_tasks import run_tasks
    run_tasks(tasks=ctx.config['tasks'], ctx=ctx)
