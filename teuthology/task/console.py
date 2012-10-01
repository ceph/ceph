import contextlib
import logging
import os
import re
import subprocess

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    if config is None:
        config = {}
        for _, roles_for_host in ctx.cluster.remotes.iteritems():
            config[roles_for_host[0]] = {}
    assert isinstance(config, dict)

    log.info('Console config is %s', config)

    procs = {}
    if ctx.archive is not None:
        path = os.path.join(ctx.archive, 'console')
        os.makedirs(path)

        for role in config.iterkeys():
            # figure out ipmi host
            (rem, ) = ctx.cluster.only(role).remotes.keys()
            log.info(' role %s remote %s', role, rem)
            match = re.search('@((plana|burnupi)\d\d)\.', rem.name);
            if match:
                host = match.group(1) + '.ipmi.sepia.ceph.com'
                htype = match.group(2)
                log.info('Attaching to console on %s', host)
                subprocess.call([
                        'ipmitool',
                        '-I', 'lanplus',
                        '-U', htype + 'temp',
                        '-P', htype + 'temp',
                        '-H', host,
                        'sol', 'deactivate'
                        ])
                procs[rem] = subprocess.Popen(
                    args=[
                        'ipmitool',
                        '-I', 'lanplus',
                        '-U', htype + 'temp',
                        '-P', htype + 'temp',
                        '-H', host,
                        'sol', 'activate'
                        ],
                    stdout=open(os.path.join(path, host), 'w'),
                    stderr=open(os.devnull, 'w'),
                    stdin=subprocess.PIPE,
                    )
        
    try:
        yield
    finally:
        for rem, proc in procs.iteritems():
            log.info('Terminating %s console', rem.name)
            proc.stdin.write('~.\n')
            proc.wait()
