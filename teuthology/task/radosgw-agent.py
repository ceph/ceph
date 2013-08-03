import contextlib
import logging
import argparse

from ..orchestra import run
from teuthology import misc as teuthology
import teuthology.task_util.rgw as rgw_utils

log = logging.getLogger(__name__)

def run_radosgw_agent(ctx, client, config):
    """
    Run a single radosgw-agent. See task() for config format.
    """
    src_client = config['src']
    dest_client = config['dest']

    src_zone = rgw_utils.zone_for_client(ctx, src_client)
    dest_zone = rgw_utils.zone_for_client(ctx, dest_client)

    log.info("source is %s", src_zone)
    log.info("dest is %s", dest_zone)

    testdir = teuthology.get_testdir(ctx)
    (remote,) = ctx.cluster.only(client).remotes.keys()
    remote.run(
        args=[
            'cd', testdir, run.Raw('&&'),
            'git', 'clone', 'https://github.com/ceph/radosgw-agent.git',
            'radosgw-agent.{client}'.format(client=client),
            run.Raw('&&'),
            'cd', 'radosgw-agent.{client}'.format(client=client),
            run.Raw('&&'),
            './bootstrap',
            ]
        )

    src_host, src_port = rgw_utils.get_zone_host_and_port(ctx, src_client,
                                                          src_zone)
    dest_host, dest_port = rgw_utils.get_zone_host_and_port(ctx, dest_client,
                                                            dest_zone)
    src_access, src_secret = rgw_utils.get_zone_system_keys(ctx, src_client,
                                                            src_zone)
    dest_access, dest_secret = rgw_utils.get_zone_system_keys(ctx, dest_client,
                                                              dest_zone)
    port = config.get('port', 8000)
    daemon_name = '{host}.syncdaemon'.format(host=remote.name)

    return remote.run(
        args=[
            '{tdir}/daemon-helper'.format(tdir=testdir), 'kill',
            '{tdir}/radosgw-agent.{client}/radosgw-agent'.format(tdir=testdir,
                                                                 client=client),
            '-v',
            '--src-access-key', src_access,
            '--src-secret-key', src_secret,
            '--src-host', src_host,
            '--src-port', str(src_port),
            '--src-zone', src_zone,
            '--dest-access-key', dest_access,
            '--dest-secret-key', dest_secret,
            '--dest-host', dest_host,
            '--dest-port', str(dest_port),
            '--dest-zone', dest_zone,
            '--daemon-id', daemon_name,
            '--test-server-host', '0.0.0.0', '--test-server-port', str(port),
            '--log-file', '{tdir}/archive/rgw_sync_agent.{client}.log'.format(
                tdir=testdir,
                client=client),
            ],
        wait=False,
        stdin=run.PIPE,
        logger=log.getChild(daemon_name)
        )


@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosgw-agents in test mode.

    Configuration is clients to run the agents on, with settings for
    source client, destination client, and port to listen on.  Binds
    to 0.0.0.0. Port defaults to 8000. This must be run on clients
    that have the correct zone root pools and rgw zone set in
    ceph.conf, or the task cannot read the region information from the
    cluster. An example::

      tasks:
      - ceph:
          conf:
            client.0:
              rgw zone = foo
              rgw zone root pool = .root.pool
            client.1:
              rgw zone = bar
              rgw zone root pool = .root.pool2
      - rgw: # region configuration omitted for brevity
      - radosgw-agent:
          client.0:
            src: client.0
            dest: client.1
            # port: 8000 (default)
          client.1:
            src: client.1
            dest: client.0
            port: 8001
    """
    assert isinstance(config, dict), 'rgw_sync_agent requires a dictionary config'
    log.debug("config is %s", config)


    ctx.radosgw_agent = argparse.Namespace()
    ctx.radosgw_agent.config = config

    procs = [(client, run_radosgw_agent(ctx, client, c_config)) for
             client, c_config in config.iteritems()]

    ctx.radosgw_agent.procs = procs

    try:
        yield
    finally:
        testdir = teuthology.get_testdir(ctx)
        try:
            for client, proc in procs:
                log.info("shutting down sync agent on %s", client)
                proc.stdin.close()
                proc.exitstatus.get()
        finally:
            for client, proc in procs:
                ctx.cluster.only(client).run(
                    args=[
                        'rm', '-rf',
                        '{tdir}/radosgw-agent.{client}'.format(tdir=testdir,
                                                               client=client)
                        ]
                    )
