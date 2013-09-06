import contextlib
import logging
import argparse

from ..orchestra import run
from teuthology import misc as teuthology
import teuthology.task_util.rgw as rgw_utils

log = logging.getLogger(__name__)

def run_radosgw_agent(ctx, config):
    """
    Run a single radosgw-agent. See task() for config format.
    """
    return_list = list()
    for (client, cconf) in config.items():
        # don't process entries that are not clients
        if not client.startswith('client.'):
            log.debug('key {data} does not start with \'client.\', moving on'.format(
                      data=client))
            continue

        src_client = cconf['src']
        dest_client = cconf['dest']

        src_zone = rgw_utils.zone_for_client(ctx, src_client)
        dest_zone = rgw_utils.zone_for_client(ctx, dest_client)

        log.info("source is %s", src_zone)
        log.info("dest is %s", dest_zone)

        testdir = teuthology.get_testdir(ctx)
        (remote,) = ctx.cluster.only(client).remotes.keys()
        # figure out which branch to pull from
        branch = cconf.get('force-branch', None)
        if not branch:
            branch = cconf.get('branch', 'master')
        sha1 = cconf.get('sha1')
        remote.run(
            args=[
                'cd', testdir, run.Raw('&&'),
                'git', 'clone', 
                '-b', branch,
                'https://github.com/ceph/radosgw-agent.git',
                'radosgw-agent.{client}'.format(client=client),
                ]
            )
        if sha1 is not None:
            remote.run(
                args=[
                    'cd', testdir, run.Raw('&&'),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                ]
            )
        remote.run(
            args=[
                'cd', testdir, run.Raw('&&'),
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
        sync_scope = cconf.get('sync-scope', None)
        port = cconf.get('port', 8000)
        daemon_name = '{host}.{port}.syncdaemon'.format(host=remote.name, port=port)
        in_args=[
		        'daemon-helper',
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
		        '--log-file', '{tdir}/archive/rgw_sync_agent.{client}.log'.format(
		            tdir=testdir,
		            client=client),
		        ]
        # the test server and full/incremental flags are mutually exclusive
        if sync_scope is None:
            in_args.append('--test-server-host')
            in_args.append('0.0.0.0') 
            in_args.append('--test-server-port')
            in_args.append(str(port))
            log.debug('Starting a sync test server on {client}'.format(client=client))
            # Stash the radosgw-agent server / port # for use by subsequent tasks 
            ctx.radosgw_agent.endpoint = (client, str(port))
        else:
            in_args.append('--sync-scope')
            in_args.append(sync_scope)
            log.debug('Starting a {scope} sync on {client}'.format(scope=sync_scope,client=client))

        return_list.append((client, remote.run(
            args=in_args,
		        wait=False,
		        stdin=run.PIPE,
		        logger=log.getChild(daemon_name),
		        )))
    return return_list


@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosgw-agents in test mode.

    Configuration is clients to run the agents on, with settings for
    source client, destination client, and port to listen on.  Binds
    to 0.0.0.0. Port defaults to 8000. This must be run on clients
    that have the correct zone root pools and rgw zone set in
    ceph.conf, or the task cannot read the region information from the
    cluster. 

    By default, this task will start an HTTP server that will trigger full
    or incremental syncs based on requests made to it. 
    Alternatively, a single full sync can be triggered by
    specifying 'sync-scope: full' or a loop of incremental syncs can be triggered
    by specifying 'sync-scope: incremental' (the loop will sleep
    '--incremental-sync-delay' seconds between each sync, default is 20 seconds).
    
    An example::

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
            branch: wip-next-feature-branch
            src: client.0
            dest: client.1
            sync-scope: full
            # port: 8000 (default)
          client.1:
            src: client.1
            dest: client.0
            port: 8001
    """
    assert isinstance(config, dict), 'rgw_sync_agent requires a dictionary config'
    log.debug("config is %s", config)

    overrides = ctx.config.get('overrides', {})
    # merge each client section, but only if it exists in config since there isn't
    # a sensible default action for this task
    for client in config.iterkeys():
        if config[client]:
            log.debug('config[{client}]: {data}'.format(client=client, data=config[client]))
            teuthology.deep_merge(config[client], overrides.get('radosgw-agent', {}))

    ctx.radosgw_agent = argparse.Namespace()
    ctx.radosgw_agent.config = config

    procs = run_radosgw_agent(ctx, config)

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
