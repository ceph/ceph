import contextlib
import json
import logging
import os

from cStringIO import StringIO

from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run
import ceph_manager

log = logging.getLogger(__name__)

# this was lifted from radosgw-admin-rest.
def rgwadmin(ctx, remote, cmd):
    log.info('radosgw-admin: %s' % cmd)
    testdir = teuthology.get_testdir(ctx)
    pre = [
        '{tdir}/adjust-ulimits'.format(tdir=testdir),
        'ceph-coverage'.format(tdir=testdir),
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'radosgw-admin'.format(tdir=testdir),
        '--log-to-stderr',
        '--format', 'json',
        ]
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        check_status=False,
        stdout=StringIO(),
        stderr=StringIO(),
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.info(' json result: %s' % j)
        except ValueError:
            j = out
            log.info(' raw result: %s' % j)
    return (r, j)

# this was lifted from lost_unfound.py
def rados(ctx, remote, cmd):
    testdir = teuthology.get_testdir(ctx)
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        '{tdir}/adjust-ulimits'.format(tdir=testdir),
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'rados',
    ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        check_status=False
        )

    return proc.exitstatus

@contextlib.contextmanager
def create_dirs(ctx, config):
    log.info('Creating apache directories...')
    testdir = teuthology.get_testdir(ctx)
    for client in config.iterkeys():
        ctx.cluster.only(client).run(
            args=[
                'mkdir',
                '-p',
                '{tdir}/apache/htdocs'.format(tdir=testdir),
                '{tdir}/apache/tmp'.format(tdir=testdir),
                run.Raw('&&'),
                'mkdir',
                '{tdir}/archive/apache'.format(tdir=testdir),
                ],
            )
    try:
        yield
    finally:
        log.info('Cleaning up apache directories...')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/apache/tmp'.format(tdir=testdir),
                    run.Raw('&&'),
                    'rmdir',
                    '{tdir}/apache/htdocs'.format(tdir=testdir),
                    run.Raw('&&'),
                    'rmdir',
                    '{tdir}/apache'.format(tdir=testdir),
                    ],
                )


@contextlib.contextmanager
def ship_config(ctx, config):
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    log.info('Shipping apache config and rgw.fcgi...')
    src = os.path.join(os.path.dirname(__file__), 'apache.conf.template')
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
        if system_type == 'deb':
            mod_path = '/usr/lib/apache2/modules'
            print_continue = 'on'
        else:
            mod_path = '/usr/lib64/httpd/modules'
            print_continue = 'off'
        with file(src, 'rb') as f:
            conf = f.read().format(
                testdir=testdir,
                mod_path=mod_path,
                print_continue=print_continue,
                )
            teuthology.write_file(
                remote=remote,
                path='{tdir}/apache/apache.conf'.format(tdir=testdir),
                data=conf,
                )
        teuthology.write_file(
            remote=remote,
            path='{tdir}/apache/htdocs/rgw.fcgi'.format(tdir=testdir),
            data="""#!/bin/sh
ulimit -c unlimited
exec radosgw -f
""".format(tdir=testdir)
            )
        remote.run(
            args=[
                'chmod',
                'a=rx',
                '{tdir}/apache/htdocs/rgw.fcgi'.format(tdir=testdir),
                ],
            )
    try:
        yield
    finally:
        log.info('Removing apache config...')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/apache/apache.conf'.format(tdir=testdir),
                    run.Raw('&&'),
                    'rm',
                    '-f',
                    '{tdir}/apache/htdocs/rgw.fcgi'.format(tdir=testdir),
                    ],
                )


@contextlib.contextmanager
def start_rgw(ctx, config):
    log.info('Starting rgw...')
    testdir = teuthology.get_testdir(ctx)
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()

        client_config = config.get(client)
        if client_config is None:
            client_config = {}
        log.info("rgw %s config is %s", client, client_config)
        id_ = client.split('.', 1)[1]
        log.info('client {client} is id {id}'.format(client=client, id=id_))
        run_cmd=[
            'sudo',
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                '{tdir}/daemon-helper'.format(tdir=testdir),
                'term',
            ]
        run_cmd_tail=[
                'radosgw',
                # authenticate as the client this is co-located with
                '--name', '{client}'.format(client=client),
                # use the keyring for the client we're running as
                '-k', '/etc/ceph/ceph.{client}.keyring'.format(client=client),
                '--log-file', '/var/log/ceph/rgw.log',
                '--rgw_ops_log_socket_path', '{tdir}/rgw.opslog.sock'.format(tdir=testdir),
                '{tdir}/apache/apache.conf'.format(tdir=testdir),
                '--foreground',
                run.Raw('|'),
                'sudo',
                'tee',
                '/var/log/ceph/rgw.stdout'.format(tdir=testdir),
                run.Raw('2>&1'),
            ]

        run_cmd.extend(
            teuthology.get_valgrind_args(
                testdir,
                client,
                client_config.get('valgrind')
                )
            )

        run_cmd.extend(run_cmd_tail)

        ctx.daemons.add_daemon(
            remote, 'rgw', client,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )

    try:
        yield
    finally:
        teuthology.stop_daemons_of_type(ctx, 'rgw')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/rgw.opslog.sock'.format(tdir=testdir),
                    ],
                )


@contextlib.contextmanager
def start_apache(ctx, config):
    log.info('Starting apache...')
    testdir = teuthology.get_testdir(ctx)
    apaches = {}
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
        if system_type == 'deb':
            apache_name = 'apache2'
        else:
            apache_name = '/usr/sbin/httpd'
        proc = remote.run(
            args=[
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                '{tdir}/daemon-helper'.format(tdir=testdir),
                'kill',
                apache_name,
                '-X',
                '-f',
                '{tdir}/apache/apache.conf'.format(tdir=testdir),
                ],
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )
        apaches[client] = proc

    try:
        yield
    finally:
        log.info('Stopping apache...')
        for client, proc in apaches.iteritems():
            proc.stdin.close()

        run.wait(apaches.itervalues())

# pulls the configured zone info out and also 
# inserts some sensible defaults. 
def extract_zone_info(zone_info):

    # now send the zone settings across the wire
    system_user = zone_info['user']
    system_access_key = zone_info['access_key']
    system_secret_key = zone_info['secret_key']
    zone_suffix = zone_info['zone_suffix']

    # new dict to hold the data
    zone_dict = {}
    zone_dict['domain_root'] = '.rgw.root' + zone_suffix
    zone_dict['control_pool'] = '.rgw.control' + zone_suffix
    zone_dict['gc_pool'] = '.rgw.gc' + zone_suffix
    zone_dict['log_pool'] = '.log' + zone_suffix
    zone_dict['intent_log_pool'] = '.intent-log' + zone_suffix
    zone_dict['usage_log_pool'] = '.usage' + zone_suffix
    zone_dict['user_keys_pool'] = '.users' + zone_suffix
    zone_dict['user_email_pool'] = '.users.email' + zone_suffix
    zone_dict['user_swift_pool'] = '.users.swift' + zone_suffix
    zone_dict['user_uid_pool'] = '.users.id' + zone_suffix

    system_user_dict = {}
    system_user_dict['user'] = system_user
    system_user_dict['access_key'] = system_access_key
    system_user_dict['secret_key'] = system_secret_key

    zone_dict['system_key'] = system_user_dict

    return zone_dict

# pulls the configured region info out and also 
# inserts some sensible defaults. 
def extract_region_info(region_info, host_name):
    # create a new, empty dict to populate
    region_dict = {}
    # use specifed region name or default <client name>_'region'
    if region_info.has_key('name'):
        region_dict['name'] = region_info['name']
    else: 
        region_dict['name'] = '{client}_region'.format(client=client)

    # use specified api name or default to 'default'
    if region_info.has_key('api_name'):
        region_dict['api_name'] = region_info['api_name']
    else: 
        region_dict['api_name'] = 'default'

    # Going to assume this is specified for now
    if region_info.has_key('is_master'):
        region_dict['is_master'] = region_info['is_master']
    else: 
        pass
        #region_dict['is_master'] = 'false'

    # use specified api name or default to 'default'
    if region_info.has_key('api_name'):
        region_dict['api_name'] = region_info['api_name']
    else: 
        region_dict['api_name'] = 'default'

    # add in sensible defaults for the endpoints field 
    if region_info.has_key('endpoints'):
        region_dict['endpoints'] = region_info['endpoints']
    else: 
        region_dict['endpoints'] = []
        region_dict['endpoints'].append('http://{client}:80/'.format(client=host_name))

    # use specified master zone or default to the first specified zone 
    # (if there's more than one)
    if region_info.has_key('master_zone'):
        region_dict['master_zone'] = region_info['master_zone']
    else: 
        region_dict['master_zone'] = 'default'

    region_dict['zones'] = []
    zones = region_info['zones'].split(',')
    for zone in zones:
        # build up a zone 
        name, log_meta, log_data = zone.split(':')
        log.info('zone: {zone} meta:{meta} data:{data}'.format(zone=name, 
                                            meta=log_meta, data=log_data))
        new_zone_dict = {}
        new_zone_dict['name'] = name
        new_zone_dict['endpoints'] = []
        new_zone_dict['endpoints'].append('http://{client}:80/'.format(client=host_name))
        new_zone_dict['log_meta'] = log_meta
        new_zone_dict['log_data'] = log_data
        region_dict['zones'].append(new_zone_dict) 

    # just using defaults for now, revisit this later to allow 
    # the configs to specify placement_targets and default_placement policies
    region_dict['placement_targets'] = []
    default_placement_dict = {}
    default_placement_dict['name'] = 'default-placement'
    default_placement_dict['tags'] = []
    region_dict['placement_targets'].append(default_placement_dict)

    region_dict['default_placement'] = 'default-placement'

    return region_dict

@contextlib.contextmanager
def configure_regions_and_zones(ctx, config):
    log.info('Configuring regions and zones...')

    client_list = []
    region_files = []
    zone_files = []

    # iterate through all the clients, creating files with the region zone info and shipping those
    # to all hosts. Then, pull out the zone info and stash it in client_list
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()

        client_config = config.get(client)
        if client_config is None:
            client_config = {}

        # extract the dns name from remote
        user_name, host_name = str(remote).split('@')

        log.info("rgw %s config is %s dns is %s", client, client_config, host_name)
        
        if 'region_info' in client_config:
            region_dict = extract_region_info(client_config['region_info'], host_name)
            region_name = region_dict['name']

            log.info('constructed region info: {data}'.format(data=region_dict))
            
            file_name = region_dict['name'] + '.input'
            testdir = teuthology.get_testdir(ctx)
            region_file_path = os.path.join(testdir, file_name)

            tmpFile = StringIO()

            # use json.dumps as a pretty printer so that the generated file
            # is easier to read
            tmpFile.write('{data}'.format(data=json.dumps(region_dict, sort_keys=True,
                                          indent=4)))

            # ship this region file to all clients
            for region_client in config.iterkeys():
                (region_remote,) = ctx.cluster.only(region_client).remotes.iterkeys()

                log.info('sending region file {region_file} to {host}'.format( \
                                            region_file=region_file_path,
                                            host=str(region_remote)))


                tmpFile.seek(0)

                teuthology.write_file(
                  remote=region_remote,
                  path=region_file_path,
                  data=tmpFile,
                )

            region_files.append(region_file_path)


            # now work on the zone info
            if 'zone_info' in client_config:
                zone_info = client_config['zone_info']
                zone_dict = extract_zone_info(zone_info)
    
                zone_suffix = zone_info['zone_suffix']
                zone_name = zone_info['name']
    
                file_name = 'zone' + zone_suffix + '.input'
                testdir = teuthology.get_testdir(ctx)
                zone_file_path = os.path.join(testdir, file_name)
                log.info('Sending zone file {file_out} to host {host}'.format(file_out=zone_file_path, \
                                                                   host=host_name))
        
                tmpFile = StringIO()
        
                tmpFile.write('{data}'.format(data=json.dumps(zone_dict,  
                                              sort_keys=True, indent=4)))
    
                # zone info only gets sent to the particular client
                tmpFile.seek(0)
                teuthology.write_file(
                  remote=remote,
                  path=zone_file_path,
                  data=tmpFile,
                )
        
                zone_files.append(zone_file_path)
    
                # add an entry that is client, remote, region, zone, suffix, zone file path
                # this data is used later 
                client_entry = []
                client_entry.append(client)
                client_entry.append(remote)
                client_entry.append(region_name)
                client_entry.append(zone_name)
                client_entry.append(zone_suffix)
                client_entry.append(zone_file_path)
                client_list.append(client_entry)
    
            else:  # if 'zone_info' in client_config:
                log.info('no zone info found for client {client}'.format(client=client))

        else: # if 'region_info' in client_config:
            log.info('no region info found for client {client}'.format(client=client))


    # now apply the region info on all clients
    for region_file_path in region_files:
        for client_entry in client_list: 

            client = client_entry[0]
            remote = client_entry[1]
            region_name = client_entry[2]
            zone_name = client_entry[3]
            suffix = client_entry[4]
            zone_file_path = client_entry[5]

            log.info('cl: {client_name} rn: {region} zn: {zone} rf: {rf} suf:{suffix} zfp:{zfp}'.format(\
                 client_name=str(remote), region=region_name, zone=zone_name,
                 rf=region_file_path, suffix=suffix, zfp=zone_file_path))

            # now use this file to set the region info on the host
            (err, out) = rgwadmin(ctx, remote, ['region', 'set', '--infile', 
                              region_file_path, '--name', 
                              '{client}'.format(client=client),
                              '--rgw-region',
                              '{region}'.format(region=region_name),
                              '--rgw-zone',
                              '{zone}'.format(zone=zone_name)])
            assert not err

    # go through the clients and set the zone info
    for client_entry in client_list: 
        client = client_entry[0]
        remote = client_entry[1]
        region_name = client_entry[2]
        zone_name = client_entry[3]
        suffix = client_entry[4]
        zone_file_path = client_entry[5]


        (err, out) = rgwadmin(ctx, remote, ['zone', 'set', 
                        '--rgw-zone={zone}'.format(zone=zone_name), 
                        '--infile', zone_file_path,
                        '--name', '{client}'.format(client=client),
                        '--rgw-region',
                        '{region}'.format(region=region_name),
                        ])
        assert not err

    # set this to the default region
    for client_entry in client_list: 

        client = client_entry[0]
        remote = client_entry[1]
        region_name = client_entry[2]
        zone_name = client_entry[3]
        suffix = client_entry[4]
        zone_file_path = client_entry[5]
        log.info('rf: {rf}'.format(rf=region_file_path))
        (err, out) = rgwadmin(ctx, remote, ['region', 'default', 
                            '--name', '{client}'.format(client=client),
                            '--rgw-region',
                            '{region}'.format(region=region_name),
                            #'--rgw-zone',
                            #'{zone}'.format(zone=zone_name)
                            ])
        if out:
            log.info('JB out: {data}'.format(data=out()))
        if err:
            log.info('err: {err} out: {out}'.format(err=err, out=out))

        assert not err

    # only do this if there's at least one zone specified
    if len(client_list) > 0:
        # need the mon to make rados calls
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

        # delete the pools for the default
        err = rados(ctx, mon, ['-p', '.rgw.root', 'rm', 'region_info.default'])
        log.info('err is %d' % err)

        err = rados(ctx, mon, ['-p', '.rgw.root', 'rm', 'zone_info.default'])
        log.info('err is %d' % err)
        
    # delete the pools for the suffixs
    for client_entry in client_list: 

        client = client_entry[0]
        remote = client_entry[1]
        region_name = client_entry[2]
        zone_name = client_entry[3]
        suffix = client_entry[4]
        zone_file_path = client_entry[5]

        err = rados(ctx, mon, ['-p', '.rgw.root' + suffix, 
                               'rm', 'region_info.default'])
        log.info('err is %d' % err)
            
        err = rados(ctx, mon, ['-p', '.rgw.root' + suffix, 
                               'rm', 'zone_info.default'])
        log.info('err is %d' % err)

    # update the region map
    for client_entry in client_list: 

        client = client_entry[0]
        remote = client_entry[1]
        region_name = client_entry[2]
        zone_name = client_entry[3]
        suffix = client_entry[4]
        zone_file_path = client_entry[5]

        (err, out) = rgwadmin(ctx, remote, ['region-map', 'update',
                                            '--name', 
                                            '{client}'.format(client=client),
                                            '--rgw-region',
                                            '{region}'.format(region=region_name),
                                            '--rgw-zone',
                                            '{zone}'.format(zone=zone_name)])
        assert not err

    try:
        yield
    finally:
        log.info('Cleaning up regions and zones....')

        for client_entry in client_list: 

            client = client_entry[0]
            remote = client_entry[1]
            region_name = client_entry[2]
            zone_name = client_entry[3]
            suffix = client_entry[4]
            zone_file_path = client_entry[5]

            for region_file in region_files:
                log.info('deleting {file_name} from host {host}'.format( \
                         file_name=region_file, 
                         host=str(remote)))
                remote.run(
                    args=[
                        'rm',
                        '-f',
                        '{file_path}'.format(file_path=region_file),
                    ],
                )

            for zone_file in zone_files:
                log.info('deleting {file_name} from host {host}'.format( \
                         file_name=zone_file, 
                         host=str(remote)))
                remote.run(
                    args=[
                        'rm',
                        '-f',
                        '{file_path}'.format(file_path=zone_file),
                    ],
                )

@contextlib.contextmanager
def task(ctx, config):
    """
    Spin up apache configured to run a rados gateway.
    Only one should be run per machine, since it uses a hard-coded port for now.

    For example, to run rgw on all clients::

        tasks:
        - ceph:
        - rgw:

    To only run on certain clients::

        tasks:
        - ceph:
        - rgw: [client.0, client.3]

    or

        tasks:
        - ceph:
        - rgw:
            client.0:
            client.3:

    To run radosgw through valgrind:

        tasks:
        - ceph:
        - rgw:
            client.0:
              valgrind: [--tool=memcheck]
            client.3:
              valgrind: [--tool=memcheck]

    Note that without a modified fastcgi module e.g. with the default
    one on CentOS, you must have rgw print continue = false in ceph.conf::

        tasks:
        - ceph:
            conf:
              global:
                rgw print continue: false
        - rgw: [client.0]
    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    #overrides = ctx.config.get('overrides', {})
    #teuthology.deep_merge(config, overrides.get('rgw', {}))

    log.info('jb, config is: %s' % config)
    log.info('jb, config2 is: %s' % dict(conf=config.get('conf', {})))

    for _, roles_for_host in ctx.cluster.remotes.iteritems():
        running_rgw = False
        for role in roles_for_host:
            if role in config.iterkeys():
                assert not running_rgw, "Only one client per host can run rgw."
                running_rgw = True

    with contextutil.nested(
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: ship_config(ctx=ctx, config=config),
        lambda: start_rgw(ctx=ctx, config=config),
        lambda: start_apache(ctx=ctx, config=config),
        lambda: configure_regions_and_zones(ctx=ctx, config=config),
        ):
        yield
