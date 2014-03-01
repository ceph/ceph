"""
Force pg creation on all osds
"""
from teuthology import misc as teuthology
from ..orchestra import run
import logging

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Create the specified number of pools and write 16 objects to them (thereby forcing
    the PG creation on each OSD). This task creates pools from all the clients,
    in parallel. It is easy to add other daemon types which have the appropriate
    permissions, but I don't think anything else does.
    The config is just the number of pools to create. I recommend setting
    "mon create pg interval" to a very low value in your ceph config to speed
    this up.
    
    You probably want to do this to look at memory consumption, and
    maybe to test how performance changes with the number of PGs. For example:
    
    tasks:
    - ceph:
        config:
          mon:
            mon create pg interval: 1
    - manypools: 3000
    - radosbench:
        clients: [client.0]
        time: 360
    """
    
    log.info('creating {n} pools'.format(n=config))
    
    poolnum = int(config)
    creator_remotes = []
    client_roles = teuthology.all_roles_of_type(ctx.cluster, 'client')
    log.info('got client_roles={client_roles_}'.format(client_roles_=client_roles))
    for role in client_roles:
        log.info('role={role_}'.format(role_=role))
        creator_remote = teuthology.get_single_remote_value(ctx,
                'client.{id}'.format(id=role))
        creator_remotes.append((creator_remote, 'client.{id}'.format(id=role)))

    remaining_pools = poolnum
    poolprocs=dict()
    while (remaining_pools > 0):
        log.info('{n} pools remaining to create'.format(n=remaining_pools))
	for remote, role_ in creator_remotes:
            poolnum = remaining_pools
            remaining_pools -= 1
            if remaining_pools < 0:
                continue
            log.info('creating pool{num} on {role}'.format(num=poolnum, role=role_))
	    proc = remote.run(
	        args=[
		    'rados',
		    '--name', role_,
		    'mkpool', 'pool{num}'.format(num=poolnum), '-1',
		    run.Raw('&&'),
		    'rados',
		    '--name', role_,
		    '--pool', 'pool{num}'.format(num=poolnum),
		    'bench', '0', 'write', '-t', '16', '--block-size', '1'
		    ],
		wait = False
	    )
            log.info('waiting for pool and object creates')
	    poolprocs[remote] = proc
        
        run.wait(poolprocs.itervalues())
    
    log.info('created all {n} pools and wrote 16 objects to each'.format(n=poolnum))
