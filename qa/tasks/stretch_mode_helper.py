"""
This module contains the helper function to set up the cluster for stretch mode.
Setting up the cluster for stretch mode, the following format is used (e.g.):
    DC_OSDS = {
        'dc1': {
            "host01": [0, 1],
            "host02": [2, 3],
        },
        'dc2': {
            "host03": [4, 5],
            "host04": [6, 7],
        },
    }
    DC_MONS = {
        'dc1': {
            "host01": ['a'],
            "host02": ['b'],
        },
        'dc2': {
            "host03": ['c'],
            "host04": ['d'],
        },
        'dc3': {
            "host05": ['e'],
        }
    }
"""
import logging
from io import StringIO

log = logging.getLogger(__name__)

def _get_hosts_from_all_dc(dc_osds):
    """
    Get the hosts from the data centers.
    """
    ret = set()
    for _, hosts in dc_osds.items():
        for host, _ in hosts.items():
            ret.add(host)
    return ret

def setup_stretch_mode(
                        mgr_cluster,
                        ctx,
                        CLIENT,
                        STRETCH_BUCKET_TYPE,
                        STRETCH_SUB_BUCKET_TYPE,
                        STRETCH_CRUSH_RULE_NAME,
                        DC_OSDS,
                        DC_MONS,
                        ):
    """
    Set up the cluster for stretch mode by:
    - Setting the election strategy to "connectivity"
    - Adding and moving CRUSH buckets for data centers
    - Adding and moving CRUSH buckets for hosts
    - Moving hosts to data centers
    - Moving OSDs to the appropriate hosts
    - Setting the location of the monitors (including the tiebreaker monitor)
    - Removing the current host from the CRUSH map
    - Modifying and setting the CRUSH map
    RETURN: The ID of the newly created stretch crush rule.
    """

    # Set the election strategy to "connectivity"
    mgr_cluster.mon_manager.raw_cluster_cmd('mon', 'set', 'election_strategy', 'connectivity')

    # Add and move CRUSH buckets for data centers
    for dc in DC_OSDS:
        mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'add-bucket', dc, STRETCH_BUCKET_TYPE)
        mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'move', dc, 'root=default')

    # Add and move CRUSH buckets for hosts
    for host in _get_hosts_from_all_dc(DC_OSDS):
        mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'add-bucket', host, STRETCH_SUB_BUCKET_TYPE)

    # Move hosts to data centers
    for dc, dc_hosts in DC_OSDS.items():
        for host, _ in dc_hosts.items():
            mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'move', host, f'{STRETCH_BUCKET_TYPE}={dc}')

    # Move OSDs to the appropriate hosts
    for dc, dc_hosts in DC_OSDS.items():
        for host, osds in dc_hosts.items():
            for osd in osds:
                mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'move', f'osd.{str(osd)}', f'{STRETCH_SUB_BUCKET_TYPE}={host}')

    # Set the location of the monitors (including the tiebreaker monitor)
    for dc, dc_mons in DC_MONS.items():
        for host, mons in dc_mons.items():
            for mon in mons:
                mgr_cluster.mon_manager.raw_cluster_cmd('mon', 'set_location', mon, f'{STRETCH_BUCKET_TYPE}={dc}', f'{STRETCH_SUB_BUCKET_TYPE}={host}')

    # Remove the current host from the CRUSH map
    (client,) = ctx.cluster.only(CLIENT).remotes.keys()
    arg = ['hostname', '-s']
    hostname = client.run(args=arg, wait=True, stdout=StringIO(), timeout=30).stdout.getvalue()
    hostname = hostname.strip()
    mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'remove', hostname)

    # Modify and set the CRUSH map
    mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'getcrushmap', '-o', 'crushmap')
    arg = ['crushtool', '--decompile', 'crushmap', '-o', 'crushmap.txt']
    client.run(args=arg, wait=True, stdout=StringIO(), timeout=30)
    with open('crushmap.txt', 'r') as f:
        crushmap_content = f.read()
    crushmap_content = crushmap_content.replace('# end crush map', '')
    with open('crushmap_modified.txt', 'w') as f:
        f.write(crushmap_content)
        f.write(f'''
                rule {STRETCH_CRUSH_RULE_NAME} {{
                    id 2
                    type replicated
                    step take default
                    step choose firstn 2 type {STRETCH_BUCKET_TYPE}
                    step chooseleaf firstn 2 type {STRETCH_SUB_BUCKET_TYPE}
                    step emit
                }}
                # end crush map
                ''')
    arg = ['crushtool', '--compile', 'crushmap_modified.txt', '-o', 'crushmap.bin']
    client.run(args=arg, wait=True, stdout=StringIO(), timeout=30)
    mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'setcrushmap', '-i', 'crushmap.bin')
    return mgr_cluster.mon_manager.get_crush_rule_id(STRETCH_CRUSH_RULE_NAME)