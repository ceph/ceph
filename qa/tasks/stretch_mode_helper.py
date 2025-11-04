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
import json
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

def check_connection_score(mgr_cluster):
    """
    Check the connection score of all the mons.
    Election Strategy must be 3 "connectivity".
    """
    # election strategy must be "connectivity"
    election_strategy = mgr_cluster.mon_manager.get_mon_election_strategy()
    if str(election_strategy) != "3":
        log.error("Election strategy is not 'connectivity'")
        return False
    # get all the mons looks like this: [{'name': 'a', 'rank': 0}, ...]
    mons = mgr_cluster.mon_manager.get_all_mons_name_and_rank()
    for mon in mons:
        # get the connection score
        cscore = mgr_cluster.mon_manager.raw_cluster_cmd(
            'daemon', 'mon.{}'.format(mon['name']),
            'connection', 'scores', 'dump')
        # parse the connection score
        cscore = json.loads(cscore)
        # check if the current mon rank is correct
        if cscore["rank"] != mon["rank"]:
            log.error(
                "Rank mismatch {} != {}".format(
                    cscore["rank"], mon["rank"]
                )
            )
            return False
        # check if current mon have all the peer reports and ourself
        if len(cscore["reports"]) != len(mons):
            log.error(
                "Reports count mismatch {}".format(cscore["reports"])
            )
            return False

        for report in cscore["reports"]:
            report_rank = []
            for peer in report["peer_scores"]:
                # check if the peer is alive
                if not peer["peer_alive"]:
                    log.error("Peer {} is not alive".format(peer))
                    return False
                report_rank.append(peer["peer_rank"])

            # check if current mon has all the ranks and no duplicates
            expected_ranks = [
                mon["rank"]
                for mon in mons
            ]
            if report_rank.sort() != expected_ranks.sort():
                log.error("Rank mismatch in report {}".format(report))
                return False

    log.info("Connection score is clean!")
    return True

def setup_stretch_mode(
                        mgr_cluster,
                        ctx,
                        client,
                        stretch_bucket_type,
                        stretch_sub_bucket_type,
                        stretch_crush_rule_name,
                        dc_osds,
                        dc_mons,
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
    for dc in dc_osds:
        mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'add-bucket', dc, stretch_bucket_type)
        mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'move', dc, 'root=default')

    # Add and move CRUSH buckets for hosts
    for host in _get_hosts_from_all_dc(dc_osds):
        mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'add-bucket', host, stretch_sub_bucket_type)

    # Move hosts to data centers
    for dc, dc_hosts in dc_osds.items():
        for host, _ in dc_hosts.items():
            mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'move', host, f'{stretch_bucket_type}={dc}')

    # Move OSDs to the appropriate hosts
    for dc, dc_hosts in dc_osds.items():
        for host, osds in dc_hosts.items():
            for osd in osds:
                mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'crush', 'move', f'osd.{str(osd)}', f'{stretch_sub_bucket_type}={host}')

    # Set the location of the monitors (including the tiebreaker monitor)
    for dc, dc_mons in dc_mons.items():
        for host, mons in dc_mons.items():
            for mon in mons:
                mgr_cluster.mon_manager.raw_cluster_cmd('mon', 'set_location', mon, f'{stretch_bucket_type}={dc}', f'{stretch_sub_bucket_type}={host}')

    # Remove the current host from the CRUSH map
    (client,) = ctx.cluster.only(client).remotes.keys()
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
                rule {stretch_crush_rule_name} {{
                    id 2
                    type replicated
                    step take default
                    step choose firstn 2 type {stretch_bucket_type}
                    step chooseleaf firstn 2 type {stretch_sub_bucket_type}
                    step emit
                }}
                # end crush map
                ''')
    arg = ['crushtool', '--compile', 'crushmap_modified.txt', '-o', 'crushmap.bin']
    client.run(args=arg, wait=True, stdout=StringIO(), timeout=30)
    mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'setcrushmap', '-i', 'crushmap.bin')
    return mgr_cluster.mon_manager.get_crush_rule_id(stretch_crush_rule_name)