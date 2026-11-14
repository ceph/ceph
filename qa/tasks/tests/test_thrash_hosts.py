"""
Unit tests for host thrashing in ceph_manager.OSDThrasher

Tests can be run by invoking pytest from the qa directory:

cd ceph/qa
python3 -m venv tasks/tests/venv
source tasks/tests/venv/bin/activate
pip install -r tasks/tests/requirements.txt
pytest tasks/tests/test_thrash_hosts.py
deactivate (when done)

"""
import random
from unittest.mock import MagicMock
from tasks.ceph_manager import OSDThrasher

NUM_HOSTS = 4
OSDS_PER_HOST = 4
HOSTS = {
    f'host{h}': [f'osd.{h + NUM_HOSTS * i}' for i in range(OSDS_PER_HOST)]
    for h in range(NUM_HOSTS)
}
IN_OUT_ACTIONS = ('out_host', 'in_host', 'in_osd')


def make_thrasher(min_in):
    """
    Return an OSDThrasher with all OSDs in and only the in/out actions
    enabled, without starting the thrash threads.
    """
    thrasher = OSDThrasher.__new__(OSDThrasher)
    manager = MagicMock()
    manager.ctx.cluster.only.return_value.remotes = HOSTS
    thrasher.ceph_manager = manager
    thrasher.logger = MagicMock()
    thrasher.config = {
        'thrash_hosts': True,
        'min_in': min_in,
        'chance_down': 0,
        'thrash_primary_affinity': False,
        'reweight_osd': 0,
        'chance_inject_pause_short': 0,
    }
    thrasher.minin = min_in
    thrasher.min_in_hosts = 2
    thrasher.thrash_hosts = True
    thrasher.in_osds = list(range(NUM_HOSTS * OSDS_PER_HOST))
    thrasher.live_osds = list(thrasher.in_osds)
    thrasher.out_osds = []
    thrasher.dead_osds = []
    for chance in ['chance_thrash_cluster_full', 'chance_thrash_pg_upmap',
                   'chance_thrash_pg_upmap_items', 'chance_force_recovery',
                   'chance_reset_purged_snaps_last',
                   'chance_trim_stale_osdmaps']:
        setattr(thrasher, chance, 0)
    return thrasher


class TestThrashHosts:
    def test_hosts_stay_whole_and_enough_hosts_stay_in(self):
        """
        With a host failure domain, CRUSH often fails to map all replicas
        when the in OSDs are a few OSDs scattered over partly out hosts,
        and it cannot map them at all on a single host.
        """
        random.seed(1)
        thrasher = make_thrasher(min_in=2)
        for _ in range(2000):
            action = thrasher.choose_action()
            if getattr(action, '__name__', None) not in IN_OUT_ACTIONS:
                continue
            action()

            hosts_in = 0
            for host, roles in HOSTS.items():
                osds = [int(role.split('.')[1]) for role in roles]
                num_in = len([osd for osd in osds
                              if osd in thrasher.in_osds])
                assert num_in in (0, len(osds)), \
                    f'{host} is partly in: {thrasher.in_osds}'
                if num_in:
                    hosts_in += 1
            assert hosts_in >= 2, f'only {hosts_in} host(s) in'
