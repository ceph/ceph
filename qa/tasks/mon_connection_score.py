from tasks.ceph_test_case import CephTestCase
import json
import logging
log = logging.getLogger(__name__)


class TestStretchClusterNew(CephTestCase):

    CLUSTER = "ceph"
    MONS = {
            "a": {
                "rank": 0,
                },
            "b": {
                "rank": 1,
            },
            "c": {
                "rank": 2,
            }
        }
    WRITE_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 6
    SUCCESS_HOLD_TIME = 10

    def setUp(self):
        """
        Set up the cluster for the test.
        """
        super(TestStretchClusterNew, self).setUp()

    def tearDown(self):
        """
        Clean up the cluter after the test.
        """
        super(TestStretchClusterNew, self).tearDown()

    def _check_connection_score(self):
        """
        Check the connection score of all the mons.
        """
        for mon, _ in self.MONS.items():
            # get the connection score
            cscore = self.ceph_cluster.mon_manager.raw_cluster_cmd(
                'daemon', 'mon.{}'.format(mon),
                'connection', 'scores', 'dump')
            # parse the connection score
            cscore = json.loads(cscore)
            # check if the current mon rank is correct
            if cscore["rank"] != self.MONS[mon]["rank"]:
                log.error(
                    "Rank mismatch {} != {}".format(
                        cscore["rank"], self.MONS[mon]["rank"]
                    )
                )
                return False
            # check if current mon have all the peer reports and ourself
            if len(cscore['reports']) != len(self.MONS):
                log.error(
                    "Reports count mismatch {}".format(cscore['reports'])
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
                    rank
                    for data in self.MONS.values()
                    for rank in data.values()
                ]
                if report_rank.sort() != expected_ranks.sort():
                    log.error("Rank mismatch in report {}".format(report))
                    return False

        log.info("Connection score is clean!")
        return True

    def test_connection_score(self):
        # check if all mons are in quorum
        self.ceph_cluster.mon_manager.wait_for_mon_quorum_size(3)
        # check if all connection scores reflect this
        self.wait_until_true_and_hold(
            lambda: self._check_connection_score(),
            # Wait for 4 minutes for the connection score to recover
            timeout=self.RECOVERY_PERIOD * 4,
            # Hold the clean connection score for 60 seconds
            success_hold_time=self.SUCCESS_HOLD_TIME * 6
        )
