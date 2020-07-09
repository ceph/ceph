import pytest
from .fixtures import osd_support_module as osdsf
from tests import mock


class TestOSDSupport:

    def test_init(self, osdsf):
        assert osdsf.osd_ids == set()
        assert osdsf.emptying_osds == set()
        assert osdsf.check_osds == set()
        assert osdsf.empty == set()

    def test_osds_not_in_cluster(self, osdsf):
        assert osdsf.osds_not_in_cluster([1, 2]) == {1, 2}

    @mock.patch("osd_support.module.OSDSupport.get_osds_in_cluster")
    def test_osds_in_cluster(self, osds_in_cluster, osdsf):
        osds_in_cluster.return_value = [1]
        assert osdsf.osds_not_in_cluster([1, 2]) == {2}

    @pytest.mark.parametrize(
        "is_empty, osd_ids, expected",
        [
            (False, {1, 2}, []),
            (True, {1, 2}, [1, 2]),
            (None, {1, 2}, []),
        ]
    )
    def test_empty_osd(self, osdsf, is_empty, osd_ids, expected):
        with mock.patch("osd_support.module.OSDSupport.is_empty", return_value=is_empty):
            assert osdsf.empty_osds(osd_ids) == expected

    @pytest.mark.parametrize(
        "pg_count, expected",
        [
            (0, True),
            (1, False),
            (-1, False),
            (9999999999999, False),
        ]
    )
    def test_is_emtpy(self, pg_count, expected, osdsf):
        with mock.patch("osd_support.module.OSDSupport.get_pg_count", return_value=pg_count):
            assert osdsf.is_empty(1) == expected

    @pytest.mark.parametrize(
        "osd_ids, reweight_out, expected",
        [
            ({1}, [False], False),
            ({1}, [True], True),
            ({1, 2}, [True, True], True),
            ({1, 2}, [True, False], False),
        ]
    )
    def test_reweight_osds(self, osdsf, osd_ids, reweight_out, expected):
        with mock.patch("osd_support.module.OSDSupport.reweight_osd", side_effect=reweight_out):
            assert osdsf.reweight_osds(osd_ids) == expected

    @pytest.mark.parametrize(
        "osd_id, osd_df, expected",
        [
            # missing 'nodes' key
            (1, dict(nodes=[]), -1),
            # missing 'pgs' key
            (1, dict(nodes=[dict(id=1)]), -1),
            # id != osd_id
            (1, dict(nodes=[dict(id=999, pgs=1)]), -1),
            # valid
            (1, dict(nodes=[dict(id=1, pgs=1)]), 1),
        ]
    )
    def test_get_pg_count(self, osdsf, osd_id, osd_df, expected):
        with mock.patch("osd_support.module.OSDSupport.osd_df", return_value=osd_df):
            assert osdsf.get_pg_count(osd_id) == expected

    @pytest.mark.parametrize(
        "osd_id, osd_df, expected",
        [
            # missing 'nodes' key
            (1, dict(nodes=[]), -1.0),
            # missing 'crush_weight' key
            (1, dict(nodes=[dict(id=1)]), -1.0),
            # id != osd_id
            (1, dict(nodes=[dict(id=999, crush_weight=1)]), -1.0),
            # valid
            (1, dict(nodes=[dict(id=1, crush_weight=1)]), float(1)),
        ]
    )
    def test_get_osd_weight(self, osdsf, osd_id, osd_df, expected):
        with mock.patch("osd_support.module.OSDSupport.osd_df", return_value=osd_df):
            assert osdsf.get_osd_weight(osd_id) == expected

    @pytest.mark.parametrize(
        "osd_id, initial_osd_weight, mon_cmd_return, weight, expected",
        [
            # is already weighted correctly
            (1, 1.0, (0, '', ''), 1.0, True),
            # needs reweight, no errors in mon_cmd
            (1, 2.0, (0, '', ''), 1.0, True),
            # needs reweight, errors in mon_cmd
            (1, 2.0, (1, '', ''), 1.0, False),
        ]
    )
    def test_reweight_osd(self, osdsf, osd_id, initial_osd_weight, mon_cmd_return, weight, expected):
        with mock.patch("osd_support.module.OSDSupport.get_osd_weight", return_value=initial_osd_weight),\
             mock.patch("osd_support.module.OSDSupport.mon_command", return_value=mon_cmd_return):
            assert osdsf.reweight_osd(osd_id, weight=weight) == expected

    @pytest.mark.parametrize(
        "osd_ids, ok_to_stop, expected",
        [
            # no osd_ids provided
            ({}, [], set()),
            # all osds are ok_to_stop
            ({1, 2}, [True], {1, 2}),
            # osds are ok_to_stop after the second iteration
            ({1, 2}, [False, True], {2}),
            # osds are never ok_to_stop, (taking the sample size `(len(osd_ids))` into account),
            # expected to get a empty set()
            ({1, 2}, [False, False], set()),
        ]
    )
    def test_find_stop_threshold(self, osdsf, osd_ids, ok_to_stop, expected):
        with mock.patch("osd_support.module.OSDSupport.ok_to_stop", side_effect=ok_to_stop):
            assert osdsf.find_osd_stop_threshold(osd_ids) == expected

    @pytest.mark.parametrize(
        "osd_ids, mon_cmd_return, expected",
        [
            # ret is 0
            ([1], (0, '', ''), True),
            # no input yields True
            ([], (0, '', ''), True),
            # ret is != 0
            ([1], (-1, '', ''), False),
            # no input, but ret != 0
            ([], (-1, '', ''), False),
        ]
    )
    def test_ok_to_stop(self, osdsf, osd_ids, mon_cmd_return, expected):
        with mock.patch("osd_support.module.OSDSupport.mon_command", return_value=mon_cmd_return):
            assert osdsf.ok_to_stop(osd_ids) == expected

