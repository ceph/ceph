import json

from cephadm.services.osd import OSDRemovalQueue, OSD
import pytest
from tests import mock
from .fixtures import with_cephadm_module
from datetime import datetime


class MockOSD:

    def __init__(self, osd_id):
        self.osd_id = osd_id


class TestOSDRemoval:

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
    def test_get_pg_count(self, rm_util, osd_id, osd_df, expected):
        with mock.patch("cephadm.services.osd.RemoveUtil.osd_df", return_value=osd_df):
            assert rm_util.get_pg_count(osd_id) == expected

    @pytest.mark.parametrize(
        "osds, ok_to_stop, expected",
        [
            # no osd_ids provided
            ([], [False], []),
            # all osds are ok_to_stop
            ([1, 2], [True], [1, 2]),
            # osds are ok_to_stop after the second iteration
            ([1, 2], [False, True], [2]),
            # osds are never ok_to_stop, (taking the sample size `(len(osd_ids))` into account),
            # expected to get False
            ([1, 2], [False, False], []),
        ]
    )
    def test_find_stop_threshold(self, rm_util, osds, ok_to_stop, expected):
        with mock.patch("cephadm.services.osd.RemoveUtil.ok_to_stop", side_effect=ok_to_stop):
            assert rm_util.find_osd_stop_threshold(osds) == expected

    def test_process_removal_queue(self, rm_util):
        # TODO: !
        # rm_util.process_removal_queue()
        pass

    @pytest.mark.parametrize(
        "max_osd_draining_count, draining_osds, idling_osds, ok_to_stop, expected",
        [
            # drain one at a time, one already draining
            (1, [1], [1], [True], 0),
            # drain one at a time, none draining yet
            (1, [], [1, 2, 3], [True, True, True], 1),
            # drain one at a time, one already draining, none ok-to-stop
            (1, [1], [1], [False], 0),
            # drain one at a time, none draining, one ok-to-stop
            (1, [], [1, 2, 3], [False, False, True], 1),
            # drain three at a time, one already draining, all ok-to-stop
            (3, [1], [1, 2, 3], [True, True, True], 2),
            # drain two at a time, none already draining, none ok-to-stop
            (2, [], [1, 2, 3], [False, False, False], 0),
            # drain two at a time, none already draining, none idling
            (2, [], [], [], 0),
        ]
    )
    def test_ready_to_drain_osds(self, max_osd_draining_count, draining_osds, idling_osds, ok_to_stop, expected):
        with with_cephadm_module({'max_osd_draining_count': max_osd_draining_count}) as m:
            with mock.patch("cephadm.services.osd.OSDRemovalQueue.draining_osds", return_value=draining_osds):
                with mock.patch("cephadm.services.osd.OSDRemovalQueue.idling_osds", return_value=idling_osds):
                    with mock.patch("cephadm.services.osd.RemoveUtil.ok_to_stop", side_effect=ok_to_stop):
                        removal_queue = OSDRemovalQueue(m)
                        assert len(removal_queue._ready_to_drain_osds()) == expected

    def test_ok_to_stop(self, rm_util):
        rm_util.ok_to_stop([MockOSD(1)])
        rm_util._run_mon_cmd.assert_called_with({'prefix': 'osd ok-to-stop', 'ids': ['1']},
                                                error_ok=True)

    def test_safe_to_destroy(self, rm_util):
        rm_util.safe_to_destroy([1])
        rm_util._run_mon_cmd.assert_called_with({'prefix': 'osd safe-to-destroy',
                                                 'ids': ['1']}, error_ok=True)

    def test_destroy_osd(self, rm_util):
        rm_util.destroy_osd(1)
        rm_util._run_mon_cmd.assert_called_with(
            {'prefix': 'osd destroy-actual', 'id': 1, 'yes_i_really_mean_it': True})

    def test_purge_osd(self, rm_util):
        rm_util.purge_osd(1)
        rm_util._run_mon_cmd.assert_called_with(
            {'prefix': 'osd purge-actual', 'id': 1, 'yes_i_really_mean_it': True})

    def test_load(self, cephadm_module, rm_util):
        data = json.dumps([
            {
                "osd_id": 35,
                "started": True,
                "draining": True,
                "stopped": False,
                "replace": False,
                "force": False,
                "zap": False,
                "nodename": "node2",
                "drain_started_at": "2020-09-14T11:41:53.960463",
                "drain_stopped_at": None,
                "drain_done_at": None,
                "process_started_at": "2020-09-14T11:41:52.245832"
            }
        ])
        cephadm_module.set_store('osd_remove_queue', data)
        cephadm_module.to_remove_osds.load_from_store()

        expected = OSDRemovalQueue(cephadm_module)
        expected.osds.add(OSD(osd_id=35, remove_util=rm_util, draining=True))
        assert cephadm_module.to_remove_osds == expected


class TestOSD:

    def test_start(self, osd_obj):
        assert osd_obj.started is False
        osd_obj.start()
        assert osd_obj.started is True
        assert osd_obj.stopped is False

    def test_start_draining_purge(self, osd_obj):
        assert osd_obj.draining is False
        assert osd_obj.drain_started_at is None
        ret = osd_obj.start_draining()
        osd_obj.rm_util.reweight_osd.assert_called_with(osd_obj, 0.0)
        assert isinstance(osd_obj.drain_started_at, datetime)
        assert osd_obj.draining is True
        assert osd_obj.replace is False
        assert ret is True

    def test_start_draining_replace(self, osd_obj):
        assert osd_obj.draining is False
        assert osd_obj.drain_started_at is None
        osd_obj.replace = True
        ret = osd_obj.start_draining()
        osd_obj.rm_util.set_osd_flag.assert_called_with([osd_obj], 'out')
        assert isinstance(osd_obj.drain_started_at, datetime)
        assert osd_obj.draining is True
        assert osd_obj.replace is True
        assert ret is True

    def test_start_draining_stopped(self, osd_obj):
        osd_obj.stopped = True
        ret = osd_obj.start_draining()
        assert osd_obj.drain_started_at is None
        assert ret is False
        assert osd_obj.draining is False

    def test_stop_draining_replace(self, osd_obj):
        osd_obj.replace = True
        ret = osd_obj.stop_draining()
        osd_obj.rm_util.set_osd_flag.assert_called_with([osd_obj], 'in')
        assert isinstance(osd_obj.drain_stopped_at, datetime)
        assert osd_obj.draining is False
        assert ret is True

    def test_stop_draining_purge(self, osd_obj):
        osd_obj.original_weight = 1.0
        ret = osd_obj.stop_draining()
        osd_obj.rm_util.reweight_osd.assert_called_with(osd_obj, 1.0)
        assert isinstance(osd_obj.drain_stopped_at, datetime)
        assert osd_obj.draining is False
        assert ret is True

    @mock.patch('cephadm.services.osd.OSD.stop_draining')
    def test_stop(self, stop_draining_mock, osd_obj):
        osd_obj.stop()
        assert osd_obj.started is False
        assert osd_obj.stopped is True
        stop_draining_mock.assert_called_once()

    @pytest.mark.parametrize(
        "draining, empty, expected",
        [
            # must be !draining! and !not empty! to yield True
            (True, not True, True),
            # not draining and not empty
            (False, not True, False),
            # not draining and empty
            (False, True, False),
            # draining and empty
            (True, True, False),
        ]
    )
    def test_is_draining(self, osd_obj, draining, empty, expected):
        with mock.patch("cephadm.services.osd.OSD.is_empty", new_callable=mock.PropertyMock(return_value=empty)):
            osd_obj.draining = draining
            assert osd_obj.is_draining is expected

    @mock.patch("cephadm.services.osd.RemoveUtil.ok_to_stop")
    def test_is_ok_to_stop(self, _, osd_obj):
        osd_obj.is_ok_to_stop
        osd_obj.rm_util.ok_to_stop.assert_called_once()

    @pytest.mark.parametrize(
        "pg_count, expected",
        [
            (0, True),
            (1, False),
            (9999, False),
            (-1, False),
        ]
    )
    def test_is_empty(self, osd_obj, pg_count, expected):
        with mock.patch("cephadm.services.osd.OSD.get_pg_count", return_value=pg_count):
            assert osd_obj.is_empty is expected

    @mock.patch("cephadm.services.osd.RemoveUtil.safe_to_destroy")
    def test_safe_to_destroy(self, _, osd_obj):
        osd_obj.safe_to_destroy()
        osd_obj.rm_util.safe_to_destroy.assert_called_once()

    @mock.patch("cephadm.services.osd.RemoveUtil.set_osd_flag")
    def test_down(self, _, osd_obj):
        osd_obj.down()
        osd_obj.rm_util.set_osd_flag.assert_called_with([osd_obj], 'down')

    @mock.patch("cephadm.services.osd.RemoveUtil.destroy_osd")
    def test_destroy_osd(self, _, osd_obj):
        osd_obj.destroy()
        osd_obj.rm_util.destroy_osd.assert_called_once()

    @mock.patch("cephadm.services.osd.RemoveUtil.purge_osd")
    def test_purge(self, _, osd_obj):
        osd_obj.purge()
        osd_obj.rm_util.purge_osd.assert_called_once()

    @mock.patch("cephadm.services.osd.RemoveUtil.get_pg_count")
    def test_pg_count(self, _, osd_obj):
        osd_obj.get_pg_count()
        osd_obj.rm_util.get_pg_count.assert_called_once()

    def test_drain_status_human_not_started(self, osd_obj):
        assert osd_obj.drain_status_human() == 'not started'

    def test_drain_status_human_started(self, osd_obj):
        osd_obj.started = True
        assert osd_obj.drain_status_human() == 'started'

    def test_drain_status_human_draining(self, osd_obj):
        osd_obj.started = True
        osd_obj.draining = True
        assert osd_obj.drain_status_human() == 'draining'

    def test_drain_status_human_done(self, osd_obj):
        osd_obj.started = True
        osd_obj.draining = False
        osd_obj.drain_done_at = datetime.utcnow()
        assert osd_obj.drain_status_human() == 'done, waiting for purge'


class TestOSDRemovalQueue:

    def test_queue_size(self, osd_obj):
        q = OSDRemovalQueue(mock.Mock())
        assert q.queue_size() == 0
        q.osds.add(osd_obj)
        assert q.queue_size() == 1

    @mock.patch("cephadm.services.osd.OSD.start")
    @mock.patch("cephadm.services.osd.OSD.exists")
    def test_enqueue(self, exist, start, osd_obj):
        q = OSDRemovalQueue(mock.Mock())
        q.enqueue(osd_obj)
        osd_obj.start.assert_called_once()

    @mock.patch("cephadm.services.osd.OSD.stop")
    @mock.patch("cephadm.services.osd.OSD.exists")
    def test_rm_raise(self, exist, stop, osd_obj):
        q = OSDRemovalQueue(mock.Mock())
        with pytest.raises(KeyError):
            q.rm(osd_obj)
            osd_obj.stop.assert_called_once()

    @mock.patch("cephadm.services.osd.OSD.stop")
    @mock.patch("cephadm.services.osd.OSD.exists")
    def test_rm(self, exist, stop, osd_obj):
        q = OSDRemovalQueue(mock.Mock())
        q.osds.add(osd_obj)
        q.rm(osd_obj)
        osd_obj.stop.assert_called_once()
