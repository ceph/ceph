from datetime import datetime, timedelta
from unittest.mock import MagicMock
import pytest
from ...fs.schedule_client import get_prune_set, SNAPSHOT_TS_FORMAT


class TestScheduleClient(object):

    def test_get_prune_set_empty_retention_no_prune(self):
        now = datetime.now()
        candidates = set()
        for i in range(10):
            ts = now - timedelta(minutes=i*5)
            fake_dir = MagicMock()
            fake_dir.d_name = f'scheduled-{ts.strftime(SNAPSHOT_TS_FORMAT)}'
            candidates.add((fake_dir, ts))
        ret = {}
        prune_set = get_prune_set(candidates, ret)
        assert prune_set == set(), 'candidates are pruned despite empty retention'

