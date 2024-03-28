import datetime
import mgr_util

import pytest


@pytest.mark.parametrize(
    "delta, out",
    [
        (datetime.timedelta(minutes=90), '90m'),
        (datetime.timedelta(minutes=190), '3h'),
        (datetime.timedelta(days=3), '3d'),
        (datetime.timedelta(hours=3), '3h'),
        (datetime.timedelta(days=365 * 3.1), '3y'),
        (datetime.timedelta(minutes=90), '90m'),
    ]
)
def test_pretty_timedelta(delta: datetime.timedelta, out: str):
    assert mgr_util.to_pretty_timedelta(delta) == out
