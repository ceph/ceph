from teuthology import lock


class TestLock(object):

    def test_locked_since_seconds(self):
        node = { "locked_since": "2013-02-07 19:33:55.000000" }
        assert lock.locked_since_seconds(node) > 3600
