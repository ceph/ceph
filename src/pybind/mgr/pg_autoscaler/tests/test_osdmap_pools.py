# python unit test
from tests import mock
from pg_autoscaler import module


class OSDMAP:
    def __init__(self, epoch, pools):
        self.epoch = epoch
        self.pools = pools

    def get_epoch(self):
        return self.epoch

    def dump(self):
        return {'epoch': self.epoch, 'pools': self.pools}


class TestOSDMapPools(object):

    def setup_method(self):
        self.autoscaler = module.PgAutoscaler('module_name', 0, 0)

    def test_same_epoch_uses_cached_dump(self):
        osdmap = OSDMAP(10, [{'pool_name': 'a', 'pool': 1}])
        cached = {'epoch': 10, 'pools': [{'pool_name': 'a', 'pool': 1}]}
        with mock.patch.object(self.autoscaler, 'get', return_value=cached), \
                mock.patch.object(osdmap, 'dump') as dump:
            pools = self.autoscaler.get_osdmap_pools_by_name(osdmap)
        assert list(pools) == ['a']
        dump.assert_not_called()

    def test_newer_cached_dump_uses_osdmap(self):
        # a pool created after get_osdmap() must not be returned, the
        # osdmap does not know it
        osdmap = OSDMAP(10, [{'pool_name': 'a', 'pool': 1}])
        cached = {'epoch': 11, 'pools': [{'pool_name': 'a', 'pool': 1},
                                         {'pool_name': 'b', 'pool': 2}]}
        with mock.patch.object(self.autoscaler, 'get', return_value=cached):
            pools = self.autoscaler.get_osdmap_pools_by_name(osdmap)
        assert list(pools) == ['a']
