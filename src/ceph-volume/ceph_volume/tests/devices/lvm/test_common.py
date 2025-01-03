from ceph_volume.devices.lvm import common


class TestCommon(object):

    def test_get_default_args_smoke(self):
        default_args = common.get_default_args()
        assert default_args
