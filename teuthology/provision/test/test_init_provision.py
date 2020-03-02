from copy import deepcopy
from pytest import raises
from teuthology.config import config

import teuthology.provision

test_config = dict(
    pelagos=dict(
        endpoint='http://pelagos.example:5000/',
        machine_types='ptype1,ptype2,common_type',
    ),
    fog=dict(
        endpoint='http://fog.example.com/fog',
        api_token='API_TOKEN',
        user_token='USER_TOKEN',
        machine_types='ftype1,ftype2,common_type',
    )
)

class TestInitProvision(object):

    def setup(self):
        config.load(deepcopy(test_config))

    def test_get_reimage_types(self):
        reimage_types = teuthology.provision.get_reimage_types()
        assert reimage_types == ["ptype1", "ptype2", "common_type",
                                 "ftype1", "ftype2", "common_type"]

    def test_reimage(self):
        class context:
            pass
        ctx = context()
        ctx.os_type = 'sle'
        ctx.os_version = '15.1'
        with raises(Exception) as e_info:
            teuthology.provision.reimage(ctx, 'f.q.d.n.org', 'not-defined-type')
        e_str = str(e_info)
        print("Caught exception: " +  e_str)
        assert e_str.find("configured\sprovisioners") == -1

        with raises(Exception) as e_info:
            teuthology.provision.reimage(ctx, 'f.q.d.n.org', 'common_type')
        e_str = str(e_info)
        print("Caught exception: " +  e_str)
        assert e_str.find("used\swith\sone\sprovisioner\sonly") == -1
