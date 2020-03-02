from copy import deepcopy
from pytest import raises
from teuthology.config import config
from teuthology.provision import pelagos

import teuthology.provision


test_config = dict(
    pelagos=dict(
        endpoint='http://pelagos.example:5000/',
        machine_types='ptype1,ptype2',
    ),
)

class TestPelagos(object):

    def setup(self):
        config.load(deepcopy(test_config))

    def teardown(self):
        pass

    def test_get_types(self):
        #klass = pelagos.Pelagos
        types = pelagos.get_types()
        assert types == ["ptype1", "ptype2"]

    def test_disabled(self):
        config.pelagos['endpoint'] = None
        enabled = pelagos.enabled()
        assert enabled == False

    def test_pelagos(self):
            class context:
                pass

            ctx = context()
            ctx.os_type ='sle'
            ctx.os_version = '15.1'
            with raises(Exception) as e_info:
                teuthology.provision.reimage(ctx, 'f.q.d.n.org', 'ptype1')
            e_str = str(e_info)
            print("Caught exception: " +  e_str)
            assert e_str.find("Name\sor\sservice\snot\sknown") == -1

