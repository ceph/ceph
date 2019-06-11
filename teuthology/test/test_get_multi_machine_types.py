from teuthology import misc as teuthology

class Mock: pass

class TestGetMultiMachineTypes(object):

    def test_space(self):
        give = 'burnupi plana vps'
        expect = ['burnupi','plana','vps']
        assert teuthology.get_multi_machine_types(give) == expect

    def test_tab(self):
        give = 'burnupi	plana	vps'
        expect = ['burnupi','plana','vps']
        assert teuthology.get_multi_machine_types(give) == expect

    def test_comma(self):
        give = 'burnupi,plana,vps'
        expect = ['burnupi','plana','vps']
        assert teuthology.get_multi_machine_types(give) == expect

    def test_single(self):
        give = 'burnupi'
        expect = ['burnupi']
        assert teuthology.get_multi_machine_types(give) == expect


