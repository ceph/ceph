from mgr_module import MgrModule, remote_export


class Base(MgrModule):
    @remote_export
    def inherited(self) -> None:
        pass

    def not_exported(self) -> None:
        pass


class Child(Base):
    @remote_export
    def own(self) -> None:
        pass

    @classmethod
    @remote_export
    def a_classmethod(cls) -> None:
        pass

    @property
    def boom(self) -> None:
        raise AssertionError('properties must not be evaluated')


BASE = ['can_run', 'self_test']


def test_lists_own_and_inherited_exports():
    assert Child._remote_exports() == sorted(BASE + ['a_classmethod', 'inherited', 'own'])


def test_base_class_does_not_see_subclass_exports():
    assert Base._remote_exports() == sorted(BASE + ['inherited'])


def test_plain_module_exports_base_methods():
    assert MgrModule._remote_exports() == BASE


class Override(Child):
    def inherited(self) -> None:
        pass


def test_override_stays_exported():
    assert 'inherited' in Override._remote_exports()

