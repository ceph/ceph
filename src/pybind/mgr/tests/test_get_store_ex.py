import pytest

from mgr_module import CLICommandBase, MgrModule

ReaderCLICommand = CLICommandBase.make_registry_subtype('ReaderCLICommand')


class Reader(MgrModule):
    CLICommand = ReaderCLICommand


@pytest.fixture
def reader():
    return Reader('telemetry', None, None)


def test_returns_shared_value(reader):
    reader.mock_store_set('store_ex', 'dashboard/telemetry/metrics', '{"a": 1}')
    assert reader.get_store_ex('dashboard', 'telemetry/metrics') == '{"a": 1}'


def test_missing_key_returns_default(reader):
    assert reader.get_store_ex('dashboard', 'telemetry/missing') is None
    assert reader.get_store_ex('dashboard', 'telemetry/missing', 'x') == 'x'


def test_not_shared_raises_permission_error(reader):
    def denied(module, key):
        raise PermissionError(f"Module '{module}' does not share '{key}' with module 'telemetry'")
    reader._ceph_get_store_ex = denied
    with pytest.raises(PermissionError):
        reader.get_store_ex('dashboard', 'secret')


def test_unknown_module_raises_import_error(reader):
    def missing(module, key):
        raise ImportError(f"Module '{module}' not found")
    reader._ceph_get_store_ex = missing
    with pytest.raises(ImportError):
        reader.get_store_ex('nope', 'k')
