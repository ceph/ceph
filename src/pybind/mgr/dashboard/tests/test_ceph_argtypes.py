import pytest
from ceph_argparse import CephSizeBytes

class TestCephArgtype:
    def test_sanity(self):
        

class TestConvertToBytes:
    def test_with_kb(self):
        assert CephSizeBytes._convert_to_bytes(f"100KB") == 102400
        assert CephSizeBytes._convert_to_bytes(f"100K") == 102400

    def test_with_mb(self):
        assert CephSizeBytes._convert_to_bytes(f"2MB") == 2 * 1024 ** 2
        assert CephSizeBytes._convert_to_bytes(f"2M") == 2 * 1024 ** 2

    def test_with_gb(self):
        assert CephSizeBytes._convert_to_bytes(f"1GB") == 1024 ** 3
        assert CephSizeBytes._convert_to_bytes(f"1G") == 1024 ** 3

    def test_with_tb(self):
        assert CephSizeBytes._convert_to_bytes(f"1TB") == 1024 ** 4
        assert CephSizeBytes._convert_to_bytes(f"1T") == 1024 ** 4

    def test_with_pb(self):
        assert CephSizeBytes._convert_to_bytes(f"1PB") == 1024 ** 5
        assert CephSizeBytes._convert_to_bytes(f"1P") == 1024 ** 5

    def test_with_integer(self):
        assert CephSizeBytes._convert_to_bytes(50, default_unit="B") == 50

    def test_invalid_unit(self):
        with pytest.raises(ValueError):
            CephSizeBytes._convert_to_bytes("50XYZ")

    def test_b(self):
        assert CephSizeBytes._convert_to_bytes(f"500B") == 500

    def test_with_large_number(self):
        assert CephSizeBytes._convert_to_bytes(f"1000GB") == 1000 * 1024 ** 3

    def test_no_number(self):
        with pytest.raises(ValueError):
            CephSizeBytes._convert_to_bytes("GB")

    def test_no_unit_with_default_unit_gb(self):
        assert CephSizeBytes._convert_to_bytes("500", default_unit="GB") == 500 * 1024 ** 3

    def test_no_unit_with_no_default_unit_raises(self):
        with pytest.raises(ValueError):
            CephSizeBytes._convert_to_bytes("500")

    def test_unit_in_input_overrides_default(self):
        assert CephSizeBytes._convert_to_bytes("50", default_unit="KB") == 50 * 1024
        assert CephSizeBytes._convert_to_bytes("50KB", default_unit="KB") == 50 * 1024
        assert CephSizeBytes._convert_to_bytes("50MB", default_unit="KB") == 50 * 1024 ** 2
