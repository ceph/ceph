import enum
import pytest
from ceph_argparse import CephSizeBytes, CephArgtype

class TestCephArgtypeArgdesc:

    def test_to_argdesc_with_default(self):
        attrs = {}
        result = CephArgtype.to_argdesc(str, attrs, has_default=True)
        assert result == "req=false,type=CephString"

    def test_to_argdesc_without_default(self):
        attrs = {}
        result = CephArgtype.to_argdesc(str, attrs, has_default=False)
        assert result == "type=CephString"

    def test_to_argdesc_positional_false(self):
        attrs = {}
        result = CephArgtype.to_argdesc(str, attrs, positional=False)
        assert result == "positional=false,type=CephString"

    def test_to_argdesc_str(self):
        attrs = {}
        result = CephArgtype.to_argdesc(str, attrs)
        assert result == "type=CephString"

    def test_to_argdesc_int(self):
        attrs = {}
        result = CephArgtype.to_argdesc(int, attrs)
        assert result == "type=CephInt"

    def test_to_argdesc_float(self):
        attrs = {}
        result = CephArgtype.to_argdesc(float, attrs)
        assert result == "type=CephFloat"

    def test_to_argdesc_bool(self):
        attrs = {}
        result = CephArgtype.to_argdesc(bool, attrs)
        assert result == "type=CephBool"

    def test_to_argdesc_invalid_type(self):
        attrs = {}
        # Simulate an invalid type that isn't in CEPH_ARG_TYPES
        with pytest.raises(ValueError):
            CephArgtype.to_argdesc(object, attrs)

    def test_to_argdesc_with_enum(self):
        import enum
        class MyEnum(enum.Enum):
            A = "one"
            B = "two"

        attrs = {}
        result = CephArgtype.to_argdesc(MyEnum, attrs)
        assert result == "strings=one|two,type=CephChoices"

        
class TestCephArgtypeCastTo:
    def test_cast_to_with_str(self):
        result = CephArgtype.cast_to(str, 123)
        assert result == "123"

    def test_cast_to_with_int(self):
        result = CephArgtype.cast_to(int, "123")
        assert result == 123

    def test_cast_to_with_float(self):
        result = CephArgtype.cast_to(float, "123.45")
        assert result == 123.45

    def test_cast_to_with_bool(self):
        result = CephArgtype.cast_to(bool, "True")
        assert result is True

    def test_cast_to_with_enum(self):
        class MyEnum(enum.Enum):
            A = "one"
            B = "two"

        result = CephArgtype.cast_to(MyEnum, "one")
        assert result == MyEnum.A

    def test_cast_to_with_unknown_type(self):
        class UnknownType:
            pass
        
        with pytest.raises(ValueError):
            CephArgtype.cast_to(UnknownType, "value")

    def test_cast_to_invalid_value_for_type(self):
        with pytest.raises(ValueError):
            CephArgtype.cast_to(int, "invalid_integer")


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
