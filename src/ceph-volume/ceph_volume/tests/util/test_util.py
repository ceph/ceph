import pytest
from ceph_volume import util


class TestStrToInt(object):

    def test_passing_a_float_str(self):
        result = util.str_to_int("1.99")
        assert result == 1

    def test_passing_a_float_does_not_round(self):
        result = util.str_to_int("1.99", round_down=False)
        assert result == 2

    def test_text_is_not_an_integer_like(self):
        with pytest.raises(RuntimeError) as error:
            util.str_to_int("1.4GB")
        assert str(error.value) == "Unable to convert to integer: '1.4GB'"
