import pytest
from ceph_volume import util


class TestAsBytes(object):

    def test_bytes_just_gets_returned(self):
        bytes_string = "contents".encode('utf-8')
        assert util.as_bytes(bytes_string) == bytes_string

    def test_string_gets_converted_to_bytes(self):
        result = util.as_bytes('contents')
        assert isinstance(result, bytes)


class TestStrToInt(object):

    def test_passing_a_float_str_comma(self):
        result = util.str_to_int("1,99")
        assert result == 1

    def test_passing_a_float_does_not_round_comma(self):
        result = util.str_to_int("1,99", round_down=False)
        assert result == 2

    @pytest.mark.parametrize("value", ['2', 2])
    def test_passing_an_int(self, value):
        result = util.str_to_int(value)
        assert result == 2

    @pytest.mark.parametrize("value", ['1.99', 1.99])
    def test_passing_a_float(self, value):
        result = util.str_to_int(value)
        assert result == 1

    @pytest.mark.parametrize("value", ['1.99', 1.99])
    def test_passing_a_float_does_not_round(self, value):
        result = util.str_to_int(value, round_down=False)
        assert result == 2

    def test_text_is_not_an_integer_like(self):
        with pytest.raises(RuntimeError) as error:
            util.str_to_int("1.4GB")
        assert str(error.value) == "Unable to convert to integer: '1.4GB'"

    def test_input_is_not_string(self):
        with pytest.raises(RuntimeError) as error:
            util.str_to_int(None)
        assert str(error.value) == "Unable to convert to integer: 'None'"


def true_responses(upper_casing=False):
    if upper_casing:
        return ['Y', 'YES', '']
    return ['y', 'yes', '']


def false_responses(upper_casing=False):
    if upper_casing:
        return ['N', 'NO']
    return ['n', 'no']


def invalid_responses():
    return [9, 0.1, 'h', [], {}, None]


class TestStrToBool(object):

    @pytest.mark.parametrize('response', true_responses())
    def test_trueish(self, response):
        assert util.str_to_bool(response) is True

    @pytest.mark.parametrize('response', false_responses())
    def test_falseish(self, response):
        assert util.str_to_bool(response) is False

    @pytest.mark.parametrize('response', true_responses(True))
    def test_trueish_upper(self, response):
        assert util.str_to_bool(response) is True

    @pytest.mark.parametrize('response', false_responses(True))
    def test_falseish_upper(self, response):
        assert util.str_to_bool(response) is False

    @pytest.mark.parametrize('response', invalid_responses())
    def test_invalid(self, response):
        with pytest.raises(ValueError):
            util.str_to_bool(response)


class TestPromptBool(object):

    @pytest.mark.parametrize('response', true_responses())
    def test_trueish(self, response):
        fake_input = lambda x: response
        qx = 'what the what?'
        assert util.prompt_bool(qx, input_=fake_input) is True

    @pytest.mark.parametrize('response', false_responses())
    def test_falseish(self, response):
        fake_input = lambda x: response
        qx = 'what the what?'
        assert util.prompt_bool(qx, input_=fake_input) is False

    def test_try_again_true(self):
        responses = ['g', 'h', 'y']
        fake_input = lambda x: responses.pop(0)
        qx = 'what the what?'
        assert util.prompt_bool(qx, input_=fake_input) is True

    def test_try_again_false(self):
        responses = ['g', 'h', 'n']
        fake_input = lambda x: responses.pop(0)
        qx = 'what the what?'
        assert util.prompt_bool(qx, input_=fake_input) is False
