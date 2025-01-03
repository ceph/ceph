import pytest

import smb.validation


@pytest.mark.parametrize(
    "value,valid",
    [
        ("cat", True),
        ("m-o-u-s-e", True),
        ("foo-", False),
        ("-bar", False),
        ("not-too-bad-1", True),
        ("this-one-is-simply-too-long", False),
        ("t1", True),
        ("x", True),
        ("", False),
    ],
)
def test_valid_id(value, valid):
    assert smb.validation.valid_id(value) == valid
    if valid:
        smb.validation.check_id(value)
    else:
        with pytest.raises(ValueError):
            smb.validation.check_id(value)


@pytest.mark.parametrize(
    "value,valid",
    [
        ("cat", True),
        ("m-o-u-s-e", True),
        ("this-one-is-simply-fine-not-toolong", True),
        ("t1", True),
        ("x", True),
        ("A Wonderful Share", True),
        ("A_Very_Nice_Share", True),
        (">>>!!!", False),
        ("", False),
        ("A %h Family", False),
        ("A" * 64, True),
        ("A" * 65, False),
    ],
)
def test_valid_share_name(value, valid):
    assert smb.validation.valid_share_name(value) == valid
    if valid:
        smb.validation.check_share_name(value)
    else:
        with pytest.raises(ValueError):
            smb.validation.check_share_name(value)


@pytest.mark.parametrize(
    "value,valid",
    [
        ("cat", True),
        ("animals/cat", True),
        ("animals/cat", True),
        ("./animals/cat", True),
        ("/animals/cat", True),
        ("/", True),
        ("/animals/../cat", True),  # weird, but OK
        ("../cat", False),
        ("", False),
        (".", False),
        ("..", False),
    ],
)
def test_valid_path(value, valid):
    assert smb.validation.valid_path(value) == valid
    if valid:
        smb.validation.check_path(value)
    else:
        with pytest.raises(ValueError):
            smb.validation.check_path(value)


def _ovr(value):
    value[
        smb.validation.CUSTOM_CAUTION_KEY
    ] = smb.validation.CUSTOM_CAUTION_VALUE
    return value


@pytest.mark.parametrize(
    "value,errmatch",
    [
        ({"foo": "bar"}, "lack"),
        (_ovr({"foo": "bar"}), ""),
        (_ovr({"foo": "bar", "zip": "zap"}), ""),
        (_ovr({"mod:foo": "bar", "zip": "zap"}), ""),
        (_ovr({"foo\n": "bar"}), "newlines"),
        (_ovr({"foo": "bar\n"}), "newlines"),
        (_ovr({"[foo]": "bar\n"}), "brackets"),
    ],
)
def test_check_custom_options(value, errmatch):
    if not errmatch:
        smb.validation.check_custom_options(value)
    else:
        with pytest.raises(ValueError, match=errmatch):
            smb.validation.check_custom_options(value)


def test_clean_custom_options():
    orig = {'foo': 'bar', 'big': 'bad', 'bugs': 'bongo'}
    updated = _ovr(dict(orig))
    smb.validation.check_custom_options(updated)
    assert smb.validation.clean_custom_options(updated) == orig
    assert smb.validation.clean_custom_options(None) is None


@pytest.mark.parametrize(
    "value,ok,err_match",
    [
        ("tim", True, ""),
        ("britons\\arthur", True, ""),
        ("lance a lot", False, "spaces, tabs, or newlines"),
        ("tabs\ta\tlot", False, "spaces, tabs, or newlines"),
        ("bed\nivere", False, "spaces, tabs, or newlines"),
        ("runawa" + ("y" * 122), True, ""),
        ("runawa" + ("y" * 123), False, "128"),
    ],
)
def test_check_access_name(value, ok, err_match):
    if ok:
        smb.validation.check_access_name(value)
    else:
        with pytest.raises(ValueError, match=err_match):
            smb.validation.check_access_name(value)
