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
