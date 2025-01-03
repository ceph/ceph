import pytest

import smb.utils


def test_one():
    assert smb.utils.one(['a']) == 'a'
    with pytest.raises(ValueError):
        smb.utils.one([])
    with pytest.raises(ValueError):
        smb.utils.one(['a', 'b'])


def test_rand_name():
    name = smb.utils.rand_name('bob')
    assert name.startswith('bob')
    assert len(name) == 11
    name = smb.utils.rand_name('carla')
    assert name.startswith('carla')
    assert len(name) == 13
    name = smb.utils.rand_name('dangeresque')
    assert name.startswith('dangeresqu')
    assert len(name) == 18
    name = smb.utils.rand_name('fhqwhgadsfhqwhgadsfhqwhgads')
    assert name.startswith('fhqwhgadsf')
    assert len(name) == 18
    name = smb.utils.rand_name('')
    assert len(name) == 8


def test_checked():
    assert smb.utils.checked('foo') == 'foo'
    assert smb.utils.checked(77) == 77
    assert smb.utils.checked(0) == 0
    with pytest.raises(smb.utils.IsNoneError):
        smb.utils.checked(None)


def test_ynbool():
    assert smb.utils.ynbool(True) == 'Yes'
    assert smb.utils.ynbool(False) == 'No'
    # for giggles
    assert smb.utils.ynbool(0) == 'No'
