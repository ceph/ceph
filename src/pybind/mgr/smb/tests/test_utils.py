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
