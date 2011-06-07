from nose.tools import eq_ as eq

from .. import safepath

def test_simple():
    got = safepath.munge('foo')
    eq(got, 'foo')

def test_empty():
    # really odd corner case
    got = safepath.munge('')
    eq(got, '_')

def test_slash():
    got = safepath.munge('/')
    eq(got, '_')

def test_slashslash():
    got = safepath.munge('//')
    eq(got, '_')

def test_absolute():
    got = safepath.munge('/evil')
    eq(got, 'evil')

def test_absolute_subdir():
    got = safepath.munge('/evil/here')
    eq(got, 'evil/here')

def test_dot_leading():
    got = safepath.munge('./foo')
    eq(got, 'foo')

def test_dot_middle():
    got = safepath.munge('evil/./foo')
    eq(got, 'evil/foo')

def test_dot_trailing():
    got = safepath.munge('evil/foo/.')
    eq(got, 'evil/foo')

def test_dotdot():
    got = safepath.munge('../evil/foo')
    eq(got, '_./evil/foo')

def test_dotdot_subdir():
    got = safepath.munge('evil/../foo')
    eq(got, 'evil/_./foo')

def test_hidden():
    got = safepath.munge('.evil')
    eq(got, '_evil')

def test_hidden_subdir():
    got = safepath.munge('foo/.evil')
    eq(got, 'foo/_evil')
