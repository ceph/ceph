from .. import safepath

class TestSafepath(object):
    def test_simple(self):
        got = safepath.munge('foo')
        assert got == 'foo'

    def test_empty(self):
        # really odd corner case
        got = safepath.munge('')
        assert got == '_'

    def test_slash(self):
        got = safepath.munge('/')
        assert got == '_'

    def test_slashslash(self):
        got = safepath.munge('//')
        assert got == '_'

    def test_absolute(self):
        got = safepath.munge('/evil')
        assert got == 'evil'

    def test_absolute_subdir(self):
        got = safepath.munge('/evil/here')
        assert got == 'evil/here'

    def test_dot_leading(self):
        got = safepath.munge('./foo')
        assert got == 'foo'

    def test_dot_middle(self):
        got = safepath.munge('evil/./foo')
        assert got == 'evil/foo'

    def test_dot_trailing(self):
        got = safepath.munge('evil/foo/.')
        assert got == 'evil/foo'

    def test_dotdot(self):
        got = safepath.munge('../evil/foo')
        assert got == '_./evil/foo'

    def test_dotdot_subdir(self):
        got = safepath.munge('evil/../foo')
        assert got == 'evil/_./foo'

    def test_hidden(self):
        got = safepath.munge('.evil')
        assert got == '_evil'

    def test_hidden_subdir(self):
        got = safepath.munge('foo/.evil')
        assert got == 'foo/_evil'
