import pytest
from ceph_volume import terminal


class SubCommand(object):

    help = "this is the subcommand help"

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        pass


class BadSubCommand(object):

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        raise SystemExit(100)


class TestSubhelp(object):

    def test_no_sub_command_help(self):
        assert terminal.subhelp({}) == ''

    def test_single_level_help(self):
        result = terminal.subhelp({'sub': SubCommand})

        assert 'this is the subcommand help' in result

    def test_has_title_header(self):
        result = terminal.subhelp({'sub': SubCommand})
        assert 'Available subcommands:' in result

    def test_command_with_no_help(self):
        class SubCommandNoHelp(object):
            pass
        result = terminal.subhelp({'sub': SubCommandNoHelp})
        assert result == ''


class TestDispatch(object):

    def test_no_subcommand_found(self):
        result = terminal.dispatch({'sub': SubCommand}, argv=[])
        assert result is None

    def test_no_main_found(self):
        class NoMain(object):

            def __init__(self, argv):
                pass
        result = terminal.dispatch({'sub': NoMain}, argv=['sub'])
        assert result is None

    def test_subcommand_found_and_dispatched(self):
        with pytest.raises(SystemExit) as error:
            terminal.dispatch({'sub': SubCommand}, argv=['sub'])
        assert str(error.value) == '0'

    def test_subcommand_found_and_dispatched_with_errors(self):
        with pytest.raises(SystemExit) as error:
            terminal.dispatch({'sub': BadSubCommand}, argv=['sub'])
        assert str(error.value) == '100'
