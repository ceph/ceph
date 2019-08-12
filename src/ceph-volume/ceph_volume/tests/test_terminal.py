# -*- mode:python; tab-width:4; indent-tabs-mode:nil; coding:utf-8 -*-

import codecs
import io
import pytest
import sys
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


@pytest.fixture
def stream():
    def make_stream(buffer, encoding):
        # mock a stdout with given encoding
        if sys.version_info >= (3, 0):
            stdout = sys.stdout
            stream = io.TextIOWrapper(buffer,
                                      encoding=encoding,
                                      errors=stdout.errors,
                                      newline=stdout.newlines,
                                      line_buffering=stdout.line_buffering)
        else:
            stream = codecs.getwriter(encoding)(buffer)
            # StreamWriter does not have encoding attached to it, it will ask
            # the inner buffer for "encoding" attribute in this case
            stream.encoding = encoding
        return stream
    return make_stream


class TestWriteUnicode(object):

    def setup(self):
        self.octpus_and_squid_en = u'octpus and squid'
        octpus_and_squid_zh = u'章鱼和鱿鱼'
        self.message = self.octpus_and_squid_en + octpus_and_squid_zh

    def test_stdout_writer(self, capsys):
        # should work with whatever stdout is
        terminal.stdout(self.message)
        out, _ = capsys.readouterr()
        assert self.octpus_and_squid_en in out

    @pytest.mark.parametrize('encoding', ['ascii', 'utf8'])
    def test_writer(self, encoding, stream, monkeypatch, capsys):
        buffer = io.BytesIO()
        # should keep writer alive
        with capsys.disabled():
            # we want to have access to the sys.stdout's attributes in
            # make_stream(), not the ones of pytest.capture.EncodedFile
            writer = stream(buffer, encoding)
            monkeypatch.setattr(sys, 'stdout', writer)
            terminal.stdout(self.message)
            sys.stdout.flush()
            assert self.octpus_and_squid_en.encode(encoding) in buffer.getvalue()
