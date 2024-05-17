# -*- mode:python; tab-width:4; indent-tabs-mode:nil; coding:utf-8 -*-

import codecs
import io
try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO
import pytest
import sys
from ceph_volume import terminal
from ceph_volume.log import setup_console


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
            stderr = sys.stderr
            stream = io.TextIOWrapper(buffer,
                                      encoding=encoding,
                                      errors=stderr.errors,
                                      newline=stderr.newlines,
                                      line_buffering=stderr.line_buffering)
        else:
            stream = codecs.getwriter(encoding)(buffer)
            # StreamWriter does not have encoding attached to it, it will ask
            # the inner buffer for "encoding" attribute in this case
            stream.encoding = encoding
        return stream
    return make_stream


class TestWriteUnicode(object):

    def setup_method(self):
        self.octpus_and_squid_en = u'octpus and squid'
        self.octpus_and_squid_zh = u'章鱼和鱿鱼'
        self.message = self.octpus_and_squid_en + self.octpus_and_squid_zh
        setup_console()

    def test_stdout_writer(self, capsys):
        # should work with whatever stdout is
        terminal.stdout(self.message)
        _, err = capsys.readouterr()
        assert self.octpus_and_squid_en in err
        assert self.octpus_and_squid_zh in err

    @pytest.mark.parametrize('encoding', ['ascii', 'utf8'])
    def test_writer_log(self, stream, encoding, monkeypatch, caplog):
        writer = StringIO()
        terminal._Write(_writer=writer).raw(self.message)
        writer.flush()
        writer.seek(0)
        output = writer.readlines()[0]
        assert self.octpus_and_squid_en in output

    @pytest.mark.parametrize('encoding', ['utf8'])
    def test_writer(self, encoding, stream, monkeypatch, capsys, caplog):
        buffer = io.BytesIO()
        writer = stream(buffer, encoding)
        terminal._Write(_writer=writer).raw(self.message)
        writer.flush()
        writer.seek(0)
        val = buffer.getvalue()
        assert self.octpus_and_squid_en.encode(encoding) in val
