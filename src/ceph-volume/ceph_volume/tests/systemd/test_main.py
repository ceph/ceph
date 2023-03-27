import pytest
from ceph_volume import exceptions, conf
from ceph_volume.systemd.main import parse_subcommand, main, process


class TestParseSubcommand(object):

    def test_no_subcommand_found(self):
        with pytest.raises(exceptions.SuffixParsingError):
            parse_subcommand('')

    def test_sub_command_is_found(self):
        result = parse_subcommand('lvm-1-sha-1-something-0')
        assert result == 'lvm'


class Capture(object):

    def __init__(self, *a, **kw):
        self.a = a
        self.kw = kw
        self.calls = []

    def __call__(self, *a, **kw):
        self.calls.append(a)
        self.calls.append(kw)


class TestMain(object):

    def setup_method(self):
        conf.log_path = '/tmp/'

    def test_no_arguments_parsing_error(self):
        with pytest.raises(RuntimeError):
            main(args=[])

    def test_parsing_suffix_error(self):
        with pytest.raises(exceptions.SuffixParsingError):
            main(args=['asdf'])

    def test_correct_command(self, monkeypatch):
        run = Capture()
        monkeypatch.setattr(process, 'run', run)
        main(args=['ceph-volume-systemd', 'lvm-8715BEB4-15C5-49DE-BA6F-401086EC7B41-0' ])
        command = run.calls[0][0]
        assert command == [
            'ceph-volume',
            'lvm', 'trigger',
            '8715BEB4-15C5-49DE-BA6F-401086EC7B41-0'
        ]
