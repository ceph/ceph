import pytest
from ceph_volume import exceptions, conf
from ceph_volume.systemd import main


class TestParseSubcommand(object):

    def test_no_subcommand_found(self):
        with pytest.raises(exceptions.SuffixParsingError):
            main.parse_subcommand('')

    def test_sub_command_is_found(self):
        result = main.parse_subcommand('0-1-sha-1-something-lvm')
        assert result == 'lvm'


class TestParseOSDid(object):

    def test_no_id_found_if_no_digit(self):
        with pytest.raises(exceptions.SuffixParsingError):
            main.parse_osd_id('asdlj-ljahsdfaslkjhdfa')

    def test_no_id_found(self):
        with pytest.raises(exceptions.SuffixParsingError):
            main.parse_osd_id('ljahsdfaslkjhdfa')

    def test_id_found(self):
        result = main.parse_osd_id('1-ljahsdfaslkjhdfa')
        assert result == '1'


class TestParseOSDUUID(object):

    def test_uuid_is_parsed(self):
        result = main.parse_osd_uuid('1-asdf-ljkh-asdf-ljkh-asdf-lvm')
        assert result == 'asdf-ljkh-asdf-ljkh-asdf'

    def test_uuid_is_parsed_longer_sha1(self):
        result = main.parse_osd_uuid('1-foo-bar-asdf-ljkh-asdf-ljkh-asdf-lvm')
        assert result == 'foo-bar-asdf-ljkh-asdf-ljkh-asdf'

    def test_uuid_is_not_found(self):
        with pytest.raises(exceptions.SuffixParsingError):
            main.parse_osd_uuid('ljahsdfaslkjhdfa')

    def test_uuid_is_not_found_missing_id(self):
        with pytest.raises(exceptions.SuffixParsingError):
            main.parse_osd_uuid('ljahs-dfa-slkjhdfa-lvm')


class Capture(object):

    def __init__(self, *a, **kw):
        self.a = a
        self.kw = kw
        self.calls = []

    def __call__(self, *a, **kw):
        self.calls.append(a)
        self.calls.append(kw)


class TestMain(object):

    def setup(self):
        conf.log_path = '/tmp/'

    def test_parsing_error(self):
        with pytest.raises(exceptions.SuffixParsingError):
            main.main(args=[])

    def test_correct_command(self, monkeypatch):
        run = Capture()
        monkeypatch.setattr(main.process, 'run', run)
        main.main(args=['ceph-volume-systemd', '0-8715BEB4-15C5-49DE-BA6F-401086EC7B41-lvm' ])
        command = run.calls[0][0]
        assert command == [
            'ceph-volume',
            'lvm', 'activate',
            '0', '8715BEB4-15C5-49DE-BA6F-401086EC7B41'
        ]
