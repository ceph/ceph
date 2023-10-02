import pytest
from ceph_volume import exceptions
from ceph_volume.devices.lvm import trigger


class TestParseOSDid(object):

    def test_no_id_found_if_no_digit(self):
        with pytest.raises(exceptions.SuffixParsingError):
            trigger.parse_osd_id('asdlj-ljahsdfaslkjhdfa')

    def test_no_id_found(self):
        with pytest.raises(exceptions.SuffixParsingError):
            trigger.parse_osd_id('ljahsdfaslkjhdfa')

    def test_id_found(self):
        result = trigger.parse_osd_id('1-ljahsdfaslkjhdfa')
        assert result == '1'


class TestParseOSDUUID(object):

    def test_uuid_is_parsed(self):
        result = trigger.parse_osd_uuid('1-asdf-ljkh-asdf-ljkh-asdf')
        assert result == 'asdf-ljkh-asdf-ljkh-asdf'

    def test_uuid_is_parsed_longer_sha1(self):
        result = trigger.parse_osd_uuid('1-foo-bar-asdf-ljkh-asdf-ljkh-asdf')
        assert result == 'foo-bar-asdf-ljkh-asdf-ljkh-asdf'

    def test_uuid_is_not_found(self):
        with pytest.raises(exceptions.SuffixParsingError):
            trigger.parse_osd_uuid('ljahsdfaslkjhdfa')

    def test_uuid_is_not_found_missing_id(self):
        with pytest.raises(exceptions.SuffixParsingError):
            trigger.parse_osd_uuid('ljahs-dfa-slkjhdfa-foo')

    def test_robust_double_id_in_uuid(self):
        # it is possible to have the id in the SHA1, this should
        # be fine parsing that
        result = trigger.parse_osd_uuid("1-abc959fd-1ec9-4864-b141-3154f9b9f8ed")
        assert result == 'abc959fd-1ec9-4864-b141-3154f9b9f8ed'


