import os
import pytest
from ceph_volume.devices.simple import scan


class TestGetContents(object):

    def test_multiple_lines_are_left_as_is(self, tmpfile):
        magic_file = tmpfile(contents='first\nsecond\n')
        scanner = scan.Scan([])
        assert scanner.get_contents(magic_file) == 'first\nsecond\n'

    def test_extra_whitespace_gets_removed(self, tmpfile):
        magic_file = tmpfile(contents='first   ')
        scanner = scan.Scan([])
        assert scanner.get_contents(magic_file) == 'first'

    def test_single_newline_values_are_trimmed(self, tmpfile):
        magic_file = tmpfile(contents='first\n')
        scanner = scan.Scan([])
        assert scanner.get_contents(magic_file) == 'first'


class TestEtcPath(object):

    def test_directory_is_valid(self, tmpdir):
        path = str(tmpdir)
        scanner = scan.Scan([])
        scanner._etc_path = path
        assert scanner.etc_path == path

    def test_directory_does_not_exist_gets_created(self, tmpdir):
        path = os.path.join(str(tmpdir), 'subdir')
        scanner = scan.Scan([])
        scanner._etc_path = path
        assert scanner.etc_path == path
        assert os.path.isdir(path)

    def test_complains_when_file(self, tmpfile):
        path = tmpfile()
        scanner = scan.Scan([])
        scanner._etc_path = path
        with pytest.raises(RuntimeError):
            scanner.etc_path


class TestParseKeyring(object):

    def test_newlines_are_removed(self):
        contents = [
            '[client.osd-lockbox.8d7a8ab2-5db0-4f83-a785-2809aba403d5]',
            '\tkey = AQDtoGha/GYJExAA7HNl7Ukhqr7AKlCpLJk6UA==', '']
        assert '\n' not in scan.parse_keyring('\n'.join(contents))

    def test_key_has_spaces_removed(self):
        contents = [
            '[client.osd-lockbox.8d7a8ab2-5db0-4f83-a785-2809aba403d5]',
            '\tkey = AQDtoGha/GYJExAA7HNl7Ukhqr7AKlCpLJk6UA==', '']
        result = scan.parse_keyring('\n'.join(contents))
        assert result.startswith(' ') is False
        assert result.endswith(' ') is False

    def test_actual_key_is_extracted(self):
        contents = [
            '[client.osd-lockbox.8d7a8ab2-5db0-4f83-a785-2809aba403d5]',
            '\tkey = AQDtoGha/GYJExAA7HNl7Ukhqr7AKlCpLJk6UA==', '']
        result = scan.parse_keyring('\n'.join(contents))
        assert result == 'AQDtoGha/GYJExAA7HNl7Ukhqr7AKlCpLJk6UA=='
