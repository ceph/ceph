from ceph_volume.devices.simple import scan


class TestGetContentst(object):

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
