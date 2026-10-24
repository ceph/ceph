from cephadmlib.daemons.smb import _truncated_vhostname, _MAX_HOSTNAME_LEN


class TestSafeVhostname:
    def test_short_name_unchanged(self):
        assert (
            _truncated_vhostname('smb1', 'short-host') == 'smb1-short-host'
        )

    def test_empty_hname_unchanged(self):
        assert _truncated_vhostname('smb1', '') == 'smb1-'

    def test_one_over_limit_is_shortened(self):
        hname = (
            'example-host-with-a-very-long-descriptive-hostname'
            '-number-01'
        )
        assert len(f'smb1-{hname}') == 65
        fixed = _truncated_vhostname('smb1', hname)
        assert len(fixed) == _MAX_HOSTNAME_LEN
        assert fixed.startswith('smb1-')
        assert fixed.endswith(hname[-20:])

    def test_similar_long_names_do_not_collide(self):
        a = 'z' * 100 + 'AAAA.tail.example.com'
        b = 'z' * 100 + 'BBBB.tail.example.com'
        fixed_a = _truncated_vhostname('smb1', a)
        fixed_b = _truncated_vhostname('smb1', b)
        assert fixed_a != fixed_b
        assert fixed_a.endswith('.tail.example.com')
        assert fixed_b.endswith('.tail.example.com')

    def test_long_instance_id_still_fits(self):
        instance_id = 'x' * 56
        hname = 'abcde12345'
        fixed = _truncated_vhostname(instance_id, hname)
        assert len(fixed) <= _MAX_HOSTNAME_LEN
