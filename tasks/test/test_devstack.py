from textwrap import dedent

from .. import devstack


class TestDevstack(object):
    def test_parse_os_table(self):
        table_str = dedent("""
            +---------------------+--------------------------------------+
            |       Property      |                Value                 |
            +---------------------+--------------------------------------+
            |     attachments     |                  []                  |
            |  availability_zone  |                 nova                 |
            |       bootable      |                false                 |
            |      created_at     |      2014-02-21T17:14:47.548361      |
            | display_description |                 None                 |
            |     display_name    |                 NAME                 |
            |          id         | ffdbd1bb-60dc-4d95-acfe-88774c09ad3e |
            |       metadata      |                  {}                  |
            |         size        |                  1                   |
            |     snapshot_id     |                 None                 |
            |     source_volid    |                 None                 |
            |        status       |               creating               |
            |     volume_type     |                 None                 |
            +---------------------+--------------------------------------+
            """).strip()
        expected = {
            'Property': 'Value',
            'attachments': '[]',
            'availability_zone': 'nova',
            'bootable': 'false',
            'created_at': '2014-02-21T17:14:47.548361',
            'display_description': 'None',
            'display_name': 'NAME',
            'id': 'ffdbd1bb-60dc-4d95-acfe-88774c09ad3e',
            'metadata': '{}',
            'size': '1',
            'snapshot_id': 'None',
            'source_volid': 'None',
            'status': 'creating',
            'volume_type': 'None'}

        vol_info = devstack.parse_os_table(table_str)
        assert vol_info == expected




