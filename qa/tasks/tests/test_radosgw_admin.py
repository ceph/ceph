from mock import Mock

from .. import radosgw_admin

acl_with_version = """<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>foo</ID><DisplayName>Foo</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>foo</ID><DisplayName>Foo</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>
"""  # noqa


acl_without_version = """<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>foo</ID><DisplayName>Foo</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>foo</ID><DisplayName>Foo</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>
"""  # noqa


class TestGetAcl(object):

    def setup(self):
        self.key = Mock()

    def test_removes_xml_version(self):
        self.key.get_xml_acl = Mock(return_value=acl_with_version)
        result = radosgw_admin.get_acl(self.key)
        assert result.startswith('<AccessControlPolicy')

    def test_xml_version_is_already_removed(self):
        self.key.get_xml_acl = Mock(return_value=acl_without_version)
        result = radosgw_admin.get_acl(self.key)
        assert result.startswith('<AccessControlPolicy')

    def test_newline_gets_trimmed(self):
        self.key.get_xml_acl = Mock(return_value=acl_without_version)
        result = radosgw_admin.get_acl(self.key)
        assert result.endswith('\n') is False
