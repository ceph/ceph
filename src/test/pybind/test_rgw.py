from nose.tools import eq_ as eq, assert_raises
from rgw import Rgw

def test_rgw():
    rgw = Rgw()
    xml = """<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>foo</ID>
    <DisplayName>MrFoo</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">
	<ID>bar</ID>
	<DisplayName>display-name</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>"""
    blob = rgw.acl_xml2bin(xml)
    converted_xml = rgw.acl_bin2xml(blob)
    converted_blob = rgw.acl_xml2bin(converted_xml)
    eq(blob, converted_blob)
