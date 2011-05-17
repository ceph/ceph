#!/usr/bin/python

import rgw
import sys

r = rgw.Rgw()

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

print "converting %s to binary..." % xml
blob = r.acl_xml2bin(xml)
print "got blob of length %d" % len(blob)

xml2 = r.acl_bin2xml(blob)

blob2 = r.acl_xml2bin(xml2)

if (blob != blob2):
    raise "blob differed from blob2!"

sys.exit(0)
