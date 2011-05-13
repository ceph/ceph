#!/usr/bin/python

import rgw
import sys

r = rgw.Rgw()

xml = """<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n
  <Owner>\n
    <ID>foo</ID>\n
    <DisplayName>MrFoo</DisplayName>\n
  </Owner>\n
  <AccessControlList>\n
    <Grant>\n
      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\n
	<ID>bar</ID>\n
	<DisplayName>display-name</DisplayName>\n
      </Grantee>\n
      <Permission>FULL_CONTROL</Permission>\n
    </Grant>\n
  </AccessControlList>\n
</AccessControlPolicy>"""

print "converting %s to binary..." % xml
blob = r.acl_xml2bin(xml)
print "got blob of length %d" % len(blob)

xml2 = r.acl_bin2xml(blob)

blob2 = r.acl_xml2bin(xml2)

if (blob != blob2):
    raise "blob differed from blob2!"

sys.exit(0)
