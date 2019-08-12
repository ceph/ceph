#!/bin/sh -x

set -e

rados -p data rm foo || true
rados -p data put foo.tmp /etc/passwd --object-locator foo
rados -p data clonedata foo.tmp foo --object-locator foo
rados -p data get foo /tmp/foo
cmp /tmp/foo /etc/passwd
rados -p data rm foo.tmp --object-locator foo
rados -p data rm foo

echo OK