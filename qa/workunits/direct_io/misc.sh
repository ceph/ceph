#!/bin/sh -ex

# a few test cases from henry
echo "test read from hole"
dd if=/dev/zero of=dd3 bs=1 seek=1048576 count=0
dd if=dd3 of=/tmp/ddout1 skip=8 bs=512 count=2 iflag=direct
dd if=/dev/zero of=/tmp/dd3 bs=512 count=2
cmp /tmp/dd3 /tmp/ddout1

echo "other thing"
dd if=/dev/urandom of=/tmp/dd10 bs=500 count=1
dd if=/tmp/dd10 of=dd10 bs=512 seek=8388 count=1
dd if=dd10 of=/tmp/dd10out bs=512 skip=8388 count=1 iflag=direct
cmp /tmp/dd10 /tmp/dd10out

echo OK
