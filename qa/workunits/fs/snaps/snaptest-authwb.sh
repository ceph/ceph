#!/bin/sh -x

set -e

touch foo
chmod +x foo
mkdir .snap/s
find .snap/s/foo -executable | grep foo
rmdir .snap/s
rm foo

echo OK
