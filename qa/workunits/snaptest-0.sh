#!/bin/sh -x

set -e

echo asdf > foo
mkdir .snap/foo
grep asdf .snap/foo/foo
rmdir .snap/foo

echo asdf > bar
mkdir .snap/bar
rm bar
grep asdf .snap/bar/bar
rmdir .snap/bar
rm foo

echo OK