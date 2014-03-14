#!/bin/sh -x

set -e

touch file

setfattr -n user.bare -v bare file
setfattr -n user.bar -v bar file
getfattr -d file | grep bare

echo OK.
