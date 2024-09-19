#!/bin/sh -x

set -e

touch file

setfattr -n user.foo -v foo file
setfattr -n user.bar -v bar file
setfattr -n user.empty file
getfattr -d file | grep foo
getfattr -d file | grep bar
getfattr -d file | grep empty

echo OK.
