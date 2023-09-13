#!/bin/sh

set -e

mkdir x
cd x
touch a
touch b
touch c
touch d
ls
chmod 777 .
stat e || true
touch f
touch g

# over existing file
echo attempting rename over existing file...
touch ../xx
mv ../xx f
ls | grep f || false
echo rename over existing file is okay

# over negative dentry
echo attempting rename over negative dentry...
touch ../xx
mv ../xx e
ls | grep e || false
echo rename over negative dentry is ok

echo OK
