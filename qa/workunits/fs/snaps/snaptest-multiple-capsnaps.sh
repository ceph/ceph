#!/bin/sh -x

set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

echo asdf > a
mkdir .snap/1
chmod 777 a
mkdir .snap/2
echo qwer > a
mkdir .snap/3
chmod 666 a
mkdir .snap/4
echo zxcv > a
mkdir .snap/5

ls -al .snap/?/a

grep asdf .snap/1/a
stat .snap/1/a | grep 'Size: 5'

grep asdf .snap/2/a
stat .snap/2/a | grep 'Size: 5'
stat .snap/2/a | grep -- '-rwxrwxrwx'

grep qwer .snap/3/a
stat .snap/3/a | grep 'Size: 5'
stat .snap/3/a | grep -- '-rwxrwxrwx'

grep qwer .snap/4/a
stat .snap/4/a | grep 'Size: 5'
stat .snap/4/a | grep -- '-rw-rw-rw-'

grep zxcv .snap/5/a
stat .snap/5/a | grep 'Size: 5'
stat .snap/5/a | grep -- '-rw-rw-rw-'

rmdir .snap/[12345]

echo "OK"



