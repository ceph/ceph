#!/bin/sh

set -ex

if [ -d "$1" ]; then
  mkdir -p -- "$1" && cd "$1"
fi

[ "$VERIFY" != verify ] && mkdir 1
[ "$VERIFY" != verify ] && mkdir 1/.snap/first
stat 1/.snap/first
[ "$VERIFY" != verify ] && mkdir 1/2
stat 1/.snap/first/2 && exit 1
[ "$VERIFY" != verify ] && mkdir 1/2/.snap/second
stat 1/2/.snap/second
[ "$VERIFY" != verify ] && touch 1/foo
stat 1/.snap/first/foo && exit 1
[ "$VERIFY" != verify ] && mkdir 1/.snap/third
stat 1/.snap/third/foo || exit 1
[ "$VERIFY" != verify ] && mkdir 1/2/3
[ "$VERIFY" != verify ] && mkdir 1/2/.snap/fourth
stat 1/2/.snap/fourth/3

exit 0
