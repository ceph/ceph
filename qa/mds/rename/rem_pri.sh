#!/bin/sh -ex

dotest() {
    srci=$1
    srcdn=$2
    dest=$3
    n=$4

    touch mnt/$srci/srci$n
    ln mnt/$srci/srci$n mnt/$srcdn/srcdn$n
    touch mnt/$dest/dest$n

    mv mnt/$srcdn/srcdn$n mnt/$dest/dest$n
}

# srci=srcdn=destdn
dotest 'a' 'a' 'a' 1

# srcdn=destdn
dotest 'b' 'a' 'a' 2

# srci=destdn
dotest 'a' 'b' 'a' 3

# srci=srcdn
dotest 'a' 'a' 'b' 4

# all different
dotest 'a' 'b' 'c' 5
