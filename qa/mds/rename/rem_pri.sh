#!/bin/sh -ex

dotest() {
    srci=$1
    srcdn=$2
    dest=$3
    n=$4

    touch ./$srci/srci$n
    ln ./$srci/srci$n ./$srcdn/srcdn$n
    touch ./$dest/dest$n

    mv ./$srcdn/srcdn$n ./$dest/dest$n
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
