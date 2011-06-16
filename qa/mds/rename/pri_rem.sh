#!/bin/sh -ex

dotest() {
    src=$1
    desti=$2
    destdn=$3
    n=$4

    touch mnt/$src/src$n
    touch mnt/$desti/desti$n
    ln mnt/$desti/desti$n mnt/$destdn/destdn$n

    mv mnt/$src/src$n mnt/$destdn/destdn$n
}


# srcdn=destdn=desti
dotest 'a' 'a' 'a' 1

# destdn=desti
dotest 'b' 'a' 'a' 2

# srcdn=destdn
dotest 'a' 'b' 'a' 3

# srcdn=desti
dotest 'a' 'a' 'b' 4

# all different
dotest 'a' 'b' 'c' 5

