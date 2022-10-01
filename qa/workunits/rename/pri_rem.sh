#!/bin/sh -ex

dotest() {
    src=$1
    desti=$2
    destdn=$3
    n=$4

    touch ./$src/src$n
    touch ./$desti/desti$n
    ln ./$desti/desti$n ./$destdn/destdn$n

    mv ./$src/src$n ./$destdn/destdn$n
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

