#!/bin/sh -ex

dotest() {
    srci=$1
    srcdn=$2
    desti=$3
    destdn=$4
    n=$5

    touch ./$srci/srci$n
    ln ./$srci/srci$n ./$srcdn/srcdn$n
    touch ./$desti/desti$n
    ln ./$desti/desti$n ./$destdn/destdn$n

    mv ./$srcdn/srcdn$n ./$destdn/destdn$n
}

#  srci=srcdn=destdn=desti
dotest 'a' 'a' 'a' 'a' 1

#  srcdn=destdn=desti
dotest 'b' 'a' 'a' 'a' 2

#  srci=destdn=desti
dotest 'a' 'b' 'a' 'a' 3

#  srci=srcdn=destdn
dotest 'a' 'a' 'b' 'a' 4

#  srci=srcdn=desti
dotest 'a' 'a' 'a' 'b' 5

#  srci=srcdn destdn=desti
dotest 'a' 'a' 'b' 'b' 6

#  srci=destdn srcdn=desti
dotest 'a' 'b' 'b' 'a' 7

#  srci=desti srcdn=destdn
dotest 'a' 'b' 'a' 'b' 8

#  srci=srcdn
dotest 'a' 'a' 'b' 'c' 9

#  srci=desti
dotest 'a' 'b' 'a' 'c' 10

#  srci=destdn
dotest 'a' 'b' 'c' 'a' 11

#  srcdn=desti
dotest 'a' 'b' 'b' 'c' 12

#  srcdn=destdn
dotest 'a' 'b' 'c' 'b' 13

#  destdn=desti
dotest 'a' 'b' 'c' 'c' 14

#  all different
dotest 'a' 'b' 'c' 'd' 15
