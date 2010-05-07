#!/bin/sh

bad=0
for f in $1/pglog*
do
    hexdump -C $f | grep -q ^\* && bad=1 && echo $f has zeros
done
exit $bad