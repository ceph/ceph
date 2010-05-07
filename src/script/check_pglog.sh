#!/bin/sh

bad=0
for f in $1/pglog*
do
    grep -q ^\* $f && bad=1 && echo $f has zeros
done
exit $bad