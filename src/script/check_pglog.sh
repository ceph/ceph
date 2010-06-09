#!/bin/sh

cosd=`which cosd`
[ -z "$cosd" ] && cosd="./cosd"

bad=0
for f in $1/current/meta/pglog*
do
    echo -n "checking $f ... "
    $cosd --dump-pg-log $f > /dev/null && echo ok || ( bad=1 && echo corrupt )
done
exit $bad