#!/usr/bin/env bash

#command line => CEPH_BRANCH=<branch>; MACHINE_NAME=<machine_type>; SUITE_NAME=<suite>; ../schedule_subset.sh <day_of_week> $CEPH_BRANCH $MACHINE_NAME $SUITE_NAME $CEPH_QA_EMAIL $KERNEL <$FILTER>

# $1 - part (day of week)
# $2 - branch name
# $3 - machine name
# $4 - suite name
# $5 - email address
# $6 - kernel (distro or testing)
# $7 - filter out (this arg is to be at the end of the command line for now)

## example #1 
## (date +%U) week number
## % 2 - mod 2 (e.g. 0,1,0,1 ...)
## * 7 -  multiplied by 7 (e.g. 0,7,0,7...)
## $1 day of the week (0-6)
## /14 for 2 weeks

## example #2 
## (date +%U) week number
## % 4 - mod 4 (e.g. 0,1,2,3,0,1,2,3 ...)
## * 7 -  multiplied by 7 (e.g. 0,7,14,21,0,7,14,21...)
## $1 day of the week (0-6)
## /28 for 4 weeks

echo "Scheduling " $2 " branch"
if [ $2 = "master" ] ; then
        # run master branch with --newest option looking for good sha1 7 builds back with /999 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/999 --newest 7 -e $5 $7
elif [ $2 = "hammer" ] ; then
        # run hammer branch with less jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/56 -e $5 $7
elif [ $2 = "jewel" ] ; then
        # run jewel branch with /40 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/40 -e $5 $7
elif [ $2 = "kraken" ] ; then
        # run kraken branch with /999 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/999 -e $5 $7
elif [ $2 = "luminous" ] ; then
        # run luminous branch with /999 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/999 -e $5 $7
elif [ $2 = "mimic" ] ; then
        # run mimic branch with /999 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/999 -e $5 $7
elif [ $2 = "nautilus" ] ; then
        # run nautilus branch with /2999 jobs == ~ 250 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/2999 -e $5 $7
else
        # run NON master branches without --newest 
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/999 -e $5 $7
fi
