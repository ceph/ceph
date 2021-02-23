#!/usr/bin/env bash

#command line => CEPH_BRANCH=<branch>; MACHINE_NAME=<machine_type>; SUITE_NAME=<suite>; ../schedule_subset.sh <day_of_week> $CEPH_BRANCH $MACHINE_NAME $SUITE_NAME $CEPH_QA_EMAIL $KERNEL <$FILTER>

# $1 - part (added to day of year)
# $2 - branch name
# $3 - machine name
# $4 - suite name
# $5 - email address
# $6 - kernel (distro or testing)
# $7 - filter out (this arg is to be at the end of the command line for now)

echo "Scheduling " $2 " branch"
if [ $2 = "master" ] ; then
        # run master branch with --newest option looking for good sha1 7 builds back with /100000 jobs
        # using '-p 80 --force-priority' as an execption ATM
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/100000 --newest 7 -e $5 $7 -p 100 --force-priority
elif [ $2 = "jewel" ] ; then
        # run jewel branch with /40 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/40 -e $5 $7
elif [ $2 = "kraken" ] ; then
        # run kraken branch with /999 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/999 -e $5 $7
elif [ $2 = "luminous" ] ; then
        # run luminous branch with /999 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/999 -e $5 $7
elif [ $2 = "mimic" ] ; then
        # run mimic branch with /999 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/999 -e $5 $7
elif [ $2 = "nautilus" ] ; then
        # run nautilus branch with /2999 jobs == ~ 250 jobs
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/2999 -e $5 $7
elif [ $2 = "octopus" ] ; then
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/9999 -e $5 $7
        # run octopus branch with /100000 jobs for rados == ~ 300 job
elif [ $2 = "pacific" ] ; then
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/99999 -e $5 $7 -p 80 --force-priority
        # run pacific branch with /99999 jobs for rados == ~ 367 job
else
        # run NON master branches without --newest
        teuthology-suite -v -c $2 -m $3 -k $6 -s $4 --subset $(echo "$(date +%j) + $1" | bc)/999 -e $5 $7
fi
