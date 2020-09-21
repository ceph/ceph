#!/usr/bin/env bash

# $1 - part
# $2 - branch name
# $3 - machine name
# $4 - email address
# $5 - filter out (this arg is to be at the end of the command line for now)

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
        # run master branch with --newest option looking for good sha1 7 builds back
        teuthology-suite -v -c $2 -m $3 -k distro -s rados --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/28 --newest 7 -e $4 ~/vps.yaml $5
elif [ $2 = "hammer" ] ; then
        # run hammer branch with less jobs
        teuthology-suite -v -c $2 -m $3 -k distro -s rados --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/56 -e $4 ~/vps.yaml $5
elif [ $2 = "jewel" ] ; then
        # run jewel branch with /40 jobs
        teuthology-suite -v -c $2 -m $3 -k distro -s rados --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/40 -e $4 ~/vps.yaml $5
else
        # run NON master branches without --newest 
        teuthology-suite -v -c $2 -m $3 -k distro -s rados --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/28 -e $4 ~/vps.yaml $5
fi

