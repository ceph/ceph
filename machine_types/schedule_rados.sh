#!/bin/bash

# $1 - part
# $2 - branch name
# $3 - machine name
# $4 - filter out
# $5 - email address

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

teuthology-suite -v -c $2 -m $3 -k distro -s rados --subset $(echo "(($(date +%U) % 4) * 7) + $1" | bc)/28 $4 -e $5
