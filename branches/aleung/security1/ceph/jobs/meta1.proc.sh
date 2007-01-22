#!/bin/sh

for d in 1 2 4 8 12
do
 echo $d
 cd $d
 ../../../script/sum.pl mds? mds?? > mds.sum
 ../../../script/sum.pl -avg mds? mds?? > mds.avg

 ../../../script/sum.pl -start  90 -end 180 mds? mds?? > mds.sum.makedirs
 ../../../script/sum.pl -start 200 -end 300 mds? mds?? > mds.sum.walk

 cd ..
done
