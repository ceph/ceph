#!/bin/bash
config_dir="configs"
repeat=2 #5

# parameter check -- output_file name
if [ "$1" != "" ]; then
  output_file="$1"
else
  echo "Please provide the name of the output file"
  exit
fi

# parameter check -- k-value
if [ "$2" != "" ]; then
  k_way="$2"
else
  echo "Please provide the maximum K_WAY value"
  exit
fi

# parameter check --repeat
if [ "$3" != "" ]; then
  repeat="$3"
fi

echo "k-way:$k_way, num_repeat:$repeat"

# create simulators in different directories 
k=2
while [ $k -le $k_way ]
do
  mkdir "build_$k"
  cd "build_$k"
  rm -rf *
  cmake -DCMAKE_BUILD_TYPE=Release -DK_WAY_HEAP=$k ../../.
  make dmclock-sims
  cd ..
  
  k=$(( $k + 1 ))
done

# run simulators 
echo '' > $output_file
for config in "$config_dir"/*.conf
do
  k=2
  while [ $k -le $k_way ]
  do
    cd "build_$k"
    
    # repeat same experiment
    i=0
    while [ $i -lt $repeat ]
    do  
      i=$(( $i + 1 ))
      
      # clear cache first
      sync
      #sudo sh -c 'echo 1 >/proc/sys/vm/drop_caches'
      #sudo sh -c 'echo 2 >/proc/sys/vm/drop_caches'
      #sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'

      # run with heap
      msg="file_name:$k:$config"
      echo $msg >> ../$output_file
      echo "running $msg ..."
      ./sim/dmc_sim -c ../$config | awk '(/average/)' >> ../$output_file
    done # end repeat
    cd ..
    k=$(( $k + 1 ))
  done # end k_way
done # end config

