#!/bin/bash

# default value
k_way=3 #11
repeat=2 #5

output_file="" 
if [ "$1" != "" ]; then
  output_file="$1"
else
  echo "Please provide the name of the output file"
  exit
fi

echo "generating file ${output_file}"
sh data_gen.sh ${output_file} ${k_way} ${repeat}

echo "converting ${output_file} to ${output_file}.dat"
python data_parser.py ${output_file}

echo "now generating bar-chart"
#gnuplot -e 'output_file=value'  plot_gen.gnuplot 
sh plot_gen.sh  ${output_file} ${k_way}
echo "done! check ${output_file}.pdf"
