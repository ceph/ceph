#!/bin/bash

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
#echo "k-way: $k_way"
#exit

gnuplot << EOF

# Note you need gnuplot 4.4 for the pdfcairo terminal.
clear
reset

set terminal pdfcairo size 7in,5in font "Gill Sans,5" linewidth 1 rounded fontscale .8 noenhanced
set output "${output_file}.pdf"

# starts multiplot
set multiplot layout 2,1

# Line style for axes
set style line 80 lt rgb "#808080"

# Line style for grid
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80 

#set xtics rotate out
set style data histogram
set style histogram clustered

set style fill solid border
set xlabel 'Heap Timing for different K values'   
set ylabel 'Time (nanosec)'        
set key top right

set yrange [0:*]

# plot 1
set title 'Request Addition Time'
plot for [COL=2:($k_way + 1)] '${output_file}.dat' using COL:xticlabels(1) title columnheader

# plot 2
set title 'Request Completion Time'
plot for [COL=($k_way + 2):(2 * $k_way + 1)] '${output_file}.dat' using COL:xticlabels(1) title columnheader
EOF
