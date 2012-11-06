#!/bin/bash

set -x

gen_fio_file() {
  iter=$1
  f=$2
  cat > randio-$$-${iter}.fio <<EOF
[randio]
blocksize_range=32m:128m
blocksize_unaligned=1
filesize=10G:20G
readwrite=randrw
runtime=300
size=20G
filename=${f}
EOF
}

sudo apt-get -y install fio
for i in $(seq 1 20); do
  fcount=$(ls donetestfile* 2>/dev/null | wc -l)
  donef="foo"
  fiof="bar"
  if test ${fcount} -gt 0; then
     # choose random file
     r=$[ ${RANDOM} % ${fcount} ]
     testfiles=( $(ls donetestfile*) )
     donef=${testfiles[${r}]}
     fiof=$(echo ${donef} | sed -e "s|done|fio|")
     gen_fio_file $i ${fiof}
  else
     fiof=fiotestfile.$$.$i
     donef=donetestfile.$$.$i
     gen_fio_file $i ${fiof}
  fi

  sudo rm -f ${donef}
  sudo fio randio-$$-$i.fio
  sudo ln ${fiof} ${donef}
  ls -la
done
