#!/bin/bash
IP=$1
first_port=$2
last_port=$3

function scanner {
  for ((port=$first_port; port<=$last_port; port++))
    do
      (echo >/dev/tcp/$IP/$port)> /dev/null 2>&1 && echo $port open || echo "$port closed"
    done
}

scanner
