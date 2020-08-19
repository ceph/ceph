#!/bin/bash

#
# Testing script which under 'target' option starts 'rbd-nvmf-gw' tool
# and maps RBD image, under 'host' option setups nvme driver.
#

NODE=nqn.2020-08.io.spdk:rbd

SCRIPT_PATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
SPDK_PATH=$SCRIPT_PATH/../../spdk

if ! [ $(id -u) = 0 ]; then
   echo "This script must be run as root"
   exit 1
fi

usage()
{
	echo "Usage:"
	echo " as target:  <target> <ip> <rbd-pool> <rbd-image>"
	echo " as host:    <host>   <ip>"
	exit
}

if [[ $# < 2 ]]; then
	usage
fi

IP=$2

if [ $1 == "host" ]; then
	echo "transport=tcp,traddr=$IP,nqn=$NODE,trsvcid=4420,queue_size=512" > /dev/nvme-fabrics
	exit
fi

if [ $1 != "target" ]; then
	usage
fi

if [[ $# < 4 ]]; then
	usage
fi

RBD_POOL=$3
RBD_IMAGE=$4

echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

$SCRIPT_PATH/../../../build/bin/rbd-nvmf-gw -m 0xff -B &
sleep 1

$SPDK_PATH/scripts/rpc.py bdev_rbd_create -b Rbd1 $RBD_POOL $RBD_IMAGE 512
$SPDK_PATH/scripts/rpc.py nvmf_create_transport -t TCP -g nvmf_example

$SPDK_PATH/scripts/rpc.py nvmf_create_subsystem -t nvmf_example -s SPDK00000000000001 -a -m 32 $NODE
$SPDK_PATH/scripts/rpc.py nvmf_subsystem_add_ns -t nvmf_example $NODE Rbd1
$SPDK_PATH/scripts/rpc.py nvmf_subsystem_add_listener -t tcp -f Ipv4 -a $IP -s 4420 -p nvmf_example $NODE


wait
