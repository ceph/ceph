#!/bin/bash

set -ex

sudo modprobe nvme-fabrics
sudo modprobe nvme-tcp
sudo dnf install nvme-cli -y

# import NVMEOF_GATEWAY_IP_ADDRESS and NVMEOF_GATEWAY_NAME=nvmeof.poolname.smithiXXX.abcde
source /etc/ceph/nvmeof.env

HOSTNAME=$(hostname)
IMAGE="quay.io/ceph/nvmeof-cli:latest"
RBD_POOL=$(awk -F'.' '{print $2}' <<< "$NVMEOF_GATEWAY_NAME")
RBD_IMAGE="myimage"
RBD_SIZE=$((1024*8)) #8GiB
BDEV="mybdev"
SERIAL="SPDK00000000000001"
NQN="nqn.2016-06.io.spdk:cnode1"
PORT="4420"
SRPORT="5500"
DISCOVERY_PORT="8009"

rbd create $RBD_POOL/$RBD_IMAGE --size $RBD_SIZE
sudo podman run -it $IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $SRPORT create_bdev --pool $RBD_POOL --image $RBD_IMAGE --bdev $BDEV
sudo podman images
sudo podman ps
sudo podman run -it $IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $SRPORT create_subsystem --subnqn $NQN --serial $SERIAL
sudo podman run -it $IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $SRPORT add_namespace --subnqn $NQN --bdev $BDEV
sudo podman run -it $IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $SRPORT create_listener -n $NQN -g client.$NVMEOF_GATEWAY_NAME -a $NVMEOF_GATEWAY_IP_ADDRESS -s $PORT
sudo podman run -it $IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $SRPORT add_host --subnqn $NQN --host "*"
sudo podman run -it $IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $SRPORT get_subsystems
sudo lsmod | grep nvme
sudo nvme list
sudo nvme discover -t tcp -a $NVMEOF_GATEWAY_IP_ADDRESS -s $DISCOVERY_PORT
sudo nvme connect -t tcp --traddr $NVMEOF_GATEWAY_IP_ADDRESS -s $PORT -n $NQN
sudo nvme list

echo "testing nvmeof initiator..."

nvme_model="SPDK bdev Controller"

echo "Test 1: create initiator - starting"
if ! sudo nvme list | grep -q "$nvme_model"; then
  echo "nvmeof initiator not created!"
  exit 1
fi
echo "Test 1: create initiator - passed!"


echo "Test 2: device size - starting"
image_size_in_bytes=$(($RBD_SIZE * 1024 * 1024))
nvme_size=$(sudo nvme list --output-format=json | \
        jq -r ".Devices | .[] | select(.ModelNumber == \"$nvme_model\") | .PhysicalSize")
if [ "$image_size_in_bytes" != "$nvme_size" ]; then
  echo "block device size do not match!"
  exit 1
fi
echo "Test 2: device size - passed!"


echo "Test 3: basic IO - starting"
nvme_drive=$(sudo nvme list --output-format=json | \
        jq -r ".Devices | .[] | select(.ModelNumber == \"$nvme_model\") | .DevicePath")
io_input_file="/tmp/nvmeof_test_input"
echo "Hello world" > $io_input_file
truncate -s 2k $io_input_file
sudo dd if=$io_input_file of=$nvme_drive oflag=direct count=1 bs=2k #write
io_output_file="/tmp/nvmeof_test_output"
sudo dd if=$nvme_drive of=$io_output_file iflag=direct count=1 bs=2k #read
if ! cmp $io_input_file $io_output_file; then
  echo "nvmeof initiator - io test failed!"
  exit 1
fi
sudo rm -f $io_input_file $io_output_file
echo "Test 3: basic IO - passed!"


echo "nvmeof initiator tests passed!"
