#!/bin/bash

set -e

function create_loops() {

	NUM_OSDS=$1
	if [[ -z $NUM_OSDS ]]; then
	  echo "Call setup_loop <num_osds> to setup with more osds"
	  echo "Using default number of osds: 1."
	  NUM_OSDS=1
	fi

	# minimum 5 GB for each osd
	SIZE=$(expr $NUM_OSDS \* 5)
	# extra space just in case
	SIZE=$(expr $SIZE + 2)

	echo "Using ${SIZE} GB of space"

	# look for an available loop device
	avail_loop=$(sudo losetup -f)
	loop_name=$(basename -- $avail_loop)

	if [[ ! -e $avail_loop ]]
	then
		# in case we have to create the loop, find the minor device number.
		num_loops=$(lsmod | grep loop | awk '{print $3}')
		num_loops=$((num_loops + 1))
		echo creating loop $avail_loop minor: $num_loops
		mknod $avail_loop b 7 $num_loops
	fi

	if mountpoint -q $avail_loop
	then
		sudo umount $avail_loop
	fi

	if [[ ! -z $(losetup -l | grep $avail_loop) ]]
	then
		sudo losetup -d $avail_loop
	fi

	if [[ ! -e loop-images ]]
	then
		mkdir -p loop-images
	fi
	sudo rm -f loop-images/*
	sudo dd if=/dev/zero of="loop-images/disk${loop_name}.img" bs=1G count=$SIZE
	sudo losetup $avail_loop "loop-images/disk${loop_name}.img"

	if [[ ! -z $(sudo vgs | grep vg1) ]]
	then
		sudo lvm vgremove -f -y vg1
	fi
	sudo pvcreate $avail_loop
	sudo vgcreate vg1 $avail_loop

	for ((i=0;i<$NUM_OSDS;i++)); do
	  sudo vgchange --refresh
	  sudo lvcreate --size 5G --name "lv${i}" "vg1"
	done;
}
