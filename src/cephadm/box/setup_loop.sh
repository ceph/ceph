#!/bin/bash

function clean_vg() {
  sudo lvm vgremove -f -y vg1
  sudo rm loop-images/*
}


function create_loops() {
	clean_vg

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

	# in case we have to create the loop, find the minor device number.
	num_loops=$(lsmod | grep loop | awk '{print $3}')
	num_loops=$((num_loops + 1))
	echo creating loop $avail_loop minor: $num_loops
	mknod $avail_loop b 7 $num_loops
	sudo umount $avail_loop
	sudo losetup -d $avail_loop
	mkdir -p loop-images
	# sudo fallocate -l 10G "loop-images/disk${loop_name}.img"
	sudo dd if=/dev/zero of="loop-images/disk${loop_name}.img" bs=1G count=$SIZE
	sudo losetup $avail_loop "loop-images/disk${loop_name}.img"

	sudo vgcreate vg1 $avail_loop
	sudo pvcreate $avail_loop

	for ((i=0;i<$NUM_OSDS;i++)); do
	  sudo vgchange --refresh
	  sudo lvcreate --size 5G --name "lv${i}" "vg1"
	done;
}
