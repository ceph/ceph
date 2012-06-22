#!/bin/sh -x
set -e

IMAGE_URL=http://cloud-images.ubuntu.com/releases/precise/release/ubuntu-12.04-server-cloudimg-amd64-disk1.img

wget -O base.qcow2 $IMAGE_URL

image=base.raw
qemu-img convert -O raw base.qcow2 $image
rm -f base.qcow2

# Note: this assumes that sector size is 512, and that there's only one
# partition. very brittle.
START_SECT=$(fdisk -lu $image | grep ^$image | awk '{print $3}')
START_BYTE=$(echo "$START_SECT * 512" | bc)

root=/tmp/$$

cleanup() {
    sudo chroot $root rm -f /etc/resolv.conf || true
    sudo chroot $root ln -s ../run/resolvconf/resolv.conf /etc/resolv.conf || true
	sudo umount $root/proc || true
	sudo umount $root/sys || true
	sudo umount $root/dev/pts || true
	sudo umount $root
    sudo rmdir $root
}
trap cleanup INT TERM EXIT

sudo mkdir $root
sudo mount -o loop,offset=$START_BYTE $image $root

# set up chroot
sudo mount -t proc proc $root/proc
sudo mount -t sysfs sysfs $root/sys
sudo mount -t devpts devptr $root/dev/pts

# set up network access
sudo chroot $root rm /etc/resolv.conf
sudo cp /etc/resolv.conf $root/etc/resolv.conf

# packages
# These should be kept in sync with ceph-qa-chef.git/cookbooks/ceph-qa/default.rb
sudo chroot $root apt-get -y --force-yes install iozone3 bonnie++ dbench \
    tiobench build-essential attr libtool automake gettext uuid-dev      \
    libacl1-dev bc xfsdump dmapi xfslibs-dev

# install ltp without ltp-network-test, so we don't pull in xinetd and
# a bunch of other unnecessary stuff
sudo chroot $root apt-get -y --force-yes --no-install-recommends install ltp-kernel-test

# add 9p fs support
sudo chroot $root apt-get -y --force-yes install linux-image-extra-virtual

cleanup
trap - INT TERM EXIT

qemu-img convert -O qcow2 $image output.qcow2
rm -f $image

exit 0
