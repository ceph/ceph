#!/bin/sh -x

set -e

image="$1"
root=/tmp/$$
dist=squeeze

srcdir=`dirname $0`

# image
[ -e $image ] && echo $image exists && exit 1
dd if=/dev/zero of=$image bs=1M seek=1023 count=1
yes | mke2fs -j $image

mkdir $root
mount -o loop $image $root

cleanup() {
	umount $root/proc || true
	umount $root/sys || true
	umount $root/dev/pts || true
	umount $root
}
trap cleanup INT TERM EXIT

debootstrap $dist $root http://http.us.debian.org/debian/

cat <<EOF >> $root/etc/fstab
none /dev/pts devpts gid=5,mode=620 0 0
proc /proc proc defaults 0 0
sysfs /sys sysfs defaults 0 0
none /sys/kernel/debug debugfs defaults 0 0
tmpfs /tmp tmpfs defaults,size=768M 0 0
none /host hostfs defaults 0 0
EOF

echo "uml" > $root/etc/hostname
mkdir $root/host

# set up chroot
mount -t proc proc $root/proc
mount -t sysfs sysfs $root/sys
mount -t devpts devptr $root/dev/pts -o gid=5,mode=620

# set up network
cp $srcdir/network-from-cmdline $root/etc/init.d/network-from-cmdline
chroot $root update-rc.d network-from-cmdline defaults 20

# kcon_ helpers
cp $srcdir/kcon_most.sh $root/root/most.sh
cp $srcdir/kcon_all.sh $root/root/all.sh

# fix up consoles
cat <<EOF >> $root/etc/securetty 

# uml
tty0
vc/0
EOF

cat <<EOF >> $root/etc/inittab

# uml just one console
c0:1235:respawn:/sbin/getty 38400 tty0 linux
EOF
grep -v tty[23456] $root/etc/inittab > $root/etc/inittab.new
mv $root/etc/inittab.new $root/etc/inittab

# no root password
chroot $root passwd -d root

# copy creating user's authorized_keys
mkdir $root/root/.ssh
chmod 700 $root/root/.ssh
cp ~/.ssh/authorized_keys $root/root/.ssh/authorized_keys
chmod 600 $root/root/.ssh/authorized_keys

# packages
chroot $root apt-get -y --force-yes install ssh libcrypto\+\+ bind9-host

exit 0
