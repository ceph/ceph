
  $ sudo modprobe -r rbd
  $ sudo modprobe -r libceph
  $ lsmod | grep libceph
  [1]
  $ rbd create --size 1 img
  $ DEV=$(sudo rbd map img)
  $ sudo grep -q ',key=' /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap $DEV
  $ rbd rm --no-progress img
