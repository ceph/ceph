Set up

  $ ceph osd pool create rbda
  pool 'rbda' created
  $ rbd pool init rbda
  $ rbd create rbda/image1 --size 1000

Test at map options level

  $ OPTIONS="alloc_size=65536,lock_on_read"
  $ EXPECTED="${OPTIONS}"
  $ DEV=$(sudo rbd map rbda/image1 --options ${OPTIONS})
  $ sudo grep -q ${EXPECTED} /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap rbda/image1

Test at global level

  $ OPTIONS="alloc_size=4096,crc"
  $ EXPECTED="${OPTIONS}"
  $ rbd config global set global rbd_default_map_options ${OPTIONS}
  $ DEV=$(sudo rbd map rbda/image1)
  $ sudo grep -q ${EXPECTED} /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap rbda/image1

  $ OPTIONS="alloc_size=65536,lock_on_read"
  $ EXPECTED="alloc_size=65536,crc,lock_on_read"
  $ DEV=$(sudo rbd map rbda/image1 --options ${OPTIONS})
  $ sudo grep -q ${EXPECTED} /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap rbda/image1

Test at pool level

  $ OPTIONS="alloc_size=8192,share"
  $ EXPECTED="${OPTIONS}"
  $ rbd config pool set rbda rbd_default_map_options ${OPTIONS}
  $ DEV=$(sudo rbd map rbda/image1)
  $ sudo grep -q ${EXPECTED} /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap rbda/image1

  $ OPTIONS="lock_on_read,alloc_size=65536"
  $ EXPECTED="alloc_size=65536,lock_on_read,share"
  $ DEV=$(sudo rbd map rbda/image1 --options ${OPTIONS})
  $ sudo grep -q ${EXPECTED} /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap rbda/image1

Test at image level

  $ OPTIONS="alloc_size=16384,tcp_nodelay"
  $ EXPECTED="${OPTIONS}"
  $ rbd config image set rbda/image1 rbd_default_map_options ${OPTIONS}
  $ DEV=$(sudo rbd map rbda/image1)
  $ sudo grep -q ${EXPECTED} /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap rbda/image1

  $ OPTIONS="lock_on_read,alloc_size=65536"
  $ EXPECTED="alloc_size=65536,lock_on_read,tcp_nodelay"
  $ DEV=$(sudo rbd map rbda/image1 --options ${OPTIONS})
  $ sudo grep -q ${EXPECTED} /sys/bus/rbd/devices/${DEV#/dev/rbd}/config_info
  $ sudo rbd unmap rbda/image1

Teardown

  $ ceph osd pool rm rbda rbda --yes-i-really-really-mean-it
  pool 'rbda' removed
