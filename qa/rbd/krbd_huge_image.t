
  $ get_field() {
  >     rbd info --format=json $1 | python -c "import sys, json; print json.load(sys.stdin)['$2']"
  > }

Write to first and last sectors and make sure we hit the right objects:

  $ ceph osd pool create hugeimg 12 >/dev/null 2>&1
  $ rbd pool init hugeimg
  $ rbd create --size 4E --object-size 4K --image-feature layering hugeimg/img
  $ DEV=$(sudo rbd map hugeimg/img)
  $ xfs_io -c 'pwrite 0 512' $DEV >/dev/null # first sector
  $ xfs_io -c 'pwrite 4611686018427387392 512' $DEV >/dev/null # last sector
  $ sudo rbd unmap $DEV

  $ get_field hugeimg/img size
  4611686018427387904
  $ get_field hugeimg/img objects
  1125899906842624
  $ rados -p hugeimg ls | grep $(get_field hugeimg/img block_name_prefix) | sort
  .*\.0000000000000000 (re)
  .*\.0003ffffffffffff (re)

Dump first and last megabytes:

  $ DEV=$(sudo rbd map hugeimg/img)
  $ hexdump -n 1048576 $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0000200 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0100000
  $ hexdump -s 4611686018426339328 $DEV
  3ffffffffff00000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  3ffffffffffffe00 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  4000000000000000
  $ sudo rbd unmap $DEV

  $ ceph osd pool delete hugeimg hugeimg --yes-i-really-really-mean-it >/dev/null 2>&1
