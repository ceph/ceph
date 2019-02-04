
  $ get_block_name_prefix() {
  >     rbd info --format=json $1 | python -c "import sys, json; print json.load(sys.stdin)['block_name_prefix']"
  > }

  $ rbd create --size 200M img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite -b 4M 0 200M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create img@snap
  $ rbd snap protect img@snap

cloneimg1:
1 object in an object set, 4M
25 full object sets
25 objects in total

  $ rbd clone img@snap cloneimg1
  $ DEV=$(sudo rbd map cloneimg1)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ fallocate -z -l 100M $DEV
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg1)
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV

cloneimg2:
7 objects in an object set, 28M
3 full object sets
min((100M % 28M) / 512K, 7) = 7 objects in the last object set
28 objects in total

  $ rbd clone --stripe-unit 512K --stripe-count 7 img@snap cloneimg2
  $ DEV=$(sudo rbd map cloneimg2)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ fallocate -z -l 100M $DEV
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg2)
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV

cloneimg3:
23 objects in an object set, 92M
1 full object set
min((100M % 92M) / 512K, 23) = 16 objects in the last object set
39 objects in total

  $ rbd clone --stripe-unit 512K --stripe-count 23 img@snap cloneimg3
  $ DEV=$(sudo rbd map cloneimg3)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ fallocate -z -l 100M $DEV
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg3)
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV

cloneimg4:
65 objects in an object set, 260M
0 full object sets
min((100M % 260M) / 512K, 65) = 65 objects in the last object set
65 objects in total

  $ rbd clone --stripe-unit 512K --stripe-count 65 img@snap cloneimg4
  $ DEV=$(sudo rbd map cloneimg4)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ fallocate -z -l 100M $DEV
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg4)
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  6400000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  c800000
  $ sudo rbd unmap $DEV

  $ rados -p rbd ls | grep -c $(get_block_name_prefix cloneimg1)
  25
  $ rados -p rbd ls | grep -c $(get_block_name_prefix cloneimg2)
  28
  $ rados -p rbd ls | grep -c $(get_block_name_prefix cloneimg3)
  39
  $ rados -p rbd ls | grep -c $(get_block_name_prefix cloneimg4)
  65

  $ rbd rm --no-progress cloneimg4
  $ rbd rm --no-progress cloneimg3
  $ rbd rm --no-progress cloneimg2
  $ rbd rm --no-progress cloneimg1
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img
