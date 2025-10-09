
  $ rbd create --size 4M img
  $ DEV=$(sudo rbd map img)

Zero, < 1 block:

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 156672 -l 512 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131584 -l 64512 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131584 -l 65024 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131072 -l 65024 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

Zero, 1 block:

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131072 -l 65536 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0030000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131072 -l 66048 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0030000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 130560 -l 66048 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0030000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 130560 -l 66560 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0030000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

Zero, < 2 blocks:

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 163840 -l 65536 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131584 -l 130048 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131584 -l 130560 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0030000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0040000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131072 -l 130560 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0030000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

Zero, 2 blocks:

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131072 -l 131072 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0040000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 131072 -l 131584 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0040000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 130560 -l 131584 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0040000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 130560 -l 132096 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0020000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0040000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

Zero, 37 blocks:

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 589824 -l 2424832 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0090000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  02e0000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 589312 -l 2424832 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0090000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  02d0000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 590336 -l 2424832 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  00a0000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  02e0000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

Truncate:

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 4193792 -l 512 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 4129280 -l 65024 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 4128768 -l 65536 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  03f0000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 4128256 -l 66048 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  03f0000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 4063744 -l 130560 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  03f0000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 4063232 -l 131072 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  03e0000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 4062720 -l 131584 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  03e0000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 512 -l 4193792 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0010000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

Delete:

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 0 -l 4194304 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

Empty clone:

  $ xfs_io -c 'pwrite -S 0xab -w 0 4M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create --no-progress img@snap
  $ rbd snap protect img@snap

  $ rbd clone img@snap cloneimg1
  $ DEV=$(sudo rbd map cloneimg1)
  $ blkdiscard -o 720896 -l 2719744 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 abab abab abab abab abab abab abab abab
  *
  0400000
  $ sudo rbd unmap $DEV

  $ rbd clone img@snap cloneimg2
  $ DEV=$(sudo rbd map cloneimg2)
  $ blkdiscard -o 1474560 -l 2719744 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 abab abab abab abab abab abab abab abab
  *
  0400000
  $ sudo rbd unmap $DEV

  $ rbd clone img@snap cloneimg3
  $ DEV=$(sudo rbd map cloneimg3)
  $ blkdiscard -o 0 -l 4194304 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 abab abab abab abab abab abab abab abab
  *
  0400000
  $ sudo rbd unmap $DEV

Full clone:

  $ rbd clone img@snap cloneimg4
  $ DEV=$(sudo rbd map cloneimg4)

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 720896 -l 2719744 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  00b0000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0340000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 1474560 -l 2719744 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0170000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

  $ xfs_io -c 'pwrite -w 0 4M' $DEV >/dev/null
  $ blkdiscard -o 0 -l 4194304 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0400000

  $ sudo rbd unmap $DEV

Multiple object requests:

  $ rbd create --size 50M --stripe-unit 16K --stripe-count 5 fancyimg
  $ DEV=$(sudo rbd map fancyimg)

  $ xfs_io -c 'pwrite -b 4M -w 0 50M' $DEV >/dev/null
  $ blkdiscard -o 0 -l 143360 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  3200000

  $ xfs_io -c 'pwrite -b 4M -w 0 50M' $DEV >/dev/null
  $ blkdiscard -o 0 -l 286720 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0008000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0014000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  001c000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0028000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0030000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  003c000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0044000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  3200000

  $ xfs_io -c 'pwrite -b 4M -w 0 50M' $DEV >/dev/null
  $ blkdiscard -o 0 -l 573440 $DEV
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0050000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  3200000

  $ sudo rbd unmap $DEV

  $ rbd rm --no-progress fancyimg
  $ rbd rm --no-progress cloneimg4
  $ rbd rm --no-progress cloneimg3
  $ rbd rm --no-progress cloneimg2
  $ rbd rm --no-progress cloneimg1
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img
