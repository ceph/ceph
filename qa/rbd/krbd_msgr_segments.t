
  $ get_block_name_prefix() {
  >     rbd info --format=json $1 | python3 -c "import sys, json; print(json.load(sys.stdin)['block_name_prefix'])"
  > }

Short segments:

  $ rbd create --size 12M img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -d -c 'pwrite 5120 512' $DEV >/dev/null
  $ xfs_io -d -c 'pwrite 12577280 512' $DEV >/dev/null
  $ hexdump $DEV
  0000000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0001400 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0001600 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0bfea00 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0bfec00 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ rbd rm --no-progress img

Short segment, ceph_msg_data_bio_cursor_init():

  $ rbd create --size 12M img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -d -c 'pwrite 0 512' $DEV >/dev/null
  $ rados -p rbd stat $(get_block_name_prefix img).0000000000000000
  .* size 512 (re)
  $ xfs_io -d -c 'pread -b 2M 0 2M' $DEV >/dev/null
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0000200 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ rbd rm --no-progress img

Short segment, ceph_msg_data_bio_advance():

  $ rbd create --size 12M img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -d -c 'pwrite 0 1049088' $DEV >/dev/null
  $ rados -p rbd stat $(get_block_name_prefix img).0000000000000000
  .* size 1049088 (re)
  $ xfs_io -d -c 'pread -b 2M 0 2M' $DEV >/dev/null
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0100200 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ rbd rm --no-progress img

Cloned bios (dm-snapshot.ko, based on generic/081):

  $ rbd create --size 300M img
  $ DEV=$(sudo rbd map img)
  $ sudo vgcreate vg_img $DEV
    Physical volume "/dev/rbd?" successfully created* (glob)
    Volume group "vg_img" successfully created
  $ sudo lvcreate -L 256M -n lv_img vg_img
    Logical volume "lv_img" created.
  $ udevadm settle
  $ sudo mkfs.ext4 -q /dev/mapper/vg_img-lv_img
  $ sudo lvcreate -L 4M --snapshot -n lv_snap vg_img/lv_img | grep created
    Logical volume "lv_snap" created.
  $ udevadm settle
  $ sudo mount /dev/mapper/vg_img-lv_snap /mnt
  $ sudo xfs_io -f -c 'pwrite 0 5M' /mnt/file1 >/dev/null
  $ sudo umount /mnt
  $ sudo vgremove -f vg_img
    Logical volume "lv_snap" successfully removed* (glob)
    Logical volume "lv_img" successfully removed* (glob)
    Volume group "vg_img" successfully removed* (glob)
  $ sudo pvremove $DEV
    Labels on physical volume "/dev/rbd?" successfully wiped* (glob)
  $ sudo rbd unmap $DEV
  $ rbd rm --no-progress img
