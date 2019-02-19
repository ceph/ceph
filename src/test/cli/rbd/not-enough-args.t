  $ rbd info
  rbd: image name was not specified
  [22]
  $ rbd create
  rbd: image name was not specified
  [22]
  $ rbd clone
  rbd: image name was not specified
  [22]
  $ rbd clone foo
  rbd: snapshot name was not specified
  [22]
  $ rbd clone foo@snap
  rbd: destination image name was not specified
  [22]
  $ rbd clone foo bar
  rbd: snapshot name was not specified
  [22]
  $ rbd clone foo bar@snap
  rbd: snapshot name was not specified
  [22]
  $ rbd children
  rbd: image name was not specified
  [22]
  $ rbd flatten
  rbd: image name was not specified
  [22]
  $ rbd resize
  rbd: image name was not specified
  [22]
  $ rbd rm
  rbd: image name was not specified
  [22]
  $ rbd export
  rbd: image name was not specified
  [22]
  $ rbd import
  rbd: path was not specified
  [22]
  $ rbd diff
  rbd: image name was not specified
  [22]
  $ rbd export-diff
  rbd: image name was not specified
  [22]
  $ rbd export-diff foo
  rbd: path was not specified
  [22]
  $ rbd export-diff foo@snap
  rbd: path was not specified
  [22]
  $ rbd merge-diff
  rbd: first diff was not specified
  [22]
  $ rbd merge-diff /tmp/diff1
  rbd: second diff was not specified
  [22]
  $ rbd merge-diff /tmp/diff1 /tmp/diff2
  rbd: path was not specified
  [22]
  $ rbd import-diff
  rbd: path was not specified
  [22]
  $ rbd import-diff /tmp/diff
  rbd: image name was not specified
  [22]
  $ rbd cp
  rbd: image name was not specified
  [22]
  $ rbd cp foo
  rbd: destination image name was not specified
  [22]
  $ rbd cp foo@snap
  rbd: destination image name was not specified
  [22]
  $ rbd deep cp
  rbd: image name was not specified
  [22]
  $ rbd deep cp foo
  rbd: destination image name was not specified
  [22]
  $ rbd deep cp foo@snap
  rbd: destination image name was not specified
  [22]
  $ rbd mv
  rbd: image name was not specified
  [22]
  $ rbd mv foo
  rbd: destination image name was not specified
  [22]
  $ rbd image-meta list
  rbd: image name was not specified
  [22]
  $ rbd image-meta get
  rbd: image name was not specified
  [22]
  $ rbd image-meta get foo
  rbd: metadata key was not specified
  [22]
  $ rbd image-meta set
  rbd: image name was not specified
  [22]
  $ rbd image-meta set foo
  rbd: metadata key was not specified
  [22]
  $ rbd image-meta set foo key
  rbd: metadata value was not specified
  [22]
  $ rbd image-meta remove
  rbd: image name was not specified
  [22]
  $ rbd image-meta remove foo
  rbd: metadata key was not specified
  [22]
  $ rbd object-map rebuild
  rbd: image name was not specified
  [22]
  $ rbd snap ls
  rbd: image name was not specified
  [22]
  $ rbd snap create
  rbd: image name was not specified
  [22]
  $ rbd snap create foo
  rbd: snapshot name was not specified
  [22]
  $ rbd snap rollback
  rbd: image name was not specified
  [22]
  $ rbd snap rollback foo
  rbd: snapshot name was not specified
  [22]
  $ rbd snap rm
  rbd: image name was not specified
  [22]
  $ rbd snap rm foo
  rbd: snapshot name was not specified
  [22]
  $ rbd snap purge
  rbd: image name was not specified
  [22]
  $ rbd snap protect
  rbd: image name was not specified
  [22]
  $ rbd snap protect foo
  rbd: snapshot name was not specified
  [22]
  $ rbd snap unprotect
  rbd: image name was not specified
  [22]
  $ rbd snap unprotect foo
  rbd: snapshot name was not specified
  [22]
  $ rbd watch
  rbd: image name was not specified
  [22]
  $ rbd status
  rbd: image name was not specified
  [22]
  $ rbd device map
  rbd: image name was not specified
  [22]
  $ rbd device unmap
  rbd: unmap requires either image name or device path
  [22]
  $ rbd feature disable
  rbd: image name was not specified
  [22]
  $ rbd feature disable foo
  rbd: at least one feature name must be specified
  [22]
  $ rbd feature enable
  rbd: image name was not specified
  [22]
  $ rbd feature enable foo
  rbd: at least one feature name must be specified
  [22]
  $ rbd lock list
  rbd: image name was not specified
  [22]
  $ rbd lock add
  rbd: image name was not specified
  [22]
  $ rbd lock add foo
  rbd: lock id was not specified
  [22]
  $ rbd lock remove
  rbd: image name was not specified
  [22]
  $ rbd lock remove foo
  rbd: lock id was not specified
  [22]
  $ rbd lock remove foo id
  rbd: locker was not specified
  [22]
  $ rbd bench --io-type write
  rbd: image name was not specified
  [22]
  $ rbd mirror pool enable rbd
  rbd: must specify 'image' or 'pool' mode.
  [22]
  $ rbd mirror pool peer add rbd
  rbd: remote cluster was not specified
  [22]
  $ rbd mirror pool peer remove rbd
  rbd: must specify peer uuid
  [22]
  $ rbd mirror image demote
  rbd: image name was not specified
  [22]
  $ rbd mirror image disable
  rbd: image name was not specified
  [22]
  $ rbd mirror image enable
  rbd: image name was not specified
  [22]
  $ rbd mirror image promote
  rbd: image name was not specified
  [22]
  $ rbd mirror image resync
  rbd: image name was not specified
  [22]
