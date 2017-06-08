  $ ceph-authtool kring --create-keyring --gen-key
  creating kring

# TODO is this nice?
  $ ceph-authtool --cap osd 'broken' kring
  $ ceph-authtool kring --list|grep -P '^\tcaps '
  \tcaps osd = "broken" (esc)

# TODO is this nice?
  $ ceph-authtool --cap xyzzy 'broken' kring
  $ ceph-authtool kring --list|grep -P '^\tcaps '
  \tcaps xyzzy = "broken" (esc)
