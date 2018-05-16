  $ ceph-authtool kring --create-keyring --gen-key --mode 0644
  creating kring

# TODO is this nice?
  $ ceph-authtool --cap osd 'broken' kring
  $ ceph-authtool kring --list|grep -E '^[[:space:]]caps '
  \tcaps osd = "broken" (esc)

# TODO is this nice?
  $ ceph-authtool --cap xyzzy 'broken' kring
  $ ceph-authtool kring --list|grep -E '^[[:space:]]caps '
  \tcaps xyzzy = "broken" (esc)
