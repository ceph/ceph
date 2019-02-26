  $ ceph-authtool kring --create-keyring --gen-key --mode 0644
  creating kring

  $ ceph-authtool --cap osd 'allow rx pool=swimming' kring
  $ ceph-authtool kring --list|grep -E '^[[:space:]]caps '
  \tcaps osd = "allow rx pool=swimming" (esc)

# TODO it seems --cap overwrites all previous caps; is this wanted?
  $ ceph-authtool --cap mds 'allow' kring
  $ ceph-authtool kring --list|grep -E '^[[:space:]]caps '
  \tcaps mds = "allow" (esc)
