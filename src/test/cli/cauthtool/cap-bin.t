  $ ceph-authtool kring --create-keyring --gen-key --bin
  creating kring

  $ ceph-authtool --cap osd 'allow rx pool=swimming' --bin kring
  $ ceph-authtool kring --list|grep -P '^\tcaps '
  \tcaps osd = "allow rx pool=swimming" (esc)
