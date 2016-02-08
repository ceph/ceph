  $ ceph-authtool kring --create-keyring --gen-key
  creating kring

  $ ceph-authtool --cap osd 'allow rx pool=swimming' kring
  $ ceph-authtool kring --list|grep -E '^\Wcaps '
  \tcaps osd = "allow rx pool=swimming" (esc)

  $ cat kring
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \tcaps osd = "allow rx pool=swimming" (esc)
