  $ cauthtool kring --create-keyring --gen-key
  creating kring

  $ cauthtool --cap osd 'allow rx pool=swimming' kring
  $ cauthtool kring --list|grep -P '^\tcaps '
  \tcaps osd = "allow rx pool=swimming" (esc)

  $ cat kring
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid = [0-9]{20} \(esc\) (re)
  \tcaps osd = "allow rx pool=swimming" (esc)
