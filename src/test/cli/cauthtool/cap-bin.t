  $ cauthtool kring --create-keyring --gen-key --bin
  creating kring

  $ cauthtool --cap osd 'allow rx pool=swimming' --bin kring
  $ cauthtool kring --list|grep -P '^\tcaps '
  \tcaps osd = "allow rx pool=swimming" (esc)
